"""SR860 Configured Rate Streaming - Stream at the SR860's configured rate.

This script respects the SR860's current configuration or allows the user to
configure streaming parameters via command line. It does NOT try to optimize
or force rates - it uses exactly what is configured.
"""

import argparse
import logging
import multiprocessing as mp
import numpy as np
import queue
import signal
import socket
import struct
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

import pyvisa

# Import base functionality from optimal stream
from sr860_optimal_stream import *
from sr860_max_stream import StreamReceiver, BinaryFileWriter

# Import binary reader for automatic analysis
from sr860_binary_reader import SR860BinaryReader

logging.basicConfig(
    format="%(levelname)-8s [%(processName)-12s:%(threadName)-10s] %(asctime)s | %(message)s",
    datefmt="%H:%M:%S.%f",
    level=logging.INFO,
)


class ProcessStreamReceiver:
    """Process-based UDP receiver for SR860 streaming data."""
    
    def __init__(self, config: StreamConfig, data_queue: mp.Queue, stats_queue: mp.Queue, stop_event: mp.Event):
        self.config = config
        self.data_queue = data_queue
        self.stats_queue = stats_queue
        self.stop_event = stop_event
        self.socket = None
        
    def run(self):
        """Main receiver loop running in separate process."""
        # Set up signal handling in the child process
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        
        # Initialize local statistics
        stats = EnhancedStreamingStats(start_time=time.time())
        
        # Create and configure socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Socket optimization for high-throughput
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16*1024*1024)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)
            except:
                pass
        except Exception as e:
            logging.warning(f"Some socket optimizations failed: {e}")
        
        self.socket.bind(('', self.config.port))
        self.socket.settimeout(0.01)  # Short timeout for responsive shutdown
        
        logging.info(f"Process-based receiver listening on port {self.config.port}")
        
        # Pre-allocate packet buffer
        packet_buffer = bytearray(self.config.packet_bytes + 100)
        last_stats_update = time.time()
        stats_update_interval = 1.0  # Send stats every second
        
        while not self.stop_event.is_set():
            try:
                # Receive packet
                bytes_received = self.socket.recv_into(packet_buffer)
                
                if bytes_received < 4:
                    continue
                
                # Parse header
                header_bytes = packet_buffer[:4]
                header = (header_bytes[0] << 24) | (header_bytes[1] << 16) | (header_bytes[2] << 8) | header_bytes[3]
                
                # Extract fields according to SR860 manual
                packet_counter = header & 0xFF
                sample_rate_code = (header >> 16) & 0xFF
                
                # Extract status bits (bits 24-31)
                status = (header >> 24) & 0xFF
                is_little_endian = bool(status & 0x10)  # Bit 28
                has_integrity_check = bool(status & 0x20)  # Bit 29
                has_overload = bool(status & 0x01)  # Bit 24
                has_error = bool(status & 0x02)  # Bit 25
                
                # Calculate samples
                data_bytes = bytes_received - 4
                bytes_per_sample = self.config.bytes_per_point * self.config.points_per_sample
                samples_in_packet = data_bytes // bytes_per_sample
                
                # Update statistics
                stats.update_packet(bytes_received, samples_in_packet, packet_counter)
                
                # Track rate codes
                if not hasattr(stats, 'rate_code_counts'):
                    stats.rate_code_counts = {}
                stats.rate_code_counts[sample_rate_code] = stats.rate_code_counts.get(sample_rate_code, 0) + 1
                
                # Track byte order and integrity settings from packet headers
                if not hasattr(stats, 'header_info'):
                    stats.header_info = {
                        'is_little_endian': is_little_endian,
                        'has_integrity_check': has_integrity_check,
                        'packet_count': 0,
                        'overload_count': 0,
                        'error_count': 0
                    }
                
                stats.header_info['packet_count'] += 1
                if has_overload:
                    stats.header_info['overload_count'] += 1
                if has_error:
                    stats.header_info['error_count'] += 1
                
                # Send data packet to writer queue
                packet_data = {
                    'data': bytes(packet_buffer[:bytes_received]),
                    'timestamp': time.time_ns(),
                    'samples': samples_in_packet,
                    'is_little_endian': is_little_endian,
                    'has_integrity_check': has_integrity_check,
                    'has_overload': has_overload,
                    'has_error': has_error
                }
                
                try:
                    self.data_queue.put_nowait(packet_data)
                except queue.Full:
                    stats.errors += 1
                    logging.warning("Data queue full - packet dropped")
                
                # Send periodic stats updates
                current_time = time.time()
                if current_time - last_stats_update >= stats_update_interval:
                    stats_summary = {
                        'packets_received': stats.packets_received,
                        'samples_received': stats.samples_received,
                        'bytes_received': stats.bytes_received,
                        'sequence_errors': stats.sequence_errors,
                        'receive_errors': stats.errors,
                        'rate_code_counts': dict(stats.rate_code_counts) if hasattr(stats, 'rate_code_counts') else {},
                        'header_info': dict(stats.header_info) if hasattr(stats, 'header_info') else {}
                    }
                    try:
                        self.stats_queue.put_nowait(stats_summary)
                    except queue.Full:
                        pass  # Don't block on stats
                    last_stats_update = current_time
                    
            except socket.timeout:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    stats.errors += 1
                    logging.error(f"Receive error: {e}")
                    
        # Cleanup
        if self.socket:
            self.socket.close()
        
        # Send final stats
        final_stats = {
            'packets_received': stats.packets_received,
            'samples_received': stats.samples_received,
            'bytes_received': stats.bytes_received,
            'sequence_errors': stats.sequence_errors,
            'receive_errors': stats.errors,
            'rate_code_counts': dict(stats.rate_code_counts) if hasattr(stats, 'rate_code_counts') else {},
            'header_info': dict(stats.header_info) if hasattr(stats, 'header_info') else {},
            'final': True
        }
        try:
            self.stats_queue.put(final_stats)
        except:
            pass
        
        logging.info("Receiver process shutting down")


def receiver_process_entry(config: StreamConfig, data_queue: mp.Queue, stats_queue: mp.Queue, stop_event: mp.Event):
    """Entry point for receiver process."""
    receiver = ProcessStreamReceiver(config, data_queue, stats_queue, stop_event)
    receiver.run()


class ProcessBinaryFileWriter(threading.Thread):
    """Writer thread that gets data from the receiver process queue."""
    
    def __init__(self, filename: str, config: StreamConfig, data_queue: mp.Queue, stop_event: mp.Event,
                 actual_rate_hz: Optional[float] = None, rate_divider: Optional[int] = None, 
                 max_rate_hz: Optional[float] = None, detected_little_endian: Optional[bool] = None,
                 detected_integrity_check: Optional[bool] = None):
        super().__init__(name="FileWriter")
        self.filename = filename
        self.config = config
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.samples_written = 0
        self.bytes_written = 0
        self.file = None
        
        # Store rate information in config object for header writing
        if actual_rate_hz is not None:
            self.config.actual_rate_hz = actual_rate_hz
        if rate_divider is not None:
            self.config.rate_divider = rate_divider
        if max_rate_hz is not None:
            self.config.max_rate_hz = max_rate_hz
        if detected_little_endian is not None:
            self.config.detected_little_endian = detected_little_endian
        if detected_integrity_check is not None:
            self.config.detected_integrity_check = detected_integrity_check
        
    def run(self):
        """Main writer loop."""
        # Open file and write header
        self.file = open(self.filename, 'wb')
        self._write_header()
        
        buffer = bytearray()
        buffer_size = 1024 * 1024  # 1MB buffer
        
        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Get data from queue with timeout
                packet_data = self.data_queue.get(timeout=0.1)
                
                # Extract data portion (skip 4-byte header)
                data = packet_data['data'][4:]
                samples = packet_data['samples']
                
                # Add to buffer
                buffer.extend(data)
                self.samples_written += samples
                self.bytes_written += len(data)
                
                # Flush if buffer is large enough
                if len(buffer) >= buffer_size:
                    self.file.write(buffer)
                    buffer = bytearray()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Writer error: {e}")
                
        # Final flush
        if buffer:
            self.file.write(buffer)
            
        # Close file
        if self.file:
            self.file.close()
            
        logging.info(f"Writer thread finished: {self.samples_written} samples, {self.bytes_written} bytes")
        
    def _write_header(self):
        """Write file header with metadata."""
        header = {
            'version': 1,
            'timestamp': time.time(),
            'channel': self.config.channel.value,
            'format': self.config.format.value,
            'points_per_sample': self.config.points_per_sample,
            'bytes_per_point': self.config.bytes_per_point,
            'packet_size': self.config.packet_size.value,
            'port': self.config.port,
            'use_little_endian': self.config.use_little_endian,
            'use_integrity_check': self.config.use_integrity_check,
            'actual_rate_hz': getattr(self.config, 'actual_rate_hz', None),  # Will be set by caller
            'rate_divider': getattr(self.config, 'rate_divider', None),      # Will be set by caller
            'max_rate_hz': getattr(self.config, 'max_rate_hz', None),        # Will be set by caller
            'detected_little_endian': getattr(self.config, 'detected_little_endian', None),  # From packet headers
            'detected_integrity_check': getattr(self.config, 'detected_integrity_check', None),  # From packet headers
        }
        
        # Write header size (4 bytes) and header data
        header_bytes = str(header).encode('utf-8')
        self.file.write(struct.pack('<I', len(header_bytes)))
        self.file.write(header_bytes)


class SR860ConfiguredController:
    """Controller that respects SR860's configuration without optimization."""
    
    def __init__(self, ip: str):
        self.ip = ip
        self.inst = None
        self.current_config = {}
        
    def connect(self) -> bool:
        """Connect to SR860."""
        try:
            rm = pyvisa.ResourceManager('@py')
            resource = f"TCPIP0::{self.ip}::inst0::INSTR"
            self.inst = rm.open_resource(
                resource,
                write_termination="\n",
                read_termination="\n",
                timeout=5000,
            )
            
            # Test connection
            idn = self.inst.query("*IDN?").strip()
            logging.info(f"Connected to: {idn}")
            
            # Clear errors
            self.inst.write("*CLS")
            time.sleep(0.1)
            
            return True
            
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            return False
    
    def read_current_configuration(self) -> Dict[str, Any]:
        """Read the current SR860 streaming configuration."""
        if not self.inst:
            raise RuntimeError("Not connected")
        
        logging.info("Reading current SR860 configuration...")
        
        # Read all streaming parameters
        channel_idx = int(self.inst.query("STREAMCH?").strip())
        format_idx = int(self.inst.query("STREAMFMT?").strip())
        packet_idx = int(self.inst.query("STREAMPCKT?").strip())
        port = int(self.inst.query("STREAMPORT?").strip())
        option = int(self.inst.query("STREAMOPTION?").strip())
        rate_n = int(self.inst.query("STREAMRATE?").strip())
        max_rate = float(self.inst.query("STREAMRATEMAX?").strip())
        
        # Calculate actual rate
        actual_rate = max_rate / (2 ** rate_n)
        
        # Decode options
        use_little_endian = bool(option & 1)
        use_integrity_check = bool(option & 2)
        
        # Map indices to enums
        channel_map = {0: StreamChannel.X, 1: StreamChannel.XY, 2: StreamChannel.RT, 3: StreamChannel.XYRT}
        format_map = {0: StreamFormat.FLOAT32, 1: StreamFormat.INT16}
        packet_map = {0: PacketSize.SIZE_1024, 1: PacketSize.SIZE_512, 
                      2: PacketSize.SIZE_256, 3: PacketSize.SIZE_128}
        
        self.current_config = {
            'channel': channel_map.get(channel_idx, StreamChannel.XYRT),
            'channel_idx': channel_idx,
            'format': format_map.get(format_idx, StreamFormat.FLOAT32),
            'format_idx': format_idx,
            'packet_size': packet_map.get(packet_idx, PacketSize.SIZE_1024),
            'packet_idx': packet_idx,
            'port': port,
            'use_little_endian': use_little_endian,
            'use_integrity_check': use_integrity_check,
            'rate_divider': rate_n,
            'max_rate_hz': max_rate,
            'actual_rate_hz': actual_rate,
            'option_value': option
        }
        
        # Create StreamConfig object
        config = StreamConfig(
            channel=self.current_config['channel'],
            format=self.current_config['format'],
            packet_size=self.current_config['packet_size'],
            port=port,
            use_little_endian=use_little_endian,
            use_integrity_check=use_integrity_check
        )
        
        # Calculate data rates
        bytes_per_sample = config.bytes_per_point * config.points_per_sample
        bytes_per_second = actual_rate * bytes_per_sample
        mbps = (bytes_per_second * 8) / 1e6
        packets_per_second = actual_rate / config.samples_per_packet if config.samples_per_packet > 0 else 0
        
        self.current_config.update({
            'config_object': config,
            'bytes_per_sample': bytes_per_sample,
            'bytes_per_second': bytes_per_second,
            'mbps': mbps,
            'packets_per_second': packets_per_second,
            'samples_per_packet': config.samples_per_packet
        })
        
        logging.info(f"Current configuration:")
        logging.info(f"  Channel:     {channel_map[channel_idx].name} ({config.points_per_sample} values/sample)")
        logging.info(f"  Format:      {format_map[format_idx].name} ({config.bytes_per_point} bytes/point)")
        logging.info(f"  Packet size: {config.packet_bytes} bytes")
        logging.info(f"  Port:        {port}")
        logging.info(f"  Rate:        {actual_rate:,.0f} Hz (divider: 2^{rate_n} from max {max_rate:,.0f} Hz)")
        logging.info(f"  Data rate:   {mbps:.1f} Mbps")
        logging.info(f"  Packet rate: {packets_per_second:.1f} packets/s")
        
        return self.current_config
    
    def apply_configuration(self, config: StreamConfig, rate_divider: Optional[int] = None) -> Dict[str, Any]:
        """Apply a new streaming configuration."""
        if not self.inst:
            raise RuntimeError("Not connected")
        
        logging.info("Applying new SR860 configuration...")
        
        # Apply settings
        self.inst.write(f"STREAMCH {config.channel.value}")
        self.inst.write(f"STREAMFMT {config.format.value}")
        self.inst.write(f"STREAMPCKT {config.packet_size.value}")
        self.inst.write(f"STREAMPORT {config.port}")
        self.inst.write(f"STREAMOPTION {config.option_value}")
        
        # Apply rate divider if specified
        if rate_divider is not None:
            self.inst.write(f"STREAMRATE {rate_divider}")
        
        time.sleep(0.2)
        
        # Read back actual configuration
        return self.read_current_configuration()
    
    def start_streaming(self):
        """Start SR860 streaming."""
        if not self.inst:
            raise RuntimeError("Not connected")
        
        self.inst.write("STREAM ON")
        logging.info("SR860 streaming started")
    
    def stop_streaming(self):
        """Stop SR860 streaming."""
        if self.inst:
            try:
                self.inst.write("STREAM OFF")
                logging.info("SR860 streaming stopped")
            except:
                pass
    
    def close(self):
        """Close connection."""
        if self.inst:
            try:
                self.stop_streaming()
                self.inst.close()
            except:
                pass

    def set_time_constant(self, time_constant_idx: int) -> Optional[float]:
        """Set the time constant and return the new maximum streaming rate.
        
        Args:
            time_constant_idx: Time constant index (0-21)
            
        Returns:
            New maximum streaming rate in Hz, or None if failed
        """
        if not self.inst:
            raise RuntimeError("Not connected")
        
        try:
            # Set time constant
            self.inst.write(f"OFLT {time_constant_idx}")
            time.sleep(0.5)  # Give SR860 time to update
            
            # Get new maximum rate
            new_max_rate = float(self.inst.query("STREAMRATEMAX?").strip())
            
            # Verify it was set
            actual_tc = int(self.inst.query("OFLT?").strip())
            if actual_tc != time_constant_idx:
                logging.warning(f"Time constant set failed: requested {time_constant_idx}, got {actual_tc}")
            else:
                logging.info(f"Time constant set to index {time_constant_idx}")
                logging.info(f"New maximum streaming rate: {new_max_rate:,.0f} Hz")
            
            return new_max_rate
            
        except Exception as e:
            logging.error(f"Failed to set time constant: {e}")
            return None


def configured_streaming_worker(ip: str, config: Optional[StreamConfig] = None, 
                               rate_divider: Optional[int] = None, duration: float = 10.0,
                               use_current_config: bool = True, time_constant: Optional[int] = None,
                               auto_analysis: bool = True):
    """Worker function for streaming at configured rate using process-based architecture."""
    
    logging.info(f"Process-based streaming worker started for {ip}")
    
    # Initialize start_time early to avoid UnboundLocalError
    start_time = time.time()
    
    # Create controller
    controller = SR860ConfiguredController(ip)
    
    # Create multiprocessing communication objects
    mp_context = mp.get_context('spawn')  # Use spawn for clean process creation
    stats_queue = mp_context.Queue(maxsize=100)
    data_queue = mp_context.Queue(maxsize=1000)
    stop_event = mp_context.Event()
    
    # Set up signal handling for graceful shutdown
    def signal_handler(signum, frame):
        logging.info("Received interrupt signal, stopping...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    
    receiver_process = None
    writer_thread = None
    
    try:
        # Connect
        if not controller.connect():
            raise RuntimeError("Failed to connect to SR860")
        
        # Set time constant if specified
        if time_constant is not None:
            logging.info(f"Setting time constant to index {time_constant}")
            new_max_rate = controller.set_time_constant(time_constant)
            if new_max_rate:
                logging.info(f"Time constant changed. New max rate: {new_max_rate:,.0f} Hz")
            else:
                logging.warning("Failed to set time constant, continuing with current setting")
        
        # Get or apply configuration
        if use_current_config:
            logging.info("Using current SR860 configuration")
            config_info = controller.read_current_configuration()
            config = config_info['config_object']
        else:
            logging.info("Applying command line configuration")
            config_info = controller.apply_configuration(config, rate_divider)
            config = config_info['config_object']
        
        actual_rate = config_info['actual_rate_hz']
        
        # Create binary file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create data directory if it doesn't exist
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        filename = data_dir / f"sr860_configured_{ip.replace('.', '_')}_{timestamp}.bin"
        
        # Display configuration summary
        logging.info("\n" + "="*60)
        logging.info("STREAMING CONFIGURATION SUMMARY")
        logging.info("="*60)
        logging.info(f"Mode:              {'Current Config' if use_current_config else 'User Specified'}")
        logging.info(f"Actual rate:       {actual_rate:,.0f} Hz")
        logging.info(f"Expected duration: {duration:.1f} s")
        logging.info(f"Expected samples:  {int(actual_rate * duration):,}")
        logging.info(f"Expected data:     {config_info['mbps'] * duration * 0.125:.1f} MB")
        
        # Start receiver process
        receiver_process = mp_context.Process(
            target=receiver_process_entry,
            args=(config, data_queue, stats_queue, stop_event),
            name=f"SR860_Receiver_{ip}"
        )
        receiver_process.start()
        logging.info(f"Started receiver process (PID: {receiver_process.pid})")
        
        # Start writer thread
        writer_thread = ProcessBinaryFileWriter(
            filename, config, data_queue, stop_event, 
            actual_rate_hz=actual_rate, 
            rate_divider=config_info['rate_divider'], 
            max_rate_hz=config_info['max_rate_hz'],
            detected_little_endian=config_info['use_little_endian'],
            detected_integrity_check=config_info['use_integrity_check']
        )
        writer_thread.start()
        logging.info("Started writer thread")
        
        # Give receiver time to initialize
        time.sleep(1)
        
        # Start streaming
        controller.start_streaming()
        
        # Monitor for duration
        logging.info(f"\nStreaming at {actual_rate:,.0f} Hz for {duration} seconds...")
        
        start_time = time.time()
        last_log_time = start_time
        
        # Aggregate statistics from receiver process
        total_stats = {
            'packets_received': 0,
            'samples_received': 0,
            'bytes_received': 0,
            'sequence_errors': 0,
            'receive_errors': 0,
            'rate_code_counts': {},
            'header_info': {}
        }
        
        # Track detected header information
        detected_little_endian = None
        detected_integrity_check = None
        
        while True:
            elapsed = time.time() - start_time
            
            # Check duration
            if duration > 0 and elapsed >= duration:
                stop_event.set()
                break
            
            # Process stats from receiver
            try:
                while True:
                    stats_update = stats_queue.get_nowait()
                    if stats_update.get('final', False):
                        # Final stats, update and break
                        total_stats = stats_update
                    else:
                        # Update running totals
                        total_stats['packets_received'] = stats_update['packets_received']
                        total_stats['samples_received'] = stats_update['samples_received'] 
                        total_stats['bytes_received'] = stats_update['bytes_received']
                        total_stats['sequence_errors'] = stats_update['sequence_errors']
                        total_stats['receive_errors'] = stats_update['receive_errors']
                        total_stats['rate_code_counts'] = stats_update['rate_code_counts']
                        
                        # Update header information if available
                        if 'header_info' in stats_update:
                            total_stats['header_info'] = stats_update['header_info']
                            # Update detected values for writer
                            if detected_little_endian is None and 'is_little_endian' in stats_update['header_info']:
                                detected_little_endian = stats_update['header_info']['is_little_endian']
                                detected_integrity_check = stats_update['header_info']['has_integrity_check']
                                # Update writer with detected values
                                if writer_thread and hasattr(writer_thread, 'config'):
                                    writer_thread.config.detected_little_endian = detected_little_endian
                                    writer_thread.config.detected_integrity_check = detected_integrity_check
            except queue.Empty:
                pass
            
            # Print statistics every 5 seconds
            current_time = time.time()
            if current_time - last_log_time >= 5.0:
                # Calculate rates
                if elapsed > 0:
                    actual_sample_rate = total_stats['samples_received'] / elapsed
                    actual_mbps = (total_stats['bytes_received'] * 8) / (elapsed * 1e6)
                    efficiency = (actual_sample_rate / actual_rate * 100) if actual_rate > 0 else 0
                else:
                    actual_sample_rate = actual_mbps = efficiency = 0
                
                # Check for rate changes in packet headers
                rate_stability = "STABLE"
                if len(total_stats['rate_code_counts']) > 1:
                    rate_stability = f"VARIABLE ({len(total_stats['rate_code_counts'])} rates)"
                
                logging.info(
                    f"Progress: {total_stats['samples_received']:,} samples, "
                    f"{actual_mbps:.1f} Mbps, "
                    f"efficiency: {efficiency:.1f}%, "
                    f"rate: {rate_stability}"
                )
                last_log_time = current_time
            
            time.sleep(0.1)
        
    except KeyboardInterrupt:
        logging.info("Streaming interrupted")
        stop_event.set()
    except Exception as e:
        logging.error(f"Streaming error: {e}")
        import traceback
        traceback.print_exc()
        stop_event.set()
    finally:
        # Stop everything
        logging.info("Shutting down streaming...")
        stop_event.set()
        
        # Stop SR860 streaming
        try:
            controller.stop_streaming()
        except:
            pass
        
        # Wait for receiver process to finish
        if receiver_process and receiver_process.is_alive():
            logging.info("Waiting for receiver process to finish...")
            receiver_process.join(timeout=5.0)
            if receiver_process.is_alive():
                logging.warning("Receiver process did not stop gracefully, terminating...")
                receiver_process.terminate()
                receiver_process.join(timeout=2.0)
        
        # Wait for writer thread to finish
        if writer_thread and writer_thread.is_alive():
            logging.info("Waiting for writer thread to finish...")
            writer_thread.join(timeout=5.0)
        
        # Get final stats if available
        try:
            while not stats_queue.empty():
                stats_update = stats_queue.get_nowait()
                if stats_update.get('final', False):
                    total_stats = stats_update
        except:
            pass
            
        controller.close()
        
        # Final statistics
        final_duration = time.time() - start_time
        
        # Calculate final metrics
        expected_samples = int(actual_rate * duration)
        actual_sample_rate = total_stats['samples_received'] / final_duration if final_duration > 0 else 0
        actual_mbps = (total_stats['bytes_received'] * 8) / (final_duration * 1e6) if final_duration > 0 else 0
        efficiency_percent = (actual_sample_rate / actual_rate * 100) if actual_rate > 0 else 0
        sample_loss = max(0, expected_samples - total_stats['samples_received'])
        sample_loss_percent = (sample_loss / expected_samples * 100) if expected_samples > 0 else 0
        
        logging.info("\n" + "="*60)
        logging.info("CONFIGURED STREAMING FINAL STATISTICS")
        logging.info("="*60)
        
        logging.info(f"\nConfiguration:")
        logging.info(f"  Mode:              {'Current Config' if use_current_config else 'User Specified'}")
        logging.info(f"  Channel:           {config.channel.name}")
        logging.info(f"  Format:            {config.format.name}")
        logging.info(f"  Packet size:       {config.packet_bytes} bytes")
        logging.info(f"  Configured rate:   {actual_rate:,.0f} Hz")
        
        logging.info("\nDuration:")
        logging.info(f"  Expected: {duration:.1f} s")
        logging.info(f"  Actual:   {final_duration:.1f} s")
        
        logging.info("\nSamples:")
        logging.info(f"  Expected: {expected_samples:,}")
        logging.info(f"  Actual:   {total_stats['samples_received']:,}")
        logging.info(f"  Lost:     {sample_loss:,} ({sample_loss_percent:.1f}%)")
        
        logging.info("\nSample Rate:")
        logging.info(f"  Expected: {actual_rate:,.1f} Hz")
        logging.info(f"  Actual:   {actual_sample_rate:,.1f} Hz")
        logging.info(f"  Efficiency: {efficiency_percent:.1f}%")
        
        logging.info("\nData Rate:")
        logging.info(f"  Expected: {config_info['mbps']:.1f} Mbps")
        logging.info(f"  Actual:   {actual_mbps:.1f} Mbps")
        
        logging.info("\nPacket Statistics:")
        logging.info(f"  Expected rate: {config_info['packets_per_second']:.1f} packets/s")
        logging.info(f"  Actual rate:   {total_stats['packets_received'] / final_duration:.1f} packets/s" if final_duration > 0 else "  Actual rate:   0.0 packets/s")
        logging.info(f"  Total received: {total_stats['packets_received']:,}")
        
        logging.info("\nErrors:")
        logging.info(f"  Receive errors:  {total_stats['receive_errors']}")
        logging.info(f"  Sequence errors: {total_stats['sequence_errors']}")
        
        # Header information analysis
        if 'header_info' in total_stats and total_stats['header_info']:
            header_info = total_stats['header_info']
            logging.info("\nPacket Header Analysis:")
            logging.info(f"  Byte order:      {'Little-endian' if header_info.get('is_little_endian', False) else 'Big-endian'}")
            logging.info(f"  Integrity check: {'Enabled' if header_info.get('has_integrity_check', False) else 'Disabled'}")
            logging.info(f"  Overload events: {header_info.get('overload_count', 0):,}")
            logging.info(f"  Error events:    {header_info.get('error_count', 0):,}")
            
            # Compare with configuration
            config_little_endian = config_info['use_little_endian']
            config_integrity = config_info['use_integrity_check']
            detected_little_endian = header_info.get('is_little_endian', False)
            detected_integrity = header_info.get('has_integrity_check', False)
            
            if config_little_endian != detected_little_endian:
                logging.info(f"  ⚠️  Byte order mismatch: configured {'little' if config_little_endian else 'big'}, detected {'little' if detected_little_endian else 'big'}")
            if config_integrity != detected_integrity:
                logging.info(f"  ⚠️  Integrity check mismatch: configured {'enabled' if config_integrity else 'disabled'}, detected {'enabled' if detected_integrity else 'disabled'}")
        
        # Rate stability analysis
        if 'rate_code_counts' in total_stats and total_stats['rate_code_counts']:
            logging.info("\nSR860 Rate Analysis:")
            total_packets = sum(total_stats['rate_code_counts'].values())
            for rate_code in sorted(total_stats['rate_code_counts'].keys()):
                count = total_stats['rate_code_counts'][rate_code]
                rate_hz = 1.25e6 / (2 ** rate_code) if rate_code < 32 else 0
                percentage = (count / total_packets) * 100 if total_packets > 0 else 0
                logging.info(f"  Rate code {rate_code}: {rate_hz:>10,.0f} Hz - {count:>6,} packets ({percentage:>5.1f}%)")
            
            unique_rates = len(total_stats['rate_code_counts'])
            if unique_rates == 1:
                logging.info(f"  Rate stability:    STABLE - Single rate maintained")
            elif unique_rates <= 3:
                logging.info(f"  Rate stability:    MODERATE - {unique_rates} different rates")
            else:
                logging.info(f"  Rate stability:    UNSTABLE - {unique_rates} different rates")
        
        logging.info(f"\nOutput file: {filename}")
        logging.info(f"File size: {writer_thread.bytes_written/1e6:.1f} MB" if writer_thread else "File size: Unknown")
        
        # Enhanced performance assessment
        has_packet_loss = total_stats['sequence_errors'] > 0
        has_rate_variation = 'rate_code_counts' in total_stats and len(total_stats['rate_code_counts']) > 1
        
        logging.info("\n" + "="*50)
        logging.info("PERFORMANCE DIAGNOSIS")
        logging.info("="*50)
        
        if not has_packet_loss and efficiency_percent >= 95:
            logging.info("\n✅ EXCELLENT PERFORMANCE!")
            logging.info(f"   • No packet loss detected")
            logging.info(f"   • {efficiency_percent:.1f}% efficiency")
            if has_rate_variation:
                logging.info(f"   • SR860 used multiple rates (check time constant)")
        elif not has_packet_loss and efficiency_percent >= 80:
            logging.info("\n✅ GOOD PERFORMANCE")
            logging.info(f"   • No packet loss detected")
            logging.info(f"   • {efficiency_percent:.1f}% efficiency")
            if has_rate_variation:
                logging.info(f"   • Rate variation detected - SR860 adjusted its rate")
        elif not has_packet_loss:
            logging.info("\n⚠️  SR860 RATE INCONSISTENCY")
            logging.info(f"   • No packet loss - all transmitted packets received")
            logging.info(f"   • {efficiency_percent:.1f}% efficiency")
            logging.info(f"   • SR860 is not maintaining configured rate")
            logging.info(f"   • Check time constant and filter settings")
        else:
            logging.info("\n❌ TRUE PACKET LOSS DETECTED")
            logging.info(f"   • {total_stats['sequence_errors']} packets lost")
            logging.info(f"   • {efficiency_percent:.1f}% efficiency")
            logging.info(f"   • Network or system issues causing packet drops")
            logging.info(f"   • Consider reducing streaming rate")
        
        # If rate inconsistency but no packet loss
        if not has_packet_loss and efficiency_percent < 95:
            logging.info("\nRate inconsistency notes:")
            logging.info("   • This is normal SR860 behavior under certain conditions")
            logging.info("   • Time constant may be limiting the actual streaming rate")
            logging.info("   • Use STREAMRATEMAX? to check maximum allowed rate")
            logging.info("   • Adjust STREAMRATE n to reduce rate if needed")

        # Automatic analysis
        if auto_analysis and filename and Path(filename).exists():
            logging.info("\n" + "="*60)
            logging.info("AUTOMATIC ANALYSIS")
            logging.info("="*60)
            logging.info("Opening plots for data analysis...")
            
            try:
                # Create binary reader and perform analysis
                reader = SR860BinaryReader(str(filename))
                
                # Get file info
                info = reader.get_info()
                logging.info(f"Analyzing file: {info['filename']}")
                logging.info(f"Samples: {info['n_samples']:,}")
                logging.info(f"Channels: {info['channel_config']}")
                logging.info(f"Format: {info['data_format']}")
                
                # Display plots
                import matplotlib.pyplot as plt
                
                # Time series plot
                logging.info("Generating time series plot...")
                fig1 = reader.plot_time_series()
                
                # Frequency spectrum if enough samples
                if info['n_samples'] >= 1024:
                    logging.info("Generating frequency spectrum...")
                    fig2 = reader.plot_spectrum()
                
                # X-Y plots if available
                if 'X' in info['channel_config'] and 'Y' in info['channel_config']:
                    logging.info("Generating X-Y plots...")
                    fig3 = reader.plot_xy_data()
                
                # Show all plots
                logging.info("Displaying plots (close windows to continue)...")
                plt.show()
                
                logging.info("Analysis complete!")
                
            except Exception as e:
                logging.error(f"Automatic analysis failed: {e}")
                import traceback
                traceback.print_exc()
        elif auto_analysis:
            logging.warning("Auto-analysis requested but no valid output file found")


def main():
    """Main application for configured rate SR860 streaming."""
    
    parser = argparse.ArgumentParser(
        description="SR860 Configured Rate Streaming - Stream at SR860's configured rate",
        epilog="By default, uses the SR860's current configuration. "
               "Specify options to override specific parameters."
    )
    
    # Connection
    parser.add_argument("--ip", default="192.168.1.156", help="SR860 IP address")
    parser.add_argument("--duration", type=float, default=10.0, help="Duration in seconds")
    
    # Configuration options (all optional - if not specified, use current)
    parser.add_argument("--channel", choices=['X', 'XY', 'RT', 'XYRT'],
                        help="Streaming channels (default: use current)")
    parser.add_argument("--format", choices=['float32', 'int16'],
                        help="Data format (default: use current)")
    parser.add_argument("--packet", type=int, choices=[1024, 512, 256, 128],
                        help="Packet size in bytes (default: use current)")
    parser.add_argument("--port", type=int,
                        help="UDP port (default: use current)")
    parser.add_argument("--rate-divider", type=int,
                        help="Rate divider n where rate = max_rate/2^n (default: use current)")
    parser.add_argument("--endian", choices=['big', 'little'],
                        help="Byte order (default: use current)")
    parser.add_argument("--no-integrity", action="store_true",
                        help="Disable integrity checking (default: use current)")
    
    # Time constant option
    parser.add_argument("--time-constant", type=int, metavar="INDEX",
                        help="Set time constant index (0-21) before streaming. "
                             "Lower values = faster response = higher max rate. "
                             "0: 1μs (max 1.25MHz), 9: 1ms (max 78kHz), 21: 300s (max 1.22Hz)")
    
    # Mode selection
    parser.add_argument("--use-current", action="store_true",
                        help="Force use of current SR860 configuration (ignore other options)")
    
    # Automatic analysis option
    parser.add_argument("--no-auto-analysis", action="store_true",
                        help="Disable automatic analysis and plotting after streaming")
    
    args = parser.parse_args()
    
    # Determine if we should use current config or apply new settings
    use_current = args.use_current or (
        args.channel is None and 
        args.format is None and 
        args.packet is None and 
        args.port is None and 
        args.rate_divider is None and
        args.endian is None and
        not args.no_integrity and
        args.time_constant is None
    )
    
    config = None
    if not use_current:
        # Build configuration from command line arguments
        # We'll need to connect first to get defaults for unspecified options
        temp_controller = SR860ConfiguredController(args.ip)
        if temp_controller.connect():
            current = temp_controller.read_current_configuration()
            temp_controller.close()
            
            # Build config using specified options or current values
            config = StreamConfig(
                channel=StreamChannel[args.channel] if args.channel else current['channel'],
                format=StreamFormat.FLOAT32 if args.format == 'float32' else 
                       StreamFormat.INT16 if args.format == 'int16' else current['format'],
                packet_size={1024: PacketSize.SIZE_1024, 512: PacketSize.SIZE_512,
                           256: PacketSize.SIZE_256, 128: PacketSize.SIZE_128}.get(
                               args.packet, current['packet_size']),
                port=args.port if args.port else current['port'],
                use_little_endian=args.endian == 'little' if args.endian else current['use_little_endian'],
                use_integrity_check=not args.no_integrity if args.no_integrity else current['use_integrity_check']
            )
        else:
            logging.error("Failed to connect to get current configuration")
            return
    
    logging.info("="*60)
    logging.info("SR860 CONFIGURED RATE STREAMING")
    logging.info("="*60)
    logging.info(f"Mode: {'Using current SR860 configuration' if use_current else 'Applying specified configuration'}")
    logging.info(f"Target: Stream at SR860's configured rate without forcing optimization")
    
    # Run streaming
    try:
        configured_streaming_worker(
            args.ip,
            config=config,
            rate_divider=args.rate_divider,
            duration=args.duration,
            use_current_config=use_current,
            time_constant=args.time_constant,
            auto_analysis=not args.no_auto_analysis
        )
    except KeyboardInterrupt:
        logging.info("Streaming stopped by user")


if __name__ == "__main__":
    main() 