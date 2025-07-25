"""SR860 Configured Rate Streaming - Stream at the SR860's configured rate.

This script respects the SR860's current configuration or allows the user to
configure streaming parameters via command line. It does NOT try to optimize
or force rates - it uses exactly what is configured.
"""

import argparse
import errno
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

# Import from our clean SR860 class
from sr860_class import (
    StreamConfig, StreamChannel, StreamFormat, PacketSize,
    EnhancedStreamingStats, LossEvent, SR860Class
)

# Import binary reader for automatic analysis
from sr860_binary_reader import SR860BinaryReader

logging.basicConfig(
    format="%(levelname)-8s [%(processName)-12s:%(threadName)-10s] %(asctime)s | %(message)s",
    datefmt="%H:%M:%S.%f",
    level=logging.INFO,
)


class TimestampedStreamReceiver:
    """Enhanced UDP receiver with accurate sample timestamping based on packet counters."""
    
    def __init__(self, config: StreamConfig, data_queue: mp.Queue, stats_queue: mp.Queue, 
                 stop_event: mp.Event, stream_start_time: float, actual_rate_hz: float):
        self.config = config
        self.data_queue = data_queue
        self.stats_queue = stats_queue
        self.stop_event = stop_event
        self.socket = None
        
        # Timing parameters
        self.stream_start_time = stream_start_time  # Time when STREAM ON was issued
        self.actual_rate_hz = actual_rate_hz        # Actual SR860 sample rate
        
        # Packet counter tracking
        self.last_packet_counter = None
        self.packet_counter_wraps = 0
        self.total_packets_expected = 0
        self.gaps_detected = []
        
        # Statistics
        self.total_samples_received = 0
        self.total_missing_samples = 0
        
    def run(self):
        """Main receiver loop with packet counter tracking."""
        # Set up signal handling in the child process
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        
        # Initialize enhanced statistics with detailed loss analysis
        stats = EnhancedStreamingStats(start_time=time.time())
        
        # Create and configure socket with maximum buffer
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Socket optimization for high-throughput and low latency
        try:
            # Increase receive buffer to 64MB
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64*1024*1024)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Set high priority
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)
            except:
                pass
                
            # Enable timestamp if available
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_TIMESTAMP, 1)
            except:
                pass
                
        except Exception as e:
            logging.warning(f"Some socket optimizations failed: {e}")
        
        self.socket.bind(('', self.config.port))
        self.socket.setblocking(False)  # Non-blocking mode
        
        logging.info(f"Timestamped receiver listening on port {self.config.port}")
        
        # Pre-allocate packet buffer
        packet_buffer = bytearray(self.config.packet_bytes + 100)
        last_stats_update = time.time()
        stats_update_interval = 1.0  # Send stats every second
        
        # Calculate samples per packet
        samples_per_packet = self.config.samples_per_packet
        bytes_per_sample = self.config.bytes_per_point * self.config.points_per_sample
        
        while not self.stop_event.is_set():
            try:
                # Non-blocking receive
                try:
                    bytes_received = self.socket.recv_into(packet_buffer)
                    receive_timestamp = time.time()
                except socket.error as e:
                    if e.errno == socket.errno.EAGAIN or e.errno == socket.errno.EWOULDBLOCK:
                        # No data available
                        time.sleep(0.0001)  # Brief sleep to avoid busy-waiting
                        continue
                    else:
                        raise
                
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
                
                # Handle packet counter and detect gaps
                gap_info = None
                if self.last_packet_counter is not None:
                    # Calculate expected counter (handling wrap)
                    expected_counter = (self.last_packet_counter + 1) & 0xFF
                    
                    if packet_counter != expected_counter:
                        # Gap detected
                        if packet_counter > self.last_packet_counter:
                            # Normal gap
                            missing_packets = packet_counter - self.last_packet_counter - 1
                        else:
                            # Counter wrapped
                            missing_packets = (256 - self.last_packet_counter - 1) + packet_counter
                            self.packet_counter_wraps += 1
                        
                        if missing_packets > 0:
                            missing_samples = missing_packets * samples_per_packet
                            self.total_missing_samples += missing_samples
                            
                            gap_info = {
                                'start_packet': self.last_packet_counter,
                                'end_packet': packet_counter,
                                'missing_packets': missing_packets,
                                'missing_samples': missing_samples,
                                'time': receive_timestamp
                            }
                            self.gaps_detected.append(gap_info)
                            
                            logging.warning(f"Gap detected: {missing_packets} packets ({missing_samples} samples) missing")
                    elif packet_counter == 0 and self.last_packet_counter == 255:
                        # Normal wrap
                        self.packet_counter_wraps += 1
                
                self.last_packet_counter = packet_counter
                self.total_packets_expected += 1
                
                # Calculate global packet index
                global_packet_idx = self.packet_counter_wraps * 256 + packet_counter
                
                # Extract data and calculate timestamps
                data_bytes = bytes_received - 4
                actual_samples = data_bytes // bytes_per_sample
                
                # Create sample data with timestamps
                sample_data = []
                for i in range(actual_samples):
                    # Calculate global sample index
                    global_sample_idx = global_packet_idx * samples_per_packet + i
                    
                    # Calculate timestamp for this sample
                    sample_timestamp = self.stream_start_time + (global_sample_idx / self.actual_rate_hz)
                    
                    # Extract sample data
                    sample_start = 4 + i * bytes_per_sample
                    sample_end = sample_start + bytes_per_sample
                    sample_bytes = packet_buffer[sample_start:sample_end]
                    
                    sample_data.append({
                        'data': bytes(sample_bytes),
                        'timestamp': sample_timestamp,
                        'global_idx': global_sample_idx
                    })
                
                self.total_samples_received += actual_samples
                
                # Update statistics
                stats.update_packet(bytes_received, actual_samples, packet_counter)
                
                # Track rate codes
                if not hasattr(stats, 'rate_code_counts'):
                    stats.rate_code_counts = {}
                stats.rate_code_counts[sample_rate_code] = stats.rate_code_counts.get(sample_rate_code, 0) + 1
                
                # Send enhanced packet data to writer queue
                packet_data = {
                    'type': 'timestamped_packet',
                    'raw_data': bytes(packet_buffer[:bytes_received]),
                    'packet_counter': packet_counter,
                    'global_packet_idx': global_packet_idx,
                    'samples': sample_data,
                    'gap_info': gap_info,
                    'is_little_endian': is_little_endian,
                    'has_integrity_check': has_integrity_check,
                    'has_overload': has_overload,
                    'has_error': has_error,
                    'receive_timestamp': receive_timestamp
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
                        'samples_received': self.total_samples_received,
                        'bytes_received': stats.bytes_received,
                        'sequence_errors': stats.sequence_errors,
                        'receive_errors': stats.errors,
                        'missing_samples': self.total_missing_samples,
                        'gaps_detected': len(self.gaps_detected),
                        'packet_counter_wraps': self.packet_counter_wraps,
                        'rate_code_counts': dict(stats.rate_code_counts) if hasattr(stats, 'rate_code_counts') else {},
                        'timing_info': {
                            'stream_start': self.stream_start_time,
                            'actual_rate_hz': self.actual_rate_hz,
                            'total_expected_packets': self.total_packets_expected
                        }
                    }
                    try:
                        self.stats_queue.put_nowait(stats_summary)
                    except queue.Full:
                        pass  # Don't block on stats
                    last_stats_update = current_time
                    
            except Exception as e:
                if not self.stop_event.is_set():
                    stats.errors += 1
                    logging.error(f"Receive error: {e}")
                    
        # Cleanup
        if self.socket:
            self.socket.close()
        
        # Send final stats with gap summary
        final_stats = {
            'packets_received': stats.packets_received,
            'samples_received': self.total_samples_received,
            'bytes_received': stats.bytes_received,
            'sequence_errors': stats.sequence_errors,
            'receive_errors': stats.errors,
            'missing_samples': self.total_missing_samples,
            'gaps_detected': len(self.gaps_detected),
            'gap_details': self.gaps_detected[:10],  # First 10 gaps for summary
            'packet_counter_wraps': self.packet_counter_wraps,
            'rate_code_counts': dict(stats.rate_code_counts) if hasattr(stats, 'rate_code_counts') else {},
            'timing_info': {
                'stream_start': self.stream_start_time,
                'actual_rate_hz': self.actual_rate_hz,
                'total_expected_packets': self.total_packets_expected
            },
            'final': True
        }
        try:
            self.stats_queue.put(final_stats)
        except:
            pass
        
        logging.info("Timestamped receiver process shutting down")


def timestamped_receiver_process_entry(config: StreamConfig, data_queue: mp.Queue, 
                                       stats_queue: mp.Queue, stop_event: mp.Event,
                                       stream_start_time: float, actual_rate_hz: float):
    """Entry point for timestamped receiver process."""
    receiver = TimestampedStreamReceiver(config, data_queue, stats_queue, stop_event,
                                        stream_start_time, actual_rate_hz)
    receiver.run()


class TimestampedBinaryFileWriter(threading.Thread):
    """Writer thread that handles timestamped packets with gap insertion."""
    
    def __init__(self, filename: str, config: StreamConfig, data_queue: mp.Queue, stop_event: mp.Event,
                 actual_rate_hz: Optional[float] = None, rate_divider: Optional[int] = None, 
                 max_rate_hz: Optional[float] = None, detected_little_endian: Optional[bool] = None,
                 detected_integrity_check: Optional[bool] = None, stream_start_time: Optional[float] = None):
        super().__init__(name="TimestampedFileWriter")
        self.filename = filename
        self.config = config
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.samples_written = 0
        self.bytes_written = 0
        self.gaps_inserted = 0
        self.file = None
        
        # Store rate and timing information
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
        if stream_start_time is not None:
            self.config.stream_start_time = stream_start_time
            
        # Track last global sample index for gap detection
        self.last_global_sample_idx = -1
        
    def run(self):
        """Main writer loop with gap handling."""
        # Open file and write header
        self.file = open(self.filename, 'wb')
        self._write_header()
        
        # Prepare NaN values for gap filling
        if self.config.format == 0:  # float32
            nan_bytes = struct.pack('<f', float('nan')) * self.config.points_per_sample
        else:  # int16
            nan_bytes = struct.pack('<h', -32768) * self.config.points_per_sample  # Use minimum int16 as "missing"
        
        buffer = bytearray()
        buffer_size = 1024 * 1024  # 1MB buffer
        
        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Get data from queue with timeout
                packet_data = self.data_queue.get(timeout=0.1)
                
                if packet_data.get('type') != 'timestamped_packet':
                    logging.warning(f"Unexpected packet type: {packet_data.get('type')}")
                    continue
                
                # Check for gap before this packet
                samples = packet_data['samples']
                if samples:
                    first_sample_idx = samples[0]['global_idx']
                    
                    if self.last_global_sample_idx >= 0:
                        expected_idx = self.last_global_sample_idx + 1
                        gap_samples = first_sample_idx - expected_idx
                        
                        if gap_samples > 0:
                            # Insert NaN values for missing samples
                            logging.info(f"Inserting {gap_samples} NaN samples for gap")
                            for _ in range(gap_samples):
                                buffer.extend(nan_bytes)
                                self.gaps_inserted += 1
                                self.bytes_written += len(nan_bytes)
                    
                    # Write actual sample data
                    for sample in samples:
                        buffer.extend(sample['data'])
                        self.samples_written += 1
                        self.bytes_written += len(sample['data'])
                        self.last_global_sample_idx = sample['global_idx']
                
                # Flush if buffer is large enough
                if len(buffer) >= buffer_size:
                    self.file.write(buffer)
                    buffer = bytearray()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Timestamped writer error: {e}")
                import traceback
                traceback.print_exc()
                
        # Final flush
        if buffer:
            self.file.write(buffer)
            
        # Close file
        if self.file:
            self.file.close()
            
        logging.info(f"Timestamped writer finished: {self.samples_written} samples written, "
                    f"{self.gaps_inserted} gap samples inserted, {self.bytes_written} bytes total")
        
    def _write_header(self):
        """Write file header with metadata including timing info."""
        header = {
            'version': 2,  # Version 2 includes timing information
            'timestamp': time.time(),
            'channel': self.config.channel.value,
            'format': self.config.format.value,
            'points_per_sample': self.config.points_per_sample,
            'bytes_per_point': self.config.bytes_per_point,
            'packet_size': self.config.packet_size.value,
            'port': self.config.port,
            'use_little_endian': self.config.use_little_endian,
            'use_integrity_check': self.config.use_integrity_check,
            'actual_rate_hz': getattr(self.config, 'actual_rate_hz', None),
            'rate_divider': getattr(self.config, 'rate_divider', None),
            'max_rate_hz': getattr(self.config, 'max_rate_hz', None),
            'detected_little_endian': getattr(self.config, 'detected_little_endian', None),
            'detected_integrity_check': getattr(self.config, 'detected_integrity_check', None),
            'stream_start_time': getattr(self.config, 'stream_start_time', None),
            'receiver_type': 'timestamped',
            'has_gap_insertion': True,
            'gap_fill_value': 'NaN' if self.config.format == 0 else -32768
        }
        
        # Write header size (4 bytes) and header data
        header_bytes = str(header).encode('utf-8')
        self.file.write(struct.pack('<I', len(header_bytes)))
        self.file.write(header_bytes)


class SR860ConfiguredController:
    """Controller that respects SR860's configuration without optimization."""
    
    def __init__(self, ip: str):
        self.sr860 = SR860Class(ip)
        
    def connect(self) -> bool:
        """Connect to SR860."""
        return self.sr860.connect()
    
    def read_current_configuration(self) -> Dict[str, Any]:
        """Read the current SR860 streaming configuration."""
        return self.sr860.read_current_configuration()
    
    def apply_configuration(self, config: StreamConfig, rate_divider: Optional[int] = None) -> Dict[str, Any]:
        """Apply a new streaming configuration."""
        return self.sr860.apply_configuration(config, rate_divider)
    
    def start_streaming(self):
        """Start SR860 streaming."""
        self.sr860.start_streaming()
    
    def stop_streaming(self):
        """Stop SR860 streaming."""
        self.sr860.stop_streaming()
    
    def close(self):
        """Close connection."""
        self.sr860.close()

    def set_time_constant(self, time_constant_idx: int) -> Optional[float]:
        """Set the time constant and return the new maximum streaming rate."""
        return self.sr860.set_time_constant(time_constant_idx)


def configured_streaming_worker(ip: str, config: Optional[StreamConfig] = None, 
                               rate_divider: Optional[int] = None, duration: float = 10.0,
                               use_current_config: bool = True, time_constant: Optional[int] = None,
                               auto_analysis: bool = True, detailed_loss_analysis: bool = False):
    """Worker function for streaming at configured rate using timestamped receiver."""
    
    logging.info(f"Timestamped streaming worker started for {ip}")
    
    # Initialize start_time early to avoid UnboundLocalError
    start_time = time.time()
    actual_rate = 0  # Initialize early to avoid UnboundLocalError
    stream_start_time = None  # Time when STREAM ON is issued
    
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
        logging.info(f"Receiver:          Timestamped receiver with packet counter tracking")
        logging.info(f"Actual rate:       {actual_rate:,.0f} Hz")
        logging.info(f"Expected duration: {duration:.1f} s")
        logging.info(f"Expected samples:  {int(actual_rate * duration):,}")
        logging.info(f"Expected data:     {config_info['mbps'] * duration * 0.125:.1f} MB")
        
        # Always use timestamped receiver
        logging.info("\nUsing TIMESTAMPED receiver with packet counter tracking")
        # Capture stream start time just before starting
        stream_start_time = time.time()
        
        # Start timestamped receiver process
        receiver_process = mp_context.Process(
            target=timestamped_receiver_process_entry,
            args=(config, data_queue, stats_queue, stop_event, stream_start_time, actual_rate),
            name=f"SR860_TimestampedReceiver_{ip}"
        )
        receiver_process.start()
        logging.info(f"Started receiver process (PID: {receiver_process.pid})")
        
        # Start timestamped writer thread
        writer_thread = TimestampedBinaryFileWriter(
            filename, config, data_queue, stop_event, 
            actual_rate_hz=actual_rate, 
            rate_divider=config_info['rate_divider'], 
            max_rate_hz=config_info['max_rate_hz'],
            detected_little_endian=config_info['use_little_endian'],
            detected_integrity_check=config_info['use_integrity_check'],
            stream_start_time=stream_start_time
        )
        writer_thread.start()
        logging.info("Started writer thread")
        
        # Give receiver time to initialize
        time.sleep(1)
        
        # Start streaming - capture exact time
        stream_start_time = time.time()
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
            'header_info': {},
            'missing_samples': 0,
            'gaps_detected': 0,
            'gap_details': []
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
                        
                        # Update gap information
                        if 'missing_samples' in stats_update:
                            total_stats['missing_samples'] = stats_update['missing_samples']
                        if 'gaps_detected' in stats_update:
                            total_stats['gaps_detected'] = stats_update['gaps_detected']
                        
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
                
                # Include gap info
                gap_info = ""
                if total_stats.get('gaps_detected', 0) > 0:
                    gap_info = f", gaps: {total_stats['gaps_detected']}"
                
                logging.info(
                    f"Progress: {total_stats['samples_received']:,} samples, "
                    f"{actual_mbps:.1f} Mbps, "
                    f"efficiency: {efficiency:.1f}%, "
                    f"rate: {rate_stability}{gap_info}"
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
        logging.info("TIMESTAMPED STREAMING FINAL STATISTICS")
        logging.info("="*60)
        
        logging.info(f"\nConfiguration:")
        logging.info(f"  Mode:              {'Current Config' if use_current_config else 'User Specified'}")
        logging.info(f"  Receiver:          Timestamped receiver with packet counter tracking")
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
        
        # Gap analysis
        if 'gaps_detected' in total_stats:
            logging.info("\nGap Analysis:")
            logging.info(f"  Gaps detected:    {total_stats['gaps_detected']}")
            logging.info(f"  Missing samples:  {total_stats.get('missing_samples', 0):,}")
            
            # Show first few gaps if any
            if 'gap_details' in total_stats and total_stats['gap_details']:
                logging.info(f"  First {min(5, len(total_stats['gap_details']))} gaps:")
                for i, gap in enumerate(total_stats['gap_details'][:5]):
                    logging.info(f"    Gap {i+1}: {gap['missing_packets']} packets "
                               f"({gap['missing_samples']} samples) between packets "
                               f"{gap['start_packet']}->{gap['end_packet']}")
        
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
        
        # Display detailed loss analysis if losses occurred and requested
        if has_packet_loss and detailed_loss_analysis:
            logging.info("\n" + "="*60)
            logging.info("DETAILED SAMPLE LOSS ANALYSIS")
            logging.info("="*60)
            
            # Get the detailed loss report from the receiver's stats
            try:
                # We need to get the stats from the receiver process
                # For now, we'll show a summary of what we have
                logging.info(f"Total sequence errors: {total_stats['sequence_errors']}")
                logging.info(f"Missing samples: {total_stats.get('missing_samples', 0):,}")
                logging.info(f"Gaps detected: {total_stats.get('gaps_detected', 0)}")
                
                if 'gap_details' in total_stats and total_stats['gap_details']:
                    logging.info("\nRecent gap details:")
                    for i, gap in enumerate(total_stats['gap_details'][:5]):
                        logging.info(f"  Gap {i+1}: {gap['missing_packets']} packets "
                                   f"({gap['missing_samples']} samples) between packets "
                                   f"{gap['start_packet']}->{gap['end_packet']}")
                
                logging.info("\nUse --detailed-loss-analysis for full pattern analysis")
                
            except Exception as e:
                logging.warning(f"Could not generate detailed loss report: {e}")
        elif has_packet_loss:
            logging.info(f"\n⚠️  SAMPLE LOSSES DETECTED:")
            logging.info(f"   {total_stats['sequence_errors']} sequence errors detected")
            logging.info(f"   Use --detailed-loss-analysis for full analysis")

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
    parser.add_argument("--ip", default="192.168.1.18", help="SR860 IP address")
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
    
    # Detailed loss analysis option
    parser.add_argument("--detailed-loss-analysis", action="store_true",
                        help="Enable detailed loss pattern analysis and reporting")
    
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
            auto_analysis=not args.no_auto_analysis,
            detailed_loss_analysis=args.detailed_loss_analysis
        )
    except KeyboardInterrupt:
        logging.info("Streaming stopped by user")


if __name__ == "__main__":
    main() 