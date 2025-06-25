#!/usr/bin/env python3
"""SR860 Maximum Rate Streaming DAQ with Binary Output.

Uses the SR860's native streaming capabilities for maximum performance:
- Native ethernet streaming up to 1.25 MHz
- Binary file output for efficiency
- UDP receiver for streaming data
- Automatic rate optimization
"""

import argparse
import logging
import multiprocessing as mp
import numpy as np
import os
import queue
import signal
import socket
import struct
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyvisa

logging.basicConfig(
    format="%(levelname)-8s [%(processName)-12s:%(threadName)-10s] %(asctime)s | %(message)s",
    datefmt="%H:%M:%S.%f",
    level=logging.INFO,
)


###############################################################################
# Data Structures
###############################################################################


class StreamChannel(Enum):
    """SR860 streaming channel configurations."""
    X = 0           # X only
    XY = 1          # X and Y
    RT = 2          # R and Theta
    XYRT = 3        # X, Y, R, and Theta


class StreamFormat(Enum):
    """SR860 streaming data formats."""
    FLOAT32 = 0     # 4 bytes per point
    INT16 = 1       # 2 bytes per point


class PacketSize(Enum):
    """SR860 ethernet packet sizes."""
    SIZE_1024 = 0   # 1024 bytes
    SIZE_512 = 1    # 512 bytes
    SIZE_256 = 2    # 256 bytes
    SIZE_128 = 3    # 128 bytes


@dataclass
class StreamConfig:
    """SR860 streaming configuration."""
    channel: StreamChannel = StreamChannel.XYRT
    format: StreamFormat = StreamFormat.FLOAT32
    packet_size: PacketSize = PacketSize.SIZE_1024
    port: int = 1865
    use_little_endian: bool = True
    use_integrity_check: bool = True
    
    @property
    def option_value(self) -> int:
        """Calculate STREAMOPTION value."""
        value = 0
        if self.use_little_endian:
            value |= 1
        if self.use_integrity_check:
            value |= 2
        return value
    
    @property
    def bytes_per_point(self) -> int:
        """Bytes per data point."""
        return 4 if self.format == StreamFormat.FLOAT32 else 2
    
    @property
    def points_per_sample(self) -> int:
        """Number of values per sample."""
        return {
            StreamChannel.X: 1,
            StreamChannel.XY: 2,
            StreamChannel.RT: 2,
            StreamChannel.XYRT: 4
        }[self.channel]
    
    @property
    def packet_bytes(self) -> int:
        """Packet size in bytes."""
        return {
            PacketSize.SIZE_1024: 1024,
            PacketSize.SIZE_512: 512,
            PacketSize.SIZE_256: 256,
            PacketSize.SIZE_128: 128
        }[self.packet_size]
        
    @property
    def samples_per_packet(self) -> int:
        """Number of samples that fit in one packet."""
        bytes_per_sample = self.bytes_per_point * self.points_per_sample
        # Account for 4-byte header in SR860 packets
        available_data_bytes = self.packet_bytes - 4
        return available_data_bytes // bytes_per_sample


###############################################################################
# Performance Statistics
###############################################################################


@dataclass
class StreamingStats:
    """Detailed streaming statistics."""
    start_time: float
    packets_received: int = 0
    bytes_received: int = 0
    samples_received: int = 0
    errors: int = 0
    last_packet_time: float = 0
    packet_intervals: deque = None
    sequence_errors: int = 0
    last_sequence: int = -1
    
    def __post_init__(self):
        if self.packet_intervals is None:
            self.packet_intervals = deque(maxlen=1000)  # Keep last 1000 intervals
            
    def update_packet(self, packet_size: int, samples: int, sequence: Optional[int] = None):
        """Update statistics with new packet."""
        current_time = time.time()
        
        self.packets_received += 1
        self.bytes_received += packet_size
        self.samples_received += samples
        
        # Track packet timing
        if self.last_packet_time > 0:
            interval = current_time - self.last_packet_time
            self.packet_intervals.append(interval)
            
        self.last_packet_time = current_time
        
        # Check sequence if provided
        if sequence is not None and self.last_sequence >= 0:
            expected = (self.last_sequence + 1) % 65536  # Assuming 16-bit sequence
            if sequence != expected:
                self.sequence_errors += 1
                logging.warning(f"Sequence error: expected {expected}, got {sequence}")
        
        if sequence is not None:
            self.last_sequence = sequence
            
    def get_summary(self, expected_rate: float, expected_duration: float) -> Dict[str, Any]:
        """Get comprehensive statistics summary."""
        actual_duration = time.time() - self.start_time
        
        # Calculate rates
        actual_sample_rate = self.samples_received / actual_duration if actual_duration > 0 else 0
        actual_packet_rate = self.packets_received / actual_duration if actual_duration > 0 else 0
        actual_mbps = (self.bytes_received * 8) / (actual_duration * 1e6) if actual_duration > 0 else 0
        
        # Expected values
        expected_samples = int(expected_rate * expected_duration)
        expected_packets = expected_samples // (1024 // 16)  # Rough estimate
        expected_bytes = expected_samples * 16  # 4 channels * 4 bytes
        expected_mbps = (expected_bytes * 8) / (expected_duration * 1e6)
        
        # Packet timing statistics
        if self.packet_intervals:
            intervals = list(self.packet_intervals)
            avg_interval = np.mean(intervals)
            std_interval = np.std(intervals)
            min_interval = np.min(intervals)
            max_interval = np.max(intervals)
            jitter = max_interval - min_interval
        else:
            avg_interval = std_interval = min_interval = max_interval = jitter = 0
            
        return {
            # Actual values
            'duration_actual': actual_duration,
            'samples_actual': self.samples_received,
            'packets_actual': self.packets_received,
            'bytes_actual': self.bytes_received,
            'sample_rate_actual': actual_sample_rate,
            'packet_rate_actual': actual_packet_rate,
            'mbps_actual': actual_mbps,
            
            # Expected values
            'duration_expected': expected_duration,
            'samples_expected': expected_samples,
            'packets_expected': expected_packets,
            'bytes_expected': expected_bytes,
            'sample_rate_expected': expected_rate,
            'mbps_expected': expected_mbps,
            
            # Performance metrics
            'samples_lost': max(0, expected_samples - self.samples_received),
            'sample_loss_percent': 100 * (1 - self.samples_received / expected_samples) if expected_samples > 0 else 0,
            'efficiency_percent': 100 * actual_sample_rate / expected_rate if expected_rate > 0 else 0,
            
            # Timing/Latency
            'avg_packet_interval_ms': avg_interval * 1000,
            'std_packet_interval_ms': std_interval * 1000,
            'min_packet_interval_ms': min_interval * 1000,
            'max_packet_interval_ms': max_interval * 1000,
            'packet_jitter_ms': jitter * 1000,
            
            # Errors
            'receive_errors': self.errors,
            'sequence_errors': self.sequence_errors,
        }


###############################################################################
# Binary File Writer
###############################################################################


class BinaryFileWriter:
    """High-performance binary file writer with buffering."""
    
    def __init__(self, filename: str, config: StreamConfig, buffer_size: int = 1024*1024):
        self.filename = filename
        self.config = config
        self.buffer_size = buffer_size
        self.file = None
        self.buffer = bytearray()
        self.samples_written = 0
        self.bytes_written = 0
        self.lock = threading.Lock()
        
        # Write header
        self._open_file()
        self._write_header()
        
    def _open_file(self):
        """Open binary file for writing."""
        self.file = open(self.filename, 'wb')
        
    def _write_header(self):
        """Write file header with metadata."""
        header = {
            'version': 1,
            'timestamp': time.time(),
            'channel': self.config.channel.value,
            'format': self.config.format.value,
            'points_per_sample': self.config.points_per_sample,
            'bytes_per_point': self.config.bytes_per_point,
        }
        
        # Write header size (4 bytes) and header data
        header_bytes = str(header).encode('utf-8')
        self.file.write(struct.pack('<I', len(header_bytes)))
        self.file.write(header_bytes)
        
    def write(self, data: bytes, sample_count: int):
        """Write data to buffer, flush when full."""
        with self.lock:
            self.buffer.extend(data)
            self.samples_written += sample_count
            self.bytes_written += len(data)
            
            if len(self.buffer) >= self.buffer_size:
                self._flush()
                
    def _flush(self):
        """Flush buffer to disk."""
        if self.buffer and self.file:
            self.file.write(self.buffer)
            self.buffer = bytearray()
            
    def close(self):
        """Close file, flushing remaining data."""
        with self.lock:
            self._flush()
            if self.file:
                self.file.close()
                self.file = None
                
        logging.info(f"Binary file closed: {self.filename}")
        logging.info(f"  Samples written: {self.samples_written}")
        logging.info(f"  Bytes written: {self.bytes_written}")


###############################################################################
# UDP Stream Receiver
###############################################################################


class StreamReceiver:
    """High-performance UDP receiver for SR860 streaming data."""
    
    def __init__(self, config: StreamConfig, writer: BinaryFileWriter, stats: StreamingStats):
        self.config = config
        self.writer = writer
        self.stats = stats
        self.socket = None
        self.running = False
        
    def start(self):
        """Start receiving UDP packets with optimized network settings."""
        self.running = True
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Aggressive socket optimization for high-throughput streaming
        try:
            # Increase receive buffer to maximum
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16*1024*1024)  # 16MB buffer
            
            # Set socket to reuse address
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Enable high priority socket (if supported)
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)
            except:
                pass  # Not supported on all systems
                
            # Disable Nagle algorithm for lower latency
            try:
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except:
                pass  # UDP doesn't use TCP options
                
        except Exception as e:
            logging.warning(f"Some socket optimizations failed: {e}")
        
        self.socket.bind(('', self.config.port))
        self.socket.settimeout(0.01)  # Reduce timeout to 10ms for faster response
        
        # Get actual buffer size
        try:
            actual_rcvbuf = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            logging.info(f"UDP receiver buffer set to: {actual_rcvbuf / 1024 / 1024:.1f} MB")
        except:
            pass
            
        logging.info(f"High-performance UDP receiver listening on port {self.config.port}")
        logging.info(f"Expected samples per packet: {self.config.samples_per_packet}")
        
        # Packet processing loop with minimal overhead
        packet_buffer = bytearray(self.config.packet_bytes + 100)  # Pre-allocate buffer
        last_overload_warning = 0
        overload_warning_interval = 1.0  # Only warn about overloads once per second
        
        while self.running:
            try:
                # Use recv_into for zero-copy operation
                bytes_received = self.socket.recv_into(packet_buffer)
                current_time = time.time()
                
                # Fast packet validation
                if bytes_received < 4:
                    logging.warning(f"Packet too short: {bytes_received} bytes")
                    continue
                    
                # Parse header efficiently (avoid struct overhead for simple operations)
                header_bytes = packet_buffer[:4]
                header = (header_bytes[0] << 24) | (header_bytes[1] << 16) | (header_bytes[2] << 8) | header_bytes[3]
                
                # Extract header fields
                packet_counter = header & 0xFF
                packet_content = (header >> 8) & 0xF
                packet_length = (header >> 12) & 0xF
                sample_rate_code = (header >> 16) & 0xFF
                status = (header >> 24) & 0xFF
                
                # Check for errors (but don't spam warnings)
                overload = bool(status & 0x01)
                error = bool(status & 0x02)
                
                # Throttle overload warnings to avoid log spam
                if overload and (current_time - last_overload_warning) > overload_warning_interval:
                    logging.debug(f"SR860 overload status in packet {packet_counter} (throttled)")
                    last_overload_warning = current_time
                    
                if error:
                    logging.warning(f"SR860 error bit set in packet {packet_counter}")
                
                # Get data portion (skip 4-byte header)
                data_bytes = bytes_received - 4
                
                # Calculate samples in this packet
                bytes_per_sample = self.config.bytes_per_point * self.config.points_per_sample
                samples_in_packet = data_bytes // bytes_per_sample
                
                # Update statistics with sequence number for loss detection
                self.stats.update_packet(bytes_received, samples_in_packet, packet_counter)
                
                # Track rate changes for diagnostic purposes
                if not hasattr(self.stats, 'last_rate_code'):
                    self.stats.last_rate_code = None
                    self.stats.rate_code_counts = {}
                    
                if self.stats.last_rate_code != sample_rate_code:
                    actual_rate_hz = 1.25e6 / (2 ** sample_rate_code) if sample_rate_code < 32 else 0
                    if self.stats.last_rate_code is not None:
                        logging.info(f"SR860 rate change: code {sample_rate_code} = {actual_rate_hz:,.0f} Hz in packet {packet_counter}")
                    self.stats.last_rate_code = sample_rate_code
                    
                self.stats.rate_code_counts[sample_rate_code] = self.stats.rate_code_counts.get(sample_rate_code, 0) + 1
                
                # Write only the data portion to file (not the header)
                # Use memoryview for zero-copy slice
                data_view = memoryview(packet_buffer)[4:4+data_bytes]
                self.writer.write(data_view, samples_in_packet)
                
            except socket.timeout:
                continue  # Short timeout for responsive shutdown
            except BlockingIOError:
                continue  # Non-blocking operation would block
            except Exception as e:
                self.stats.errors += 1
                logging.error(f"Receive error: {e}")
                if not self.running:
                    break
                    
    def stop(self):
        """Stop receiving."""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass


###############################################################################
# SR860 Streaming Controller
###############################################################################


class SR860StreamController:
    """Controls SR860 streaming configuration and state."""
    
    def __init__(self, ip: str, config: StreamConfig):
        self.ip = ip
        self.config = config
        self.inst = None
        self.max_rate = None
        self.actual_rate = None
        
    def _optimize_system_settings(self):
        """Apply system-level optimizations for high-throughput streaming."""
        logging.info("Applying system optimizations for high-throughput streaming...")
        
        try:
            import subprocess
            import os
            
            # Increase system UDP buffer sizes
            commands = [
                # Increase UDP receive buffer limits
                "sudo sysctl -w net.core.rmem_max=134217728",      # 128MB
                "sudo sysctl -w net.core.rmem_default=67108864",   # 64MB
                "sudo sysctl -w net.core.netdev_max_backlog=5000", # Increase network device queue
                "sudo sysctl -w net.core.netdev_budget=600",       # Process more packets per NAPI poll
            ]
            
            for cmd in commands:
                try:
                    result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        logging.debug(f"Applied: {cmd}")
                    else:
                        logging.debug(f"Failed to apply: {cmd} - {result.stderr.strip()}")
                except subprocess.TimeoutExpired:
                    logging.debug(f"Timeout applying: {cmd}")
                except Exception as e:
                    logging.debug(f"Error applying {cmd}: {e}")
                    
            # Set process priority (if possible)
            try:
                os.nice(-10)  # Higher priority
                logging.info("Increased process priority")
            except:
                logging.debug("Could not increase process priority (requires privileges)")
                
        except Exception as e:
            logging.debug(f"System optimization failed: {e}")
            
    def _calculate_throttled_rate(self) -> int:
        """Calculate optimal rate divider to prevent packet loss."""
        if not self.max_rate:
            return 0
            
        # Calculate expected data rates
        bytes_per_sample = self.config.bytes_per_point * self.config.points_per_sample
        samples_per_packet = self.config.samples_per_packet
        
        # Very conservative estimates based on empirical testing
        # Start with much lower limits to ensure stable operation
        
        # Conservative packet processing limit for Python UDP
        # Based on testing, Python can reliably handle ~5K packets/sec
        max_packets_per_second = 5000  # Very conservative for stable operation
        max_samples_per_second = max_packets_per_second * samples_per_packet
        
        # Conservative network bandwidth limit (assume 10 Mbps effective)
        max_bandwidth_bps = 10e6 * 0.5 / 8  # 0.625 MB/s (very conservative)
        max_samples_by_bandwidth = max_bandwidth_bps / bytes_per_sample
        
        # Take the most conservative limit
        sustainable_rate = min(max_samples_per_second, max_samples_by_bandwidth)
        
        # Add additional safety margin (50%)
        sustainable_rate *= 0.5
        
        # Start with a minimum rate divider of 3 (1/8th rate) for safety
        rate_divider = max(3, 0)
        test_rate = self.max_rate / (2 ** rate_divider)
        
        # Find appropriate rate divider
        while test_rate > sustainable_rate and rate_divider < 20:
            rate_divider += 1
            test_rate = self.max_rate / (2 ** rate_divider)
            
        logging.info(f"Conservative throttling analysis:")
        logging.info(f"  Max rate: {self.max_rate:,.0f} Hz")
        logging.info(f"  Conservative packet limit: {max_samples_per_second:,.0f} Hz")
        logging.info(f"  Conservative bandwidth limit: {max_samples_by_bandwidth:,.0f} Hz")
        logging.info(f"  Sustainable rate (with 50% margin): {sustainable_rate:,.0f} Hz")
        logging.info(f"  Selected rate: {test_rate:,.0f} Hz (divider: 2^{rate_divider})")
        
        return rate_divider
        
    def connect(self) -> bool:
        """Connect to SR860 with system optimizations."""
        # Apply system optimizations first
        self._optimize_system_settings()
        
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
            
    def configure_streaming(self, auto_throttle: bool = True) -> Dict[str, Any]:
        """Configure SR860 for sustainable high-rate streaming."""
        if not self.inst:
            raise RuntimeError("Not connected")
            
        # Get maximum streaming rate
        self.max_rate = float(self.inst.query("STREAMRATEMAX?").strip())
        logging.info(f"Maximum streaming rate: {self.max_rate} Hz")
        
        # Calculate throttled rate if requested
        if auto_throttle:
            rate_divider = self._calculate_throttled_rate()
        else:
            rate_divider = 0  # Maximum rate
            
        # Configure streaming parameters
        logging.info(f"Setting channel to {self.config.channel.name} ({self.config.channel.value})")
        self.inst.write(f"STREAMCH {self.config.channel.value}")
        time.sleep(0.1)
        
        self.inst.write(f"STREAMFMT {self.config.format.value}")
        self.inst.write(f"STREAMPCKT {self.config.packet_size.value}")
        self.inst.write(f"STREAMPORT {self.config.port}")
        self.inst.write(f"STREAMOPTION {self.config.option_value}")
        
        # Set calculated rate
        self.inst.write(f"STREAMRATE {rate_divider}")
        self.actual_rate = self.max_rate / (2 ** rate_divider) if rate_divider > 0 else self.max_rate
        
        # Verify configuration
        time.sleep(0.2)
        ch = int(self.inst.query("STREAMCH?").strip())
        fmt = int(self.inst.query("STREAMFMT?").strip())
        pkt = int(self.inst.query("STREAMPCKT?").strip())
        port = int(self.inst.query("STREAMPORT?").strip())
        rate_n = int(self.inst.query("STREAMRATE?").strip())
        
        # Verify channel was set correctly
        if ch != self.config.channel.value:
            logging.warning(f"Channel mismatch: requested {self.config.channel.value}, got {ch}")
        
        config_info = {
            'max_rate_hz': self.max_rate,
            'actual_rate_hz': self.actual_rate,
            'rate_divider': rate_n,
            'throttled': auto_throttle,
            'channel': StreamChannel(ch).name,
            'channel_value': ch,
            'format': StreamFormat(fmt).name,
            'packet_size': self.config.packet_bytes,
            'port': port,
            'bytes_per_second': self.actual_rate * self.config.bytes_per_point * self.config.points_per_sample,
            'mbps': (self.actual_rate * self.config.bytes_per_point * self.config.points_per_sample * 8) / 1e6,
            'samples_per_packet': self.config.samples_per_packet,
            'packets_per_second': self.actual_rate / self.config.samples_per_packet if self.config.samples_per_packet > 0 else 0
        }
        
        logging.info(f"Streaming configured: {config_info}")
        return config_info
        
    def start_streaming(self):
        """Start SR860 streaming."""
        if not self.inst:
            raise RuntimeError("Not connected")
            
        # NOTE: SR860 sends packets to the IP that sends STREAM ON command
        # No STREAMHOST command needed - this is automatic!
        self.inst.write("STREAM ON")
        logging.info("SR860 streaming started - packets will be sent to this computer")
        
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


###############################################################################
# Main Streaming Process
###############################################################################


def streaming_worker(ip: str, config: StreamConfig, duration: float, auto_throttle: bool = True):
    """Worker process for high-speed streaming with packet loss prevention."""
    
    logging.info(f"Streaming worker started for {ip}")
    if auto_throttle:
        logging.info("Auto-throttling enabled to prevent packet loss")
    else:
        logging.info("⚠️  Auto-throttling disabled - maximum rate mode")
    
    # Create binary file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    throttle_suffix = "_throttled" if auto_throttle else "_max"
    
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    filename = data_dir / f"sr860_stream_{ip.replace('.', '_')}_{timestamp}{throttle_suffix}.bin"
    
    # Initialize statistics
    stats = StreamingStats(start_time=time.time())
    
    # Create writer
    writer = BinaryFileWriter(filename, config)
    
    # Create receiver
    receiver = StreamReceiver(config, writer, stats)
    
    # Create controller
    controller = SR860StreamController(ip, config)
    
    try:
        # Connect and configure
        if not controller.connect():
            raise RuntimeError("Failed to connect to SR860")
            
        config_info = controller.configure_streaming(auto_throttle=auto_throttle)
        expected_rate = config_info['actual_rate_hz']
        
        logging.info(f"Configured for {expected_rate:,.0f} Hz streaming")
        logging.info(f"Expected packet rate: {config_info['packets_per_second']:.1f} packets/s")
        logging.info(f"Expected data rate: {config_info['mbps']:.1f} Mbps")
        
        # Start receiver thread
        receiver_thread = threading.Thread(target=receiver.start, name="Receiver")
        receiver_thread.start()
        
        # Give receiver time to start
        time.sleep(0.5)
        
        # Start streaming
        controller.start_streaming()
        
        # Monitor for duration
        logging.info(f"Streaming for {duration} seconds..." if duration > 0 else "Streaming indefinitely...")
        
        last_log_time = time.time()
        stable_start_time = None
        
        while True:
            elapsed = time.time() - stats.start_time
            
            if duration > 0 and elapsed >= duration:
                break
                
            # Check if streaming is stable (receiving packets)
            if stats.packets_received > 10 and stable_start_time is None:
                stable_start_time = time.time()
                logging.info("✅ Streaming stable - packets being received successfully")
            
            # If no packets received for 2 seconds, something is wrong
            if stats.packets_received == 0 and elapsed > 2.0:
                logging.warning("⚠️  No packets received after 2 seconds - stopping")
                break
                
            # Print statistics every 5 seconds
            current_time = time.time()
            if current_time - last_log_time >= 5.0:
                summary = stats.get_summary(expected_rate, elapsed)
                logging.info(
                    f"Progress: {stats.packets_received} packets, "
                    f"{stats.samples_received} samples, "
                    f"{summary['mbps_actual']:.1f} Mbps, "
                    f"efficiency: {summary['efficiency_percent']:.1f}%, "
                    f"loss: {summary['sample_loss_percent']:.1f}%"
                )
                last_log_time = current_time
                
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        logging.info("Streaming interrupted")
    except Exception as e:
        logging.error(f"Streaming error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Stop everything
        controller.stop_streaming()
        receiver.stop()
        
        if receiver_thread.is_alive():
            receiver_thread.join(timeout=2.0)
            
        writer.close()
        controller.close()
        
        # Final statistics
        final_duration = time.time() - stats.start_time
        summary = stats.get_summary(expected_rate, duration if duration > 0 else final_duration)
        
        logging.info("\n" + "="*60)
        if auto_throttle:
            logging.info("THROTTLED STREAMING FINAL STATISTICS")
        else:
            logging.info("MAXIMUM RATE STREAMING FINAL STATISTICS") 
        logging.info("="*60)
        
        logging.info("\nConfiguration:")
        logging.info(f"  Mode: {'Throttled' if auto_throttle else 'Maximum Rate'}")
        logging.info(f"  Rate divider: 2^{config_info['rate_divider']}")
        
        logging.info("\nDuration:")
        logging.info(f"  Expected: {summary['duration_expected']:.1f} s")
        logging.info(f"  Actual:   {summary['duration_actual']:.1f} s")
        
        logging.info("\nSamples:")
        logging.info(f"  Expected: {summary['samples_expected']:,}")
        logging.info(f"  Actual:   {summary['samples_actual']:,}")
        logging.info(f"  Lost:     {summary['samples_lost']:,} ({summary['sample_loss_percent']:.1f}%)")
        
        logging.info("\nSample Rate:")
        logging.info(f"  Expected: {summary['sample_rate_expected']:.1f} Hz")
        logging.info(f"  Actual:   {summary['sample_rate_actual']:.1f} Hz")
        logging.info(f"  Efficiency: {summary['efficiency_percent']:.1f}%")
        
        logging.info("\nData Rate:")
        logging.info(f"  Expected: {summary['mbps_expected']:.1f} Mbps")
        logging.info(f"  Actual:   {summary['mbps_actual']:.1f} Mbps")
        
        logging.info("\nPacket Statistics:")
        logging.info(f"  Received: {summary['packets_actual']:,}")
        logging.info(f"  Rate:     {summary['packet_rate_actual']:.1f} packets/s")
        
        logging.info("\nPacket Timing (Latency/Jitter):")
        logging.info(f"  Average interval: {summary['avg_packet_interval_ms']:.1f} ms")
        logging.info(f"  Std deviation:    {summary['std_packet_interval_ms']:.1f} ms")
        logging.info(f"  Min interval:     {summary['min_packet_interval_ms']:.1f} ms")
        logging.info(f"  Max interval:     {summary['max_packet_interval_ms']:.1f} ms")
        logging.info(f"  Jitter:          {summary['packet_jitter_ms']:.1f} ms")
        
        logging.info("\nErrors:")
        logging.info(f"  Receive errors:  {summary['receive_errors']}")
        logging.info(f"  Sequence errors: {summary['sequence_errors']}")
        
        # Rate distribution analysis
        if hasattr(stats, 'rate_code_counts') and stats.rate_code_counts:
            logging.info("\nSR860 Rate Analysis:")
            total_packets = sum(stats.rate_code_counts.values())
            for rate_code in sorted(stats.rate_code_counts.keys()):
                count = stats.rate_code_counts[rate_code]
                rate_hz = 1.25e6 / (2 ** rate_code) if rate_code < 32 else 0
                percentage = (count / total_packets) * 100 if total_packets > 0 else 0
                logging.info(f"  Rate code {rate_code}: {rate_hz:>10,.0f} Hz - {count:>6,} packets ({percentage:>5.1f}%)")
            
            unique_rates = len(stats.rate_code_counts)
            if unique_rates == 1:
                logging.info(f"  Rate stability:  STABLE - Single rate maintained")
            elif unique_rates <= 3:
                logging.info(f"  Rate stability:  MODERATE - {unique_rates} different rates")
            else:
                logging.info(f"  Rate stability:  UNSTABLE - {unique_rates} different rates")
        
        logging.info(f"\nOutput file: {filename}")
        logging.info(f"File size: {writer.bytes_written/1e6:.1f} MB")
        
        # Enhanced performance assessment
        has_packet_loss = summary['sequence_errors'] > 0
        has_rate_variation = hasattr(stats, 'rate_code_counts') and len(stats.rate_code_counts) > 1
        
        logging.info("\n" + "="*50)
        logging.info("PERFORMANCE DIAGNOSIS")
        logging.info("="*50)
        
        if not has_packet_loss and summary['efficiency_percent'] >= 95:
            logging.info("\n✅ EXCELLENT PERFORMANCE!")
            logging.info(f"   • No packet loss detected")
            logging.info(f"   • {summary['efficiency_percent']:.1f}% efficiency")
            if has_rate_variation:
                logging.info(f"   • SR860 used multiple rates (normal behavior)")
        elif not has_packet_loss and summary['efficiency_percent'] >= 80:
            logging.info("\n✅ GOOD PERFORMANCE - Rate Inconsistency")
            logging.info(f"   • No packet loss detected")
            logging.info(f"   • {summary['efficiency_percent']:.1f}% efficiency due to SR860 rate variation")
            logging.info(f"   • This is SR860 internal behavior, not network issues")
        elif not has_packet_loss:
            logging.info("\n⚠️  SR860 RATE INCONSISTENCY")
            logging.info(f"   • No packet loss - all transmitted packets received")
            logging.info(f"   • {summary['efficiency_percent']:.1f}% efficiency due to SR860 rate changes")
            logging.info(f"   • SR860 is not maintaining configured rate consistently")
            logging.info(f"   • Consider: lower time constant, different settings, or thermal issues")
        else:
            logging.info("\n❌ TRUE PACKET LOSS DETECTED")
            logging.info(f"   • {summary['sequence_errors']} packets lost")
            logging.info(f"   • {summary['efficiency_percent']:.1f}% efficiency")
            logging.info(f"   • Network or system issues causing packet drops")
            
            # Diagnose packet loss issues
            if not auto_throttle:
                logging.info("   • Try enabling auto-throttling with --throttle")
            if summary['sample_loss_percent'] > 10:
                logging.info("   • Significant packet loss - reduce rate further")
            if summary['packet_jitter_ms'] > 10:
                logging.info("   • High packet jitter indicates network congestion")
            if summary['receive_errors'] > 0:
                logging.info("   • UDP receive errors occurred")
                logging.info("   • Check network connection and system load")
                
        # Throttling recommendations
        if auto_throttle and summary['sample_loss_percent'] < 0.1:
            logging.info("\n💡 OPTIMIZATION SUGGESTION:")
            logging.info("   Perfect performance achieved with throttling!")
            logging.info("   You could try reducing throttling for higher rates.")
        elif not auto_throttle and summary['sample_loss_percent'] > 5:
            logging.info("\n💡 OPTIMIZATION SUGGESTION:")
            logging.info("   High packet loss detected in maximum rate mode.")
            logging.info("   Consider using auto-throttling to prevent loss.")


###############################################################################
# Main Application
###############################################################################


def main():
    """Main application for maximum rate SR860 streaming with packet loss prevention."""
    
    parser = argparse.ArgumentParser(description="SR860 High-Performance Streaming with Packet Loss Prevention")
    parser.add_argument("--ip", default="192.168.1.156", help="SR860 IP address")
    parser.add_argument("--duration", type=float, default=10.0, help="Duration in seconds (0=forever)")
    parser.add_argument("--channel", choices=['X', 'XY', 'RT', 'XYRT'], default='XYRT',
                        help="Streaming channels")
    parser.add_argument("--format", choices=['float32', 'int16'], default='float32',
                        help="Data format")
    parser.add_argument("--packet", type=int, choices=[1024, 512, 256, 128], default=1024,
                        help="Packet size in bytes")
    parser.add_argument("--port", type=int, default=1865, help="UDP port")
    parser.add_argument("--throttle", action="store_true", 
                        help="Enable auto-throttling to prevent packet loss (recommended)")
    parser.add_argument("--max-rate", action="store_true",
                        help="Use maximum rate without throttling (may cause packet loss)")
    args = parser.parse_args()
    
    # Determine throttling mode
    if args.max_rate:
        auto_throttle = False
    else:
        auto_throttle = True  # Default to throttling for best performance
    
    # Create configuration
    config = StreamConfig(
        channel=StreamChannel[args.channel],
        format=StreamFormat.FLOAT32 if args.format == 'float32' else StreamFormat.INT16,
        packet_size={1024: PacketSize.SIZE_1024, 512: PacketSize.SIZE_512,
                     256: PacketSize.SIZE_256, 128: PacketSize.SIZE_128}[args.packet],
        port=args.port,
        use_little_endian=True,  # Most systems are little-endian
        use_integrity_check=True
    )
    
    logging.info(f"Starting SR860 high-performance streaming")
    logging.info(f"Mode: {'Auto-throttled' if auto_throttle else 'Maximum rate'}")
    logging.info(f"Configuration: {config}")
    
    # Run streaming
    try:
        streaming_worker(args.ip, config, args.duration, auto_throttle)
    except KeyboardInterrupt:
        logging.info("Streaming stopped by user")
        

if __name__ == "__main__":
    main() 