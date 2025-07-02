#!/usr/bin/env python3
"""SR860 Class - Complete SR860 streaming functionality in a single class.

This class contains all essential functionality from sr860_max_stream.py and 
sr860_optimal_stream.py, providing a clean interface for SR860 streaming.
"""

import logging
import multiprocessing as mp
import numpy as np
import queue
import signal
import socket
import struct
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyvisa


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
# Enhanced Loss Analysis
###############################################################################


@dataclass
class LossEvent:
    """Detailed information about a sample loss event."""
    timestamp: float
    expected_sequence: int
    actual_sequence: int
    samples_lost: int
    packet_interval_before: float
    packet_interval_after: float
    packets_since_last_loss: int
    total_packets_received: int
    time_since_start: float
    time_since_last_loss: float


class EnhancedStreamingStats:
    """Enhanced streaming statistics with detailed loss analysis."""
    
    def __init__(self, start_time: float):
        self.start_time = start_time
        self.packets_received = 0
        self.bytes_received = 0
        self.samples_received = 0
        self.errors = 0
        self.last_packet_time = 0
        self.packet_intervals = deque(maxlen=1000)
        self.sequence_errors = 0
        self.last_sequence = -1
        
        # Enhanced loss tracking
        self.loss_events: List[LossEvent] = []
        self.packet_times = deque(maxlen=100)  # Store recent packet timestamps
        self.sequence_numbers = deque(maxlen=100)  # Store recent sequences
        self.last_loss_time = 0
        self.packets_since_last_loss = 0
        
        # Pattern analysis
        self.loss_intervals = []  # Time between losses
        self.loss_burst_threshold = 1.0  # Seconds - losses within this are considered a burst
        self.current_burst_start = None
        self.burst_events = []  # List of (start_time, end_time, loss_count)
        
    def update_packet(self, packet_size: int, samples: int, sequence: Optional[int] = None):
        """Update statistics with new packet and detect losses."""
        current_time = time.time()
        
        self.packets_received += 1
        self.bytes_received += packet_size
        self.samples_received += samples
        self.packets_since_last_loss += 1
        
        # Store packet timing
        self.packet_times.append(current_time)
        if sequence is not None:
            self.sequence_numbers.append(sequence)
        
        # Track packet timing
        packet_interval = 0
        if self.last_packet_time > 0:
            packet_interval = current_time - self.last_packet_time
            self.packet_intervals.append(packet_interval)
            
        self.last_packet_time = current_time
        
        # Enhanced sequence checking with detailed loss tracking
        if sequence is not None and self.last_sequence >= 0:
            expected = (self.last_sequence + 1) % 256  # SR860 uses 8-bit sequence counter
            
            if sequence != expected:
                # Calculate samples lost
                if sequence > expected:
                    seq_gap = sequence - expected
                else:
                    seq_gap = (256 + sequence) - expected  # Handle wraparound
                
                samples_lost = seq_gap * samples  # Assuming same samples per packet
                
                # Get interval before loss (if available)
                interval_before = self.packet_intervals[-2] if len(self.packet_intervals) >= 2 else 0
                
                # Create detailed loss event
                time_since_start = current_time - self.start_time
                time_since_last_loss = current_time - self.last_loss_time if self.last_loss_time > 0 else 0
                
                loss_event = LossEvent(
                    timestamp=current_time,
                    expected_sequence=expected,
                    actual_sequence=sequence,
                    samples_lost=samples_lost,
                    packet_interval_before=interval_before,
                    packet_interval_after=0,  # Will be filled in next packet
                    packets_since_last_loss=self.packets_since_last_loss,
                    total_packets_received=self.packets_received,
                    time_since_start=time_since_start,
                    time_since_last_loss=time_since_last_loss
                )
                
                self.loss_events.append(loss_event)
                self.sequence_errors += 1
                
                # Update interval after loss for previous event
                if len(self.loss_events) >= 2:
                    self.loss_events[-2].packet_interval_after = interval_before
                
                # Track loss timing patterns
                if self.last_loss_time > 0:
                    loss_interval = current_time - self.last_loss_time
                    self.loss_intervals.append(loss_interval)
                    
                    # Detect burst events
                    if loss_interval <= self.loss_burst_threshold:
                        if self.current_burst_start is None:
                            self.current_burst_start = self.last_loss_time
                    else:
                        # End of burst
                        if self.current_burst_start is not None:
                            burst_duration = self.last_loss_time - self.current_burst_start
                            burst_losses = sum(1 for event in self.loss_events 
                                             if self.current_burst_start <= event.timestamp <= self.last_loss_time)
                            self.burst_events.append((self.current_burst_start, self.last_loss_time, burst_losses))
                            self.current_burst_start = None
                
                self.last_loss_time = current_time
                self.packets_since_last_loss = 0
                
                logging.warning(f"Sample loss detected: expected seq {expected}, got {sequence}, "
                               f"lost ~{samples_lost} samples, interval: {interval_before*1000:.2f}ms")
        
        if sequence is not None:
            self.last_sequence = sequence
            
    def analyze_loss_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in sample losses."""
        if not self.loss_events:
            return {"message": "No sample losses detected"}
        
        analysis = {}
        
        # Basic loss statistics
        total_losses = len(self.loss_events)
        total_samples_lost = sum(event.samples_lost for event in self.loss_events)
        loss_rate = total_losses / (time.time() - self.start_time) if total_losses > 0 else 0
        
        analysis["basic_stats"] = {
            "total_loss_events": total_losses,
            "total_samples_lost": total_samples_lost,
            "loss_events_per_second": loss_rate,
            "average_samples_lost_per_event": total_samples_lost / total_losses if total_losses > 0 else 0
        }
        
        # Timing pattern analysis
        if len(self.loss_intervals) > 1:
            intervals = np.array(self.loss_intervals)
            analysis["timing_patterns"] = {
                "mean_interval_between_losses": np.mean(intervals),
                "std_interval_between_losses": np.std(intervals),
                "min_interval": np.min(intervals),
                "max_interval": np.max(intervals),
                "intervals": self.loss_intervals
            }
            
            # Check for periodic patterns
            if len(intervals) >= 3:
                # Look for regular intervals (coefficient of variation < 0.2 indicates regularity)
                cv = np.std(intervals) / np.mean(intervals) if np.mean(intervals) > 0 else float('inf')
                analysis["timing_patterns"]["regularity_coefficient"] = cv
                analysis["timing_patterns"]["is_regular"] = cv < 0.2
                
                # Frequency analysis if regular
                if cv < 0.3:
                    mean_interval = np.mean(intervals)
                    frequency = 1.0 / mean_interval if mean_interval > 0 else 0
                    analysis["timing_patterns"]["loss_frequency_hz"] = frequency
        
        # Packet interval analysis around losses
        intervals_before = [event.packet_interval_before for event in self.loss_events if event.packet_interval_before > 0]
        intervals_after = [event.packet_interval_after for event in self.loss_events if event.packet_interval_after > 0]
        
        if intervals_before:
            analysis["packet_intervals_before_loss"] = {
                "mean": np.mean(intervals_before),
                "std": np.std(intervals_before),
                "max": np.max(intervals_before),
                "above_normal_count": sum(1 for i in intervals_before if i > 0.1)  # > 100ms considered abnormal
            }
        
        if intervals_after:
            analysis["packet_intervals_after_loss"] = {
                "mean": np.mean(intervals_after),
                "std": np.std(intervals_after),
                "max": np.max(intervals_after),
            }
        
        # Burst analysis
        if self.burst_events:
            analysis["burst_analysis"] = {
                "total_bursts": len(self.burst_events),
                "burst_events": [
                    {
                        "start_time": start - self.start_time,
                        "duration": end - start,
                        "loss_count": count
                    }
                    for start, end, count in self.burst_events
                ]
            }
        
        # Time-based distribution
        loss_times = [event.time_since_start for event in self.loss_events]
        if loss_times:
            total_duration = time.time() - self.start_time
            # Divide into 10 time bins and count losses per bin
            bins = np.linspace(0, total_duration, 11)
            hist, _ = np.histogram(loss_times, bins)
            analysis["time_distribution"] = {
                "bins": bins[:-1].tolist(),
                "counts": hist.tolist(),
                "early_heavy": hist[0] > np.mean(hist) * 2,  # Lots of losses early on
                "late_heavy": hist[-1] > np.mean(hist) * 2   # Lots of losses later
            }
        
        # Diagnostic suggestions
        suggestions = []
        
        if analysis.get("timing_patterns", {}).get("is_regular", False):
            freq = analysis["timing_patterns"]["loss_frequency_hz"]
            suggestions.append(f"Regular loss pattern detected at {freq:.2f} Hz - suggests periodic interference")
        
        if analysis.get("packet_intervals_before_loss", {}).get("above_normal_count", 0) > total_losses * 0.5:
            suggestions.append("Many losses preceded by long packet intervals - suggests network congestion")
        
        if analysis.get("burst_analysis", {}).get("total_bursts", 0) > 0:
            bursts = analysis["burst_analysis"]["total_bursts"]
            suggestions.append(f"{bursts} loss bursts detected - suggests intermittent interference")
        
        if analysis.get("time_distribution", {}).get("early_heavy", False):
            suggestions.append("High loss rate at start - suggests initialization issues")
        
        if analysis.get("time_distribution", {}).get("late_heavy", False):
            suggestions.append("Increasing loss rate over time - suggests thermal or memory issues")
        
        analysis["diagnostic_suggestions"] = suggestions
        
        return analysis
    
    def get_detailed_loss_report(self) -> str:
        """Generate a detailed report of all loss events."""
        if not self.loss_events:
            return "No sample losses detected.\n"
        
        report = []
        report.append("DETAILED SAMPLE LOSS ANALYSIS")
        report.append("=" * 50)
        
        # Get pattern analysis
        patterns = self.analyze_loss_patterns()
        
        # Basic statistics
        basic = patterns.get("basic_stats", {})
        report.append(f"\nBASIC LOSS STATISTICS:")
        report.append(f"  Total loss events: {basic.get('total_loss_events', 0)}")
        report.append(f"  Total samples lost: {basic.get('total_samples_lost', 0)}")
        report.append(f"  Loss rate: {basic.get('loss_events_per_second', 0):.3f} events/sec")
        report.append(f"  Avg samples/event: {basic.get('average_samples_lost_per_event', 0):.1f}")
        
        # Pattern analysis
        if "timing_patterns" in patterns:
            timing = patterns["timing_patterns"]
            report.append(f"\nTIMING PATTERN ANALYSIS:")
            report.append(f"  Mean interval between losses: {timing.get('mean_interval_between_losses', 0):.3f} sec")
            report.append(f"  Std deviation: {timing.get('std_interval_between_losses', 0):.3f} sec")
            report.append(f"  Regular pattern: {'Yes' if timing.get('is_regular', False) else 'No'}")
            if timing.get('is_regular', False):
                freq = timing.get('loss_frequency_hz', 0)
                report.append(f"  Loss frequency: {freq:.2f} Hz (period: {1/freq:.3f} sec)")
        
        # Packet interval analysis
        if "packet_intervals_before_loss" in patterns:
            before = patterns["packet_intervals_before_loss"]
            report.append(f"\nPACKET INTERVALS BEFORE LOSSES:")
            report.append(f"  Mean: {before.get('mean', 0)*1000:.2f} ms")
            report.append(f"  Max: {before.get('max', 0)*1000:.2f} ms")
            report.append(f"  Abnormally long intervals: {before.get('above_normal_count', 0)}")
        
        # Diagnostic suggestions
        suggestions = patterns.get("diagnostic_suggestions", [])
        if suggestions:
            report.append(f"\nDIAGNOSTIC SUGGESTIONS:")
            for suggestion in suggestions:
                report.append(f"  • {suggestion}")
        
        # Individual loss events (up to 20 most recent)
        report.append(f"\nRECENT LOSS EVENTS (last 20):")
        report.append(f"{'Time':>8} {'Seq':>6} {'Lost':>6} {'Interval':>10} {'Since Last':>12}")
        report.append("-" * 50)
        
        recent_events = self.loss_events[-20:] if len(self.loss_events) > 20 else self.loss_events
        for event in recent_events:
            report.append(
                f"{event.time_since_start:8.2f} "
                f"{event.expected_sequence:3d}→{event.actual_sequence:3d} "
                f"{event.samples_lost:6d} "
                f"{event.packet_interval_before*1000:8.2f}ms "
                f"{event.time_since_last_loss:10.3f}s"
            )
        
        return "\n".join(report)
    
    def get_summary(self, expected_rate: float, expected_duration: float) -> Dict[str, Any]:
        """Get comprehensive statistics summary including loss analysis."""
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
            
        # Add loss analysis
        loss_patterns = self.analyze_loss_patterns()
            
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
            
            # Enhanced loss analysis
            'loss_analysis': loss_patterns,
            'detailed_loss_report': self.get_detailed_loss_report()
        }


###############################################################################
# Main SR860 Class
###############################################################################


class SR860Class:
    """Complete SR860 streaming functionality in a single class."""
    
    def __init__(self, ip: str = "192.168.1.156"):
        self.ip = ip
        self.inst = None
        self.config = None
        self.stats = None
        
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
        
        self.config = StreamConfig(
            channel=channel_map.get(channel_idx, StreamChannel.XYRT),
            format=format_map.get(format_idx, StreamFormat.FLOAT32),
            packet_size=packet_map.get(packet_idx, PacketSize.SIZE_1024),
            port=port,
            use_little_endian=use_little_endian,
            use_integrity_check=use_integrity_check
        )
        
        # Calculate data rates
        bytes_per_sample = self.config.bytes_per_point * self.config.points_per_sample
        bytes_per_second = actual_rate * bytes_per_sample
        mbps = (bytes_per_second * 8) / 1e6
        packets_per_second = actual_rate / self.config.samples_per_packet if self.config.samples_per_packet > 0 else 0
        
        config_info = {
            'config_object': self.config,
            'channel_idx': channel_idx,
            'format_idx': format_idx,
            'packet_idx': packet_idx,
            'port': port,
            'use_little_endian': use_little_endian,
            'use_integrity_check': use_integrity_check,
            'rate_divider': rate_n,
            'max_rate_hz': max_rate,
            'actual_rate_hz': actual_rate,
            'option_value': option,
            'bytes_per_sample': bytes_per_sample,
            'bytes_per_second': bytes_per_second,
            'mbps': mbps,
            'packets_per_second': packets_per_second,
            'samples_per_packet': self.config.samples_per_packet
        }
        
        logging.info(f"Current configuration:")
        logging.info(f"  Channel:     {channel_map[channel_idx].name} ({self.config.points_per_sample} values/sample)")
        logging.info(f"  Format:      {format_map[format_idx].name} ({self.config.bytes_per_point} bytes/point)")
        logging.info(f"  Packet size: {self.config.packet_bytes} bytes")
        logging.info(f"  Port:        {port}")
        logging.info(f"  Rate:        {actual_rate:,.0f} Hz (divider: 2^{rate_n} from max {max_rate:,.0f} Hz)")
        logging.info(f"  Data rate:   {mbps:.1f} Mbps")
        logging.info(f"  Packet rate: {packets_per_second:.1f} packets/s")
        
        return config_info
    
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
    
    def set_time_constant(self, time_constant_idx: int) -> Optional[float]:
        """Set the time constant and return the new maximum streaming rate."""
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
    
    def get_detailed_loss_report(self) -> str:
        """Get detailed loss report if statistics are available."""
        if self.stats:
            return self.stats.get_detailed_loss_report()
        else:
            return "No statistics available - no streaming session recorded."
    
    def get_streaming_summary(self, expected_rate: float, expected_duration: float) -> Dict[str, Any]:
        """Get comprehensive streaming summary including loss analysis."""
        if self.stats:
            return self.stats.get_summary(expected_rate, expected_duration)
        else:
            return {"message": "No statistics available - no streaming session recorded."} 