#!/usr/bin/env python3
"""SR860 Optimal Rate Streaming - Automatically determine and use the best streaming rate.

This script analyzes all constraints to determine the optimal streaming rate:
- Network bandwidth limitations
- Packet size and overhead
- Data format and channel configuration
- SR860 hardware limitations
- System processing capabilities
"""

import argparse
import logging
import numpy as np
import psutil
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyvisa

from sr860_max_stream import *


@dataclass
class SystemCapabilities:
    """System performance capabilities."""
    network_bandwidth_mbps: float
    cpu_cores: int
    cpu_freq_mhz: float
    memory_available_gb: float
    disk_write_speed_mbps: float
    python_overhead_factor: float = 0.7  # Typical Python efficiency


@dataclass
class OptimalConfiguration:
    """Optimal streaming configuration based on analysis."""
    theoretical_max_rate: float
    network_limited_rate: float
    processing_limited_rate: float
    recommended_rate: float
    rate_divider: int
    efficiency_target: float
    bottleneck: str
    recommendations: List[str]


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


# Use the enhanced statistics class in place of the original
StreamingStats = EnhancedStreamingStats


class OptimalStreamController(SR860StreamController):
    """Controller that automatically determines optimal streaming configuration."""
    
    def __init__(self, ip: str, config: StreamConfig):
        super().__init__(ip, config)
        self.system_caps = None
        self.optimal_config = None
        
    def analyze_system_capabilities(self) -> SystemCapabilities:
        """Analyze system performance capabilities."""
        logging.info("Analyzing system capabilities...")
        
        # Network bandwidth (estimate based on interface speed)
        net_stats = psutil.net_if_stats()
        max_speed = 0
        for interface, stats in net_stats.items():
            if stats.isup and stats.speed > 0:
                max_speed = max(max_speed, stats.speed)
        
        # Assume 70% efficiency for UDP streaming
        network_bandwidth = max_speed * 0.7 if max_speed > 0 else 100.0
        
        # CPU info
        cpu_count = psutil.cpu_count()
        cpu_freq = psutil.cpu_freq().current if psutil.cpu_freq() else 2000.0
        
        # Memory
        memory = psutil.virtual_memory()
        memory_available = memory.available / (1024**3)
        
        # Disk (estimate - actual measurement would require benchmark)
        disk_write_speed = 100.0  # Conservative estimate in MB/s
        
        caps = SystemCapabilities(
            network_bandwidth_mbps=network_bandwidth,
            cpu_cores=cpu_count,
            cpu_freq_mhz=cpu_freq,
            memory_available_gb=memory_available,
            disk_write_speed_mbps=disk_write_speed
        )
        
        logging.info(f"System capabilities: {caps}")
        return caps
        
    def calculate_optimal_rate(self) -> OptimalConfiguration:
        """Calculate optimal streaming rate based on all constraints."""
        if not self.inst:
            raise RuntimeError("Not connected")
            
        # Get SR860 maximum rate
        sr860_max_rate = float(self.inst.query("STREAMRATEMAX?").strip())
        logging.info(f"SR860 maximum rate: {sr860_max_rate} Hz")
        
        # Get system capabilities
        self.system_caps = self.analyze_system_capabilities()
        
        # Calculate data rates
        bytes_per_sample = self.config.bytes_per_point * self.config.points_per_sample
        overhead_factor = 1.1  # 10% packet overhead
        
        # 1. Network bandwidth limit
        network_limited_rate = (self.system_caps.network_bandwidth_mbps * 1e6 / 8) / (bytes_per_sample * overhead_factor)
        
        # 2. Packet processing limit (empirical formula based on testing)
        packets_per_second_limit = 50000 * self.system_caps.python_overhead_factor
        samples_per_packet = self.config.samples_per_packet
        packet_limited_rate = packets_per_second_limit * samples_per_packet
        
        # 3. Disk write limit
        disk_limited_rate = (self.system_caps.disk_write_speed_mbps * 1e6) / bytes_per_sample
        
        # 4. CPU processing limit (rough estimate)
        cpu_limited_rate = 1e6 * self.system_caps.cpu_cores * self.system_caps.python_overhead_factor
        
        # Find the bottleneck
        bottlenecks = {
            "SR860 hardware": sr860_max_rate,
            "Network bandwidth": network_limited_rate,
            "Packet processing": packet_limited_rate,
            "Disk write speed": disk_limited_rate,
            "CPU processing": cpu_limited_rate
        }
        
        bottleneck_name = min(bottlenecks, key=bottlenecks.get)
        theoretical_max = bottlenecks[bottleneck_name]
        
        # Calculate recommended rate (80% of theoretical for stability)
        recommended_rate = theoretical_max * 0.8
        
        # Find appropriate rate divider
        rate_divider = 0
        actual_rate = sr860_max_rate
        while actual_rate > recommended_rate and rate_divider < 20:
            rate_divider += 1
            actual_rate = sr860_max_rate / (2 ** rate_divider)
            
        # Generate recommendations
        recommendations = []
        
        if bottleneck_name == "Network bandwidth":
            recommendations.append("Consider using Gigabit Ethernet or reducing channels/format")
        elif bottleneck_name == "Packet processing":
            recommendations.append("Consider using larger packet sizes or C++ receiver")
        elif bottleneck_name == "Disk write speed":
            recommendations.append("Consider using SSD or RAM disk for data storage")
        elif bottleneck_name == "SR860 hardware":
            recommendations.append("SR860 is at maximum capability")
            
        if self.config.format == StreamFormat.FLOAT32:
            recommendations.append("Consider using INT16 format to halve bandwidth")
        if self.config.points_per_sample == 4:
            recommendations.append("Consider streaming fewer channels (e.g., XY only)")
            
        optimal = OptimalConfiguration(
            theoretical_max_rate=sr860_max_rate,
            network_limited_rate=network_limited_rate,
            processing_limited_rate=min(packet_limited_rate, cpu_limited_rate),
            recommended_rate=actual_rate,
            rate_divider=rate_divider,
            efficiency_target=0.95,  # Target 95% efficiency
            bottleneck=bottleneck_name,
            recommendations=recommendations
        )
        
        self.optimal_config = optimal
        return optimal
        
    def configure_optimal_streaming(self) -> Dict[str, Any]:
        """Configure SR860 for optimal streaming rate."""
        # First calculate optimal rate
        optimal = self.calculate_optimal_rate()
        
        logging.info("\n" + "="*60)
        logging.info("OPTIMAL STREAMING CONFIGURATION")
        logging.info("="*60)
        
        logging.info(f"\nRate Analysis:")
        logging.info(f"  SR860 maximum:     {optimal.theoretical_max_rate:,.0f} Hz")
        logging.info(f"  Network limited:   {optimal.network_limited_rate:,.0f} Hz")
        logging.info(f"  Processing limited: {optimal.processing_limited_rate:,.0f} Hz")
        logging.info(f"  Recommended:       {optimal.recommended_rate:,.0f} Hz")
        logging.info(f"  Bottleneck:        {optimal.bottleneck}")
        
        logging.info(f"\nConfiguration:")
        logging.info(f"  Channels:          {self.config.channel.name} ({self.config.points_per_sample} values/sample)")
        logging.info(f"  Format:            {self.config.format.name} ({self.config.bytes_per_point} bytes/point)")
        logging.info(f"  Packet size:       {self.config.packet_bytes} bytes")
        logging.info(f"  Samples/packet:    {self.config.samples_per_packet}")
        logging.info(f"  Rate divider:      2^{optimal.rate_divider} = {2**optimal.rate_divider}")
        
        if optimal.recommendations:
            logging.info(f"\nRecommendations:")
            for rec in optimal.recommendations:
                logging.info(f"  • {rec}")
                
        # Configure streaming with optimal rate
        self.inst.write(f"STREAMCH {self.config.channel.value}")
        self.inst.write(f"STREAMFMT {self.config.format.value}")
        self.inst.write(f"STREAMPCKT {self.config.packet_size.value}")
        self.inst.write(f"STREAMPORT {self.config.port}")
        self.inst.write(f"STREAMOPTION {self.config.option_value}")
        self.inst.write(f"STREAMRATE {optimal.rate_divider}")
        
        time.sleep(0.2)
        
        # Verify configuration
        actual_rate = float(self.inst.query("STREAMRATEMAX?").strip()) / (2 ** optimal.rate_divider)
        
        config_info = {
            'optimal_config': optimal,
            'actual_rate_hz': actual_rate,
            'bytes_per_second': actual_rate * self.config.bytes_per_point * self.config.points_per_sample,
            'mbps': (actual_rate * self.config.bytes_per_point * self.config.points_per_sample * 8) / 1e6,
            'packets_per_second': actual_rate / self.config.samples_per_packet if self.config.samples_per_packet > 0 else 0
        }
        
        return config_info


def optimal_streaming_worker(ip: str, config: StreamConfig, duration: float, 
                           auto_optimize: bool = True, detailed_loss_analysis: bool = False):
    """Worker process for optimal rate streaming."""
    
    logging.info(f"Optimal streaming worker started for {ip}")
    
    # Create binary file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    filename = data_dir / f"sr860_optimal_{ip.replace('.', '_')}_{timestamp}.bin"
    
    # Initialize enhanced statistics
    stats = EnhancedStreamingStats(start_time=time.time())
    
    # Create writer
    writer = BinaryFileWriter(filename, config)
    
    # Create receiver
    receiver = StreamReceiver(config, writer, stats)
    
    # Create optimal controller
    controller = OptimalStreamController(ip, config)
    
    try:
        # Connect and configure
        if not controller.connect():
            raise RuntimeError("Failed to connect to SR860")
            
        # Configure for optimal rate
        config_info = controller.configure_optimal_streaming()
        optimal_config = config_info['optimal_config']
        expected_rate = config_info['actual_rate_hz']
        
        # Start receiver thread
        receiver_thread = threading.Thread(target=receiver.start, name="Receiver")
        receiver_thread.start()
        
        # Give receiver time to start
        time.sleep(2)
        
        # Start streaming
        controller.start_streaming()
        
        # Monitor for duration
        logging.info(f"\nStreaming at {expected_rate:,.0f} Hz for {duration} seconds...")
        
        last_log_time = time.time()
        
        while True:
            elapsed = time.time() - stats.start_time
            
            if duration > 0 and elapsed >= duration:
                break
                
            # Print statistics every 5 seconds
            current_time = time.time()
            if current_time - last_log_time >= 5.0:
                summary = stats.get_summary(expected_rate, elapsed)
                logging.info(
                    f"Progress: {stats.samples_received:,} samples, "
                    f"{summary['mbps_actual']:.1f} Mbps, "
                    f"efficiency: {summary['efficiency_percent']:.1f}%"
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
        logging.info("OPTIMAL STREAMING FINAL STATISTICS")
        logging.info("="*60)
        
        logging.info(f"\nConfiguration Summary:")
        logging.info(f"  Bottleneck:        {optimal_config.bottleneck}")
        logging.info(f"  Rate configured:   {expected_rate:,.0f} Hz")
        logging.info(f"  Target efficiency: {optimal_config.efficiency_target*100:.0f}%")
        
        logging.info("\nDuration:")
        logging.info(f"  Expected: {summary['duration_expected']:.1f} s")
        logging.info(f"  Actual:   {summary['duration_actual']:.1f} s")
        
        logging.info("\nSamples:")
        logging.info(f"  Expected: {summary['samples_expected']:,}")
        logging.info(f"  Actual:   {summary['samples_actual']:,}")
        logging.info(f"  Lost:     {summary['samples_lost']:,} ({summary['sample_loss_percent']:.1f}%)")
        
        logging.info("\nSample Rate:")
        logging.info(f"  Expected: {summary['sample_rate_expected']:,.1f} Hz")
        logging.info(f"  Actual:   {summary['sample_rate_actual']:,.1f} Hz")
        logging.info(f"  Efficiency: {summary['efficiency_percent']:.1f}%")
        
        logging.info("\nData Rate:")
        logging.info(f"  Expected: {summary['mbps_expected']:.1f} Mbps")
        logging.info(f"  Actual:   {summary['mbps_actual']:.1f} Mbps")
        
        logging.info("\nPacket Statistics:")
        logging.info(f"  Expected rate: {summary['sample_rate_expected']/config.samples_per_packet:.1f} packets/s")
        logging.info(f"  Actual rate:   {summary['packet_rate_actual']:.1f} packets/s")
        logging.info(f"  Total received: {summary['packets_actual']:,}")
        
        logging.info("\nPacket Timing:")
        logging.info(f"  Average interval: {summary['avg_packet_interval_ms']:.2f} ms")
        logging.info(f"  Jitter (std dev): {summary['std_packet_interval_ms']:.2f} ms")
        logging.info(f"  Min interval:     {summary['min_packet_interval_ms']:.2f} ms")
        logging.info(f"  Max interval:     {summary['max_packet_interval_ms']:.2f} ms")
        
        # Display detailed loss analysis if losses occurred and requested
        if summary['sequence_errors'] > 0:
            if detailed_loss_analysis:
                logging.info("\n" + "="*60)
                logging.info("SAMPLE LOSS ANALYSIS")
                logging.info("="*60)
                
                # Print the detailed loss report
                loss_report = summary['detailed_loss_report']
                for line in loss_report.split('\n'):
                    logging.info(line)
                
                # Additional pattern insights
                loss_analysis = summary['loss_analysis']
                if 'timing_patterns' in loss_analysis:
                    patterns = loss_analysis['timing_patterns']
                    if patterns.get('is_regular', False):
                        logging.info(f"\n🔍 PATTERN DETECTED:")
                        logging.info(f"   Regular loss pattern at {patterns['loss_frequency_hz']:.2f} Hz")
                        logging.info(f"   This suggests periodic interference or system load")
                        
                # Show burst information
                if 'burst_analysis' in loss_analysis:
                    bursts = loss_analysis['burst_analysis']
                    logging.info(f"\n🔍 BURST ANALYSIS:")
                    logging.info(f"   {bursts['total_bursts']} loss bursts detected")
                    for i, burst in enumerate(bursts['burst_events'][:5]):  # Show first 5 bursts
                        logging.info(f"   Burst {i+1}: {burst['loss_count']} losses over {burst['duration']:.2f}s at t={burst['start_time']:.1f}s")
            else:
                # Brief summary for normal operation
                loss_analysis = summary['loss_analysis']
                basic_stats = loss_analysis.get('basic_stats', {})
                logging.info(f"\n⚠️  SAMPLE LOSSES DETECTED:")
                logging.info(f"   {basic_stats.get('total_loss_events', 0)} loss events, "
                            f"{basic_stats.get('total_samples_lost', 0)} samples lost")
                logging.info(f"   Use --detailed-loss-analysis for full analysis")
                
                # Show key patterns briefly
                if 'timing_patterns' in loss_analysis:
                    patterns = loss_analysis['timing_patterns']
                    if patterns.get('is_regular', False):
                        freq = patterns['loss_frequency_hz']
                        logging.info(f"   🔍 Regular pattern at {freq:.2f} Hz detected")
        else:
            logging.info("\n✅ NO SAMPLE LOSSES DETECTED - PERFECT PERFORMANCE!")
        
        # Performance assessment
        if summary['efficiency_percent'] >= optimal_config.efficiency_target * 100:
            logging.info("\n✅ OPTIMAL PERFORMANCE ACHIEVED!")
            logging.info(f"   Efficiency {summary['efficiency_percent']:.1f}% exceeds target {optimal_config.efficiency_target*100:.0f}%")
        else:
            logging.info("\n⚠️  Performance below target")
            logging.info(f"   Efficiency {summary['efficiency_percent']:.1f}% < target {optimal_config.efficiency_target*100:.0f}%")
            
            # Diagnose issues
            if summary['packet_jitter_ms'] > 5:
                logging.info("   • High packet jitter suggests network congestion")
            if summary['sample_loss_percent'] > 5:
                logging.info("   • Significant sample loss - consider reducing rate")
            if controller.system_caps and controller.system_caps.network_bandwidth_mbps < summary['mbps_expected']:
                logging.info("   • Network bandwidth may be insufficient")
                
        logging.info(f"\nOutput file: {filename}")
        logging.info(f"File size: {writer.bytes_written/1e6:.1f} MB")
        
        # Final recommendations
        if optimal_config.recommendations:
            logging.info("\nOptimization suggestions:")
            for rec in optimal_config.recommendations:
                logging.info(f"  • {rec}")


def main():
    """Main application for optimal SR860 streaming."""
    
    parser = argparse.ArgumentParser(
        description="SR860 Optimal Rate Streaming - Automatically determines best streaming rate"
    )
    parser.add_argument("--ip", default="192.168.1.156", help="SR860 IP address")
    parser.add_argument("--duration", type=float, default=10.0, help="Duration in seconds")
    parser.add_argument("--channel", choices=['X', 'XY', 'RT', 'XYRT'], default='XYRT',
                        help="Streaming channels")
    parser.add_argument("--format", choices=['float32', 'int16'], default='float32',
                        help="Data format")
    parser.add_argument("--packet", type=int, choices=[1024, 512, 256, 128], default=1024,
                        help="Packet size in bytes")
    parser.add_argument("--port", type=int, default=1865, help="UDP port")
    parser.add_argument("--time-constant", type=int, help="Set time constant index (0-21)")
    parser.add_argument("--detailed-loss-analysis", action="store_true",
                        help="Enable detailed sample loss analysis and reporting")
    args = parser.parse_args()
    
    # Create configuration
    config = StreamConfig(
        channel=StreamChannel[args.channel],
        format=StreamFormat.FLOAT32 if args.format == 'float32' else StreamFormat.INT16,
        packet_size={1024: PacketSize.SIZE_1024, 512: PacketSize.SIZE_512,
                     256: PacketSize.SIZE_256, 128: PacketSize.SIZE_128}[args.packet],
        port=args.port,
        use_little_endian=True,
        use_integrity_check=True
    )
    
    logging.info("="*60)
    logging.info("SR860 OPTIMAL RATE STREAMING")
    logging.info("="*60)
    logging.info(f"Target: Stream at maximum sustainable rate")
    logging.info(f"Configuration: {args.channel} channels, {args.format} format, {args.packet} byte packets")
    
    # Set time constant if specified
    if args.time_constant is not None:
        try:
            rm = pyvisa.ResourceManager('@py')
            inst = rm.open_resource(f"TCPIP0::{args.ip}::inst0::INSTR")
            inst.write(f"OFLT {args.time_constant}")
            inst.close()
            logging.info(f"Set time constant to index {args.time_constant}")
            time.sleep(0.5)
        except Exception as e:
            logging.error(f"Failed to set time constant: {e}")
    
    # Run optimal streaming
    try:
        optimal_streaming_worker(args.ip, config, args.duration, 
                                detailed_loss_analysis=args.detailed_loss_analysis)
    except KeyboardInterrupt:
        logging.info("Streaming stopped by user")
        

if __name__ == "__main__":
    main() 