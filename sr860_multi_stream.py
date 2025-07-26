#!/usr/bin/env python3
"""Multi-device SR860 streaming controller.

This is the main controller that manages streaming from multiple SR860 devices
based on a JSON configuration file. It spawns separate processes for each device,
aggregates statistics, and coordinates start/stop operations.
"""

import argparse
import json
import logging
import multiprocessing as mp
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Import our modules
from sr860_multi_config import MultiStreamConfig, SR860DeviceConfig
from sr860_system_optimizer import SystemOptimizer
from sr860_configured_stream import (
    timestamped_receiver_process_entry,
    TimestampedBinaryFileWriter,
    SR860ConfiguredController
)
from sr860_class import StreamConfig, StreamChannel, StreamFormat, PacketSize

# Configure logging for the main process
logging.basicConfig(
    format="%(levelname)-8s [%(processName)-12s:%(threadName)-10s] %(asctime)s | %(message)s",
    datefmt="%H:%M:%S.%f",
    level=logging.INFO,
)


@dataclass
class DeviceProcess:
    """Container for device process information."""
    device_config: SR860DeviceConfig
    process: mp.Process
    data_queue: mp.Queue
    stats_queue: mp.Queue
    stop_event: mp.Event
    writer_thread: Optional[threading.Thread] = None
    controller: Optional[SR860ConfiguredController] = None
    stream_config: Optional[StreamConfig] = None
    actual_rate_hz: Optional[float] = None


class StatsAggregator(threading.Thread):
    """Aggregates statistics from all device processes."""
    
    def __init__(self, device_processes: List[DeviceProcess], 
                 update_interval: float = 1.0,
                 enable_dashboard: bool = True):
        super().__init__(name="StatsAggregator", daemon=True)
        self.device_processes = device_processes
        self.update_interval = update_interval
        self.enable_dashboard = enable_dashboard
        self.stop_event = threading.Event()
        self.total_stats = {}
        
    def run(self):
        """Main aggregation loop."""
        last_update = time.time()
        
        while not self.stop_event.is_set():
            current_time = time.time()
            
            # Collect stats from all devices
            for dev_proc in self.device_processes:
                device_name = dev_proc.device_config.name
                
                # Initialize device stats if needed
                if device_name not in self.total_stats:
                    self.total_stats[device_name] = {
                        'packets_received': 0,
                        'samples_received': 0,
                        'bytes_received': 0,
                        'sequence_errors': 0,
                        'receive_errors': 0,
                        'missing_samples': 0,
                        'gaps_detected': 0,
                        'start_time': current_time
                    }
                
                # Get latest stats
                try:
                    while True:
                        stats_update = dev_proc.stats_queue.get_nowait()
                        # Update device stats
                        for key in ['packets_received', 'samples_received', 
                                   'bytes_received', 'sequence_errors',
                                   'receive_errors', 'missing_samples',
                                   'gaps_detected']:
                            if key in stats_update:
                                self.total_stats[device_name][key] = stats_update[key]
                except queue.Empty:
                    pass
            
            # Display dashboard if enabled
            if current_time - last_update >= self.update_interval:
                if self.enable_dashboard:
                    self._display_dashboard()
                last_update = current_time
            
            time.sleep(0.1)
    
    def _display_dashboard(self):
        """Display real-time statistics dashboard."""
        # Clear screen (Unix/Linux)
        if sys.platform != 'win32':
            print('\033[2J\033[H', end='')
        
        print("=" * 80)
        print("SR860 MULTI-DEVICE STREAMING DASHBOARD")
        print("=" * 80)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Header
        print(f"{'Device':<15} {'Rate (Hz)':<12} {'Samples':<15} {'Mbps':<8} "
              f"{'Efficiency':<10} {'Errors':<8} {'Status':<10}")
        print("-" * 80)
        
        # Device stats
        total_rate = 0
        total_mbps = 0
        
        for dev_proc in self.device_processes:
            device_name = dev_proc.device_config.name
            stats = self.total_stats.get(device_name, {})
            
            # Calculate rates
            elapsed = time.time() - stats.get('start_time', time.time())
            if elapsed > 0:
                sample_rate = stats.get('samples_received', 0) / elapsed
                mbps = stats.get('bytes_received', 0) * 8 / (elapsed * 1e6)
            else:
                sample_rate = mbps = 0
            
            # Efficiency
            if dev_proc.actual_rate_hz and dev_proc.actual_rate_hz > 0:
                efficiency = (sample_rate / dev_proc.actual_rate_hz) * 100
            else:
                efficiency = 0
            
            # Status
            errors = stats.get('sequence_errors', 0) + stats.get('receive_errors', 0)
            if errors > 0:
                status = "ERRORS"
            elif efficiency < 80:
                status = "LOW RATE"
            else:
                status = "OK"
            
            print(f"{device_name:<15} {sample_rate:<12,.0f} "
                  f"{stats.get('samples_received', 0):<15,} {mbps:<8.1f} "
                  f"{efficiency:<10.1f}% {errors:<8} {status:<10}")
            
            total_rate += sample_rate
            total_mbps += mbps
        
        # Totals
        print("-" * 80)
        print(f"{'TOTAL':<15} {total_rate:<12,.0f} {'':15} {total_mbps:<8.1f}")
        print()
    
    def stop(self):
        """Stop the aggregator."""
        self.stop_event.set()
    
    def get_final_stats(self) -> Dict[str, Any]:
        """Get final aggregated statistics."""
        return self.total_stats.copy()


class CentralLogger(threading.Thread):
    """Optional central logger for merged data files."""
    
    def __init__(self, output_file: str, device_processes: List[DeviceProcess]):
        super().__init__(name="CentralLogger", daemon=True)
        self.output_file = output_file
        self.device_processes = device_processes
        self.stop_event = threading.Event()
        self.file = None
        
    def run(self):
        """Main logging loop."""
        # This is a placeholder for central logging functionality
        # In practice, you might want to:
        # 1. Merge data streams with synchronized timestamps
        # 2. Write to a single file with device identifiers
        # 3. Handle different sampling rates
        logging.info(f"Central logger started, output: {self.output_file}")
        
        while not self.stop_event.is_set():
            time.sleep(1)
    
    def stop(self):
        """Stop the logger."""
        self.stop_event.set()
        if self.file:
            self.file.close()


def set_process_affinity(pid: int, cpu_list: List[int]):
    """Set CPU affinity for a process."""
    try:
        # Convert CPU list to taskset format
        cpu_str = ','.join(map(str, cpu_list))
        subprocess.run(['taskset', '-cp', cpu_str, str(pid)], 
                      capture_output=True, check=True)
    except Exception as e:
        logging.warning(f"Failed to set CPU affinity: {e}")


def set_process_priority(pid: int, priority: int, scheduler: str = "SCHED_FIFO"):
    """Set real-time priority for a process."""
    try:
        if scheduler == "SCHED_FIFO":
            policy = "-f"
        elif scheduler == "SCHED_RR":
            policy = "-r"
        else:
            return  # Normal scheduler, no action needed
        
        subprocess.run(['chrt', policy, str(priority), '-p', str(pid)],
                      capture_output=True, check=True)
    except Exception as e:
        logging.warning(f"Failed to set RT priority: {e}")


def create_stream_config(device_config: SR860DeviceConfig) -> StreamConfig:
    """Create StreamConfig from device configuration."""
    return StreamConfig(
        channel=StreamChannel[device_config.channel],
        format=StreamFormat.FLOAT32 if device_config.format == "float32" else StreamFormat.INT16,
        packet_size={
            128: PacketSize.SIZE_128,
            256: PacketSize.SIZE_256,
            512: PacketSize.SIZE_512,
            1024: PacketSize.SIZE_1024
        }[device_config.packet_size],
        port=device_config.port,
        use_little_endian=device_config.use_little_endian,
        use_integrity_check=device_config.use_integrity_check
    )


def device_streaming_worker(device_config: SR860DeviceConfig,
                          stream_config: StreamConfig,
                          data_queue: mp.Queue,
                          stats_queue: mp.Queue,
                          stop_event: mp.Event,
                          duration: float):
    """Worker function for a single SR860 device (runs in separate process)."""
    # Set up process-specific logging
    process_name = f"SR860_{device_config.name}"
    mp.current_process().name = process_name
    
    # Ignore SIGINT in worker processes
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    logging.info(f"Device worker started for {device_config.name} at {device_config.ip}")
    
    # Apply CPU affinity if configured
    if device_config.receiver_affinity:
        set_process_affinity(os.getpid(), device_config.receiver_affinity.cpu_list)
        set_process_priority(os.getpid(), 
                           device_config.receiver_affinity.priority,
                           device_config.receiver_affinity.scheduler)
    
    controller = None
    receiver_process = None
    writer_thread = None
    
    try:
        # Create controller and connect
        controller = SR860ConfiguredController(device_config.ip)
        if not controller.connect():
            raise RuntimeError(f"Failed to connect to {device_config.name} at {device_config.ip}")
        
        # Set time constant if specified
        if device_config.time_constant is not None:
            logging.info(f"Setting time constant to index {device_config.time_constant}")
            controller.set_time_constant(device_config.time_constant)
        
        # Apply configuration
        config_info = controller.apply_configuration(stream_config, device_config.rate_divider)
        actual_rate = config_info['actual_rate_hz']
        
        # Create output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        filename = data_dir / f"sr860_multi_{device_config.name}_{timestamp}.bin"
        
        # Start receiver subprocess
        stream_start_time = time.time()
        
        # Create sub-process context
        mp_context = mp.get_context('spawn')
        sub_data_queue = mp_context.Queue(maxsize=1000)
        sub_stats_queue = mp_context.Queue(maxsize=100)
        sub_stop_event = mp_context.Event()
        
        receiver_process = mp_context.Process(
            target=timestamped_receiver_process_entry,
            args=(stream_config, sub_data_queue, sub_stats_queue, 
                  sub_stop_event, stream_start_time, actual_rate),
            name=f"{device_config.name}_Receiver"
        )
        receiver_process.start()
        
        # Apply receiver CPU affinity
        if device_config.receiver_affinity:
            set_process_affinity(receiver_process.pid, 
                               device_config.receiver_affinity.cpu_list)
            set_process_priority(receiver_process.pid,
                               device_config.receiver_affinity.priority,
                               device_config.receiver_affinity.scheduler)
        
        # Start writer thread
        writer_thread = TimestampedBinaryFileWriter(
            filename, stream_config, sub_data_queue, sub_stop_event,
            actual_rate_hz=actual_rate,
            rate_divider=config_info['rate_divider'],
            max_rate_hz=config_info['max_rate_hz'],
            stream_start_time=stream_start_time
        )
        writer_thread.start()
        
        # Apply writer thread affinity
        if device_config.writer_affinity:
            # Note: Thread affinity is inherited from process
            pass
        
        # Give receiver time to initialize
        time.sleep(0.5)
        
        # Start streaming
        controller.start_streaming()
        logging.info(f"{device_config.name}: Streaming started at {actual_rate:,.0f} Hz")
        
        # Monitor and forward stats
        start_time = time.time()
        
        while not stop_event.is_set():
            # Check duration
            if duration > 0 and time.time() - start_time >= duration:
                break
            
            # Forward stats from sub-process to main stats queue
            try:
                while True:
                    stats = sub_stats_queue.get_nowait()
                    stats['device_name'] = device_config.name
                    stats['actual_rate_hz'] = actual_rate
                    stats_queue.put_nowait(stats)
            except queue.Empty:
                pass
            
            time.sleep(0.1)
        
        # Stop streaming
        sub_stop_event.set()
        controller.stop_streaming()
        
    except Exception as e:
        logging.error(f"{device_config.name}: Error in device worker: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if receiver_process and receiver_process.is_alive():
            receiver_process.join(timeout=2)
            if receiver_process.is_alive():
                receiver_process.terminate()
        
        if writer_thread and writer_thread.is_alive():
            writer_thread.join(timeout=2)
        
        if controller:
            controller.close()
        
        logging.info(f"{device_config.name}: Device worker finished")


class MultiDeviceController:
    """Main controller for multi-device streaming."""
    
    def __init__(self, config: MultiStreamConfig):
        self.config = config
        self.device_processes: List[DeviceProcess] = []
        self.stats_aggregator: Optional[StatsAggregator] = None
        self.central_logger: Optional[CentralLogger] = None
        self.stop_event = mp.Event()
    
    def start(self):
        """Start streaming from all devices."""
        logging.info("=" * 80)
        logging.info("SR860 MULTI-DEVICE STREAMING CONTROLLER")
        logging.info("=" * 80)
        logging.info(f"Configuration: {self.config.description}")
        logging.info(f"Devices: {len(self.config.devices)}")
        logging.info(f"Duration: {self.config.duration}s")
        
        # Apply system optimizations if we're root
        if os.geteuid() == 0:
            self._apply_system_optimizations()
        else:
            logging.warning("Not running as root - system optimizations skipped")
        
        # Create output directory
        Path(self.config.output_directory).mkdir(exist_ok=True)
        
        # Start device processes
        mp_context = mp.get_context('spawn')
        
        for device_config in self.config.devices:
            logging.info(f"Starting process for {device_config.name}...")
            
            # Create communication objects
            data_queue = mp_context.Queue(maxsize=1000)
            stats_queue = mp_context.Queue(maxsize=100)
            stop_event = mp_context.Event()
            
            # Create stream configuration
            stream_config = create_stream_config(device_config)
            
            # Create and start process
            process = mp_context.Process(
                target=device_streaming_worker,
                args=(device_config, stream_config, data_queue, 
                      stats_queue, stop_event, self.config.duration),
                name=f"Device_{device_config.name}"
            )
            process.start()
            
            # Store process info
            dev_proc = DeviceProcess(
                device_config=device_config,
                process=process,
                data_queue=data_queue,
                stats_queue=stats_queue,
                stop_event=stop_event,
                stream_config=stream_config
            )
            self.device_processes.append(dev_proc)
        
        # Start stats aggregator
        self.stats_aggregator = StatsAggregator(
            self.device_processes,
            self.config.system.stats_update_interval,
            self.config.system.enable_dashboard
        )
        self.stats_aggregator.start()
        
        # Start central logger if enabled
        if self.config.system.enable_central_logger:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = Path(self.config.output_directory) / f"sr860_central_{timestamp}.log"
            self.central_logger = CentralLogger(str(log_file), self.device_processes)
            self.central_logger.start()
        
        logging.info("All devices started. Press Ctrl+C to stop.")
    
    def _apply_system_optimizations(self):
        """Apply system-level optimizations."""
        optimizer = SystemOptimizer()
        
        if self.config.system.network_optimization:
            net_opt = self.config.system.network_optimization
            
            # Apply network tunables
            optimizer.set_network_tunables()
            
            # Configure RFS if enabled
            if net_opt.rfs_enabled:
                optimizer.configure_rfs(net_opt.rfs_entries)
            
            # Disable irqbalance
            optimizer.disable_irqbalance()
            
            # Set CPU performance governor
            if self.config.system.isolated_cpus:
                optimizer.set_cpu_performance_governor(self.config.system.isolated_cpus)
    
    def monitor(self):
        """Monitor streaming until completion or interrupt."""
        start_time = time.time()
        
        try:
            while True:
                # Check duration
                if self.config.duration > 0:
                    if time.time() - start_time >= self.config.duration:
                        logging.info("Duration reached, stopping...")
                        break
                
                # Check if all processes are alive
                all_alive = all(p.process.is_alive() for p in self.device_processes)
                if not all_alive:
                    logging.warning("One or more device processes died")
                    break
                
                time.sleep(0.5)
                
        except KeyboardInterrupt:
            logging.info("\nReceived interrupt, shutting down...")
    
    def stop(self):
        """Stop all streaming."""
        # Signal all processes to stop
        for dev_proc in self.device_processes:
            dev_proc.stop_event.set()
        
        # Stop aggregator and logger
        if self.stats_aggregator:
            self.stats_aggregator.stop()
            self.stats_aggregator.join(timeout=2)
        
        if self.central_logger:
            self.central_logger.stop()
            self.central_logger.join(timeout=2)
        
        # Wait for all processes
        for dev_proc in self.device_processes:
            logging.info(f"Waiting for {dev_proc.device_config.name}...")
            dev_proc.process.join(timeout=5)
            if dev_proc.process.is_alive():
                logging.warning(f"Force terminating {dev_proc.device_config.name}")
                dev_proc.process.terminate()
                dev_proc.process.join(timeout=2)
        
        # Get final stats
        if self.stats_aggregator:
            final_stats = self.stats_aggregator.get_final_stats()
            self._print_final_report(final_stats)
    
    def _print_final_report(self, stats: Dict[str, Any]):
        """Print final statistics report."""
        print("\n" + "=" * 80)
        print("FINAL STATISTICS REPORT")
        print("=" * 80)
        
        total_samples = 0
        total_errors = 0
        
        for device_name, device_stats in stats.items():
            print(f"\n{device_name}:")
            print(f"  Samples received: {device_stats.get('samples_received', 0):,}")
            print(f"  Packets received: {device_stats.get('packets_received', 0):,}")
            print(f"  Sequence errors:  {device_stats.get('sequence_errors', 0):,}")
            print(f"  Receive errors:   {device_stats.get('receive_errors', 0):,}")
            print(f"  Missing samples:  {device_stats.get('missing_samples', 0):,}")
            print(f"  Gaps detected:    {device_stats.get('gaps_detected', 0):,}")
            
            total_samples += device_stats.get('samples_received', 0)
            total_errors += (device_stats.get('sequence_errors', 0) + 
                           device_stats.get('receive_errors', 0))
        
        print(f"\nTOTAL:")
        print(f"  Devices:        {len(stats)}")
        print(f"  Total samples:  {total_samples:,}")
        print(f"  Total errors:   {total_errors:,}")
        
        if total_errors == 0:
            print("\n✅ All devices streamed successfully with no errors!")
        else:
            print(f"\n⚠️  Streaming completed with {total_errors} total errors")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="SR860 Multi-Device Streaming Controller"
    )
    
    parser.add_argument('config', help='JSON configuration file')
    parser.add_argument('--validate-only', action='store_true',
                       help='Validate configuration without streaming')
    parser.add_argument('--optimize-system', action='store_true',
                       help='Apply system optimizations before streaming')
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = MultiStreamConfig.load(args.config)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Validate configuration
    from sr860_multi_config import ConfigurationBuilder
    builder = ConfigurationBuilder()
    issues = builder.validate_config(config)
    
    if issues:
        print("Configuration validation results:")
        for issue in issues:
            print(f"  {issue}")
        
        if any("ERROR" in issue for issue in issues):
            print("\nConfiguration has errors. Exiting.")
            sys.exit(1)
    
    if args.validate_only:
        print("Configuration is valid.")
        sys.exit(0)
    
    # Apply system optimizations if requested
    if args.optimize_system:
        if os.geteuid() != 0:
            print("System optimization requires root privileges")
            sys.exit(1)
        
        optimizer = SystemOptimizer()
        print("Applying system optimizations...")
        optimizer.set_network_tunables()
        optimizer.disable_irqbalance()
        optimizer.set_cpu_performance_governor()
        print("System optimizations applied")
    
    # Create and run controller
    controller = MultiDeviceController(config)
    
    try:
        controller.start()
        controller.monitor()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        controller.stop()


if __name__ == '__main__':
    main() 