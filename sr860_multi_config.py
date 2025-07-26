#!/usr/bin/env python3
"""Configuration system for multi-device SR860 streaming.

This module provides tools to create, validate, and manage configurations
for streaming from multiple SR860 devices with advanced Linux optimizations.
"""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any
import socket
import psutil

from sr860_class import StreamChannel, StreamFormat, PacketSize


@dataclass
class CPUAffinity:
    """CPU affinity configuration for a process/thread."""
    cpu_list: List[int]  # List of CPU cores
    priority: int = 50   # Real-time priority (1-99)
    scheduler: str = "SCHED_FIFO"  # SCHED_FIFO, SCHED_RR, or SCHED_OTHER


@dataclass
class NetworkOptimization:
    """Network optimization settings."""
    # RSS (Receive Side Scaling)
    rss_queues: Optional[int] = None  # Number of RX queues
    rss_cpus: Optional[List[int]] = None  # CPUs for RSS IRQs
    
    # RFS (Receive Flow Steering)
    rfs_enabled: bool = True
    rfs_entries: int = 32768
    rfs_flow_cnt: int = 4096
    
    # XPS (Transmit Packet Steering)
    xps_enabled: bool = True
    xps_cpus: Optional[Dict[int, List[int]]] = None  # {queue: [cpus]}
    
    # Socket buffer sizes
    socket_buffer_size: int = 64 * 1024 * 1024  # 64MB
    
    # Other tunables
    netdev_max_backlog: int = 10000
    busy_poll_usecs: int = 50  # SO_BUSY_POLL microseconds


@dataclass
class SR860DeviceConfig:
    """Configuration for a single SR860 device."""
    name: str  # Friendly name
    ip: str    # IP address
    
    # Streaming configuration
    channel: str = "XYRT"  # X, XY, RT, or XYRT
    format: str = "float32"  # float32 or int16
    packet_size: int = 1024  # 128, 256, 512, or 1024
    port: int = 1865
    rate_divider: Optional[int] = None
    time_constant: Optional[int] = None
    use_little_endian: bool = True
    use_integrity_check: bool = False
    
    # CPU affinity
    receiver_affinity: Optional[CPUAffinity] = None
    writer_affinity: Optional[CPUAffinity] = None
    
    # Performance settings
    use_timestamped_receiver: bool = True
    enable_detailed_stats: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        # Convert CPUAffinity objects
        if self.receiver_affinity:
            d['receiver_affinity'] = asdict(self.receiver_affinity)
        if self.writer_affinity:
            d['writer_affinity'] = asdict(self.writer_affinity)
        return d
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SR860DeviceConfig':
        """Create from dictionary."""
        # Convert affinity dicts back to objects
        if data.get('receiver_affinity'):
            data['receiver_affinity'] = CPUAffinity(**data['receiver_affinity'])
        if data.get('writer_affinity'):
            data['writer_affinity'] = CPUAffinity(**data['writer_affinity'])
        return cls(**data)


@dataclass
class SystemConfig:
    """System-wide configuration."""
    # CPU isolation
    isolated_cpus: List[int] = None  # CPUs to isolate
    housekeeping_cpus: List[int] = None  # CPUs for system tasks
    
    # IRQ affinity
    network_irq_cpus: List[int] = None  # CPUs for network IRQs
    
    # Network optimization
    network_optimization: NetworkOptimization = None
    
    # Logging
    enable_central_logger: bool = False
    central_log_file: Optional[str] = None
    
    # Performance monitoring
    stats_update_interval: float = 1.0  # seconds
    enable_dashboard: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        if self.network_optimization:
            d['network_optimization'] = asdict(self.network_optimization)
        return d
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemConfig':
        """Create from dictionary."""
        if data.get('network_optimization'):
            data['network_optimization'] = NetworkOptimization(**data['network_optimization'])
        return cls(**data)


@dataclass
class MultiStreamConfig:
    """Complete multi-device streaming configuration."""
    version: int = 1
    description: str = ""
    devices: List[SR860DeviceConfig] = None
    system: SystemConfig = None
    duration: float = 0  # 0 = unlimited
    output_directory: str = "data"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'version': self.version,
            'description': self.description,
            'devices': [dev.to_dict() for dev in self.devices],
            'system': self.system.to_dict(),
            'duration': self.duration,
            'output_directory': self.output_directory
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MultiStreamConfig':
        """Create from dictionary."""
        devices = [SR860DeviceConfig.from_dict(d) for d in data['devices']]
        system = SystemConfig.from_dict(data['system'])
        return cls(
            version=data['version'],
            description=data.get('description', ''),
            devices=devices,
            system=system,
            duration=data.get('duration', 0),
            output_directory=data.get('output_directory', 'data')
        )
    
    def save(self, filename: str):
        """Save configuration to JSON file."""
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        print(f"Configuration saved to {filename}")
    
    @classmethod
    def load(cls, filename: str) -> 'MultiStreamConfig':
        """Load configuration from JSON file."""
        with open(filename, 'r') as f:
            data = json.load(f)
        return cls.from_dict(data)


class ConfigurationBuilder:
    """Interactive configuration builder."""
    
    def __init__(self):
        self.cpu_count = psutil.cpu_count()
        self.available_interfaces = self._get_network_interfaces()
    
    def _get_network_interfaces(self) -> List[str]:
        """Get available network interfaces."""
        try:
            result = subprocess.run(['ip', 'link', 'show'], 
                                  capture_output=True, text=True)
            interfaces = []
            for line in result.stdout.split('\n'):
                if ': ' in line and not line.startswith(' '):
                    parts = line.split(': ')
                    if len(parts) >= 2:
                        iface = parts[1].split('@')[0]
                        if iface not in ['lo', 'docker0']:
                            interfaces.append(iface)
            return interfaces
        except:
            return ['eth0', 'enp0s3']  # Common defaults
    
    def create_optimal_config(self, num_devices: int) -> MultiStreamConfig:
        """Create an optimal configuration for the given number of devices."""
        print(f"\nCreating optimal configuration for {num_devices} SR860 devices...")
        print(f"System has {self.cpu_count} CPU cores")
        
        # Determine CPU allocation strategy
        if self.cpu_count < 4:
            print("WARNING: System has fewer than 4 cores. Performance may be limited.")
            isolated_cpus = []
            housekeeping_cpus = list(range(self.cpu_count))
        else:
            # Reserve first 2 cores for system
            housekeeping_cpus = [0, 1]
            # Isolate remaining cores
            isolated_cpus = list(range(2, self.cpu_count))
            print(f"Isolating CPUs {isolated_cpus} for streaming")
        
        # Allocate CPUs to devices
        devices = []
        cpus_per_device = max(1, len(isolated_cpus) // num_devices) if isolated_cpus else 1
        
        for i in range(num_devices):
            # Get device details
            print(f"\n--- Device {i+1} Configuration ---")
            ip = input(f"Enter IP address for SR860 #{i+1} [192.168.1.{156+i}]: ")
            if not ip:
                ip = f"192.168.1.{156+i}"
            
            name = input(f"Enter friendly name [SR860-{i+1}]: ")
            if not name:
                name = f"SR860-{i+1}"
            
            # CPU allocation
            if isolated_cpus:
                start_cpu = 2 + (i * cpus_per_device)
                receiver_cpu = min(start_cpu, self.cpu_count - 1)
                writer_cpu = min(start_cpu + 1, self.cpu_count - 1) if cpus_per_device > 1 else receiver_cpu
            else:
                receiver_cpu = writer_cpu = i % self.cpu_count
            
            device_config = SR860DeviceConfig(
                name=name,
                ip=ip,
                channel="XYRT",
                format="float32",
                packet_size=1024,
                port=1865 + i,  # Use different ports
                receiver_affinity=CPUAffinity(
                    cpu_list=[receiver_cpu],
                    priority=90,
                    scheduler="SCHED_FIFO"
                ),
                writer_affinity=CPUAffinity(
                    cpu_list=[writer_cpu],
                    priority=50,
                    scheduler="SCHED_FIFO"
                )
            )
            devices.append(device_config)
        
        # Network optimization
        network_opt = NetworkOptimization(
            rss_queues=min(num_devices, 8),  # Up to 8 RSS queues
            rss_cpus=housekeeping_cpus if housekeeping_cpus else [0],
            rfs_enabled=True,
            xps_enabled=True
        )
        
        # System configuration
        system_config = SystemConfig(
            isolated_cpus=isolated_cpus,
            housekeeping_cpus=housekeeping_cpus,
            network_irq_cpus=housekeeping_cpus[:1],  # First housekeeping CPU
            network_optimization=network_opt,
            enable_central_logger=num_devices > 2,
            enable_dashboard=True
        )
        
        # Complete configuration
        duration = float(input("\nStreaming duration in seconds (0 for unlimited) [60]: ") or "60")
        
        config = MultiStreamConfig(
            description=f"Configuration for {num_devices} SR860 devices",
            devices=devices,
            system=system_config,
            duration=duration
        )
        
        return config
    
    def validate_config(self, config: MultiStreamConfig) -> List[str]:
        """Validate configuration and return list of warnings/errors."""
        issues = []
        
        # Check CPU allocations
        all_cpus = set()
        for device in config.devices:
            if device.receiver_affinity:
                all_cpus.update(device.receiver_affinity.cpu_list)
            if device.writer_affinity:
                all_cpus.update(device.writer_affinity.cpu_list)
        
        if max(all_cpus, default=0) >= self.cpu_count:
            issues.append(f"ERROR: CPU assignments exceed available cores ({self.cpu_count})")
        
        # Check for port conflicts
        ports = [dev.port for dev in config.devices]
        if len(ports) != len(set(ports)):
            issues.append("WARNING: Duplicate UDP ports detected")
        
        # Check network connectivity
        for device in config.devices:
            try:
                socket.create_connection((device.ip, 80), timeout=1).close()
            except:
                issues.append(f"WARNING: Cannot reach {device.name} at {device.ip}")
        
        # Check isolated CPUs
        if config.system.isolated_cpus:
            if not self._check_cpu_isolation():
                issues.append("INFO: CPU isolation not currently active. "
                            "Add isolcpus= to kernel cmdline and reboot.")
        
        return issues
    
    def _check_cpu_isolation(self) -> bool:
        """Check if CPU isolation is active."""
        try:
            with open('/proc/cmdline', 'r') as f:
                return 'isolcpus=' in f.read()
        except:
            return False


def create_example_configs():
    """Create example configuration files."""
    # Single device example
    single_config = MultiStreamConfig(
        description="Single SR860 streaming example",
        devices=[
            SR860DeviceConfig(
                name="SR860-Main",
                ip="192.168.1.156",
                channel="XYRT",
                format="float32",
                packet_size=1024,
                receiver_affinity=CPUAffinity(cpu_list=[2], priority=90),
                writer_affinity=CPUAffinity(cpu_list=[3], priority=50)
            )
        ],
        system=SystemConfig(
            isolated_cpus=[2, 3],
            housekeeping_cpus=[0, 1],
            network_optimization=NetworkOptimization()
        ),
        duration=60.0
    )
    single_config.save("sr860_single_example.json")
    
    # Dual device example
    dual_config = MultiStreamConfig(
        description="Dual SR860 synchronized streaming",
        devices=[
            SR860DeviceConfig(
                name="SR860-1",
                ip="192.168.1.156",
                port=1865,
                receiver_affinity=CPUAffinity(cpu_list=[2], priority=90),
                writer_affinity=CPUAffinity(cpu_list=[3], priority=50)
            ),
            SR860DeviceConfig(
                name="SR860-2",
                ip="192.168.1.157",
                port=1866,
                receiver_affinity=CPUAffinity(cpu_list=[4], priority=90),
                writer_affinity=CPUAffinity(cpu_list=[5], priority=50)
            )
        ],
        system=SystemConfig(
            isolated_cpus=[2, 3, 4, 5],
            housekeeping_cpus=[0, 1],
            network_irq_cpus=[1],
            network_optimization=NetworkOptimization(
                rss_queues=4,
                rss_cpus=[0, 1]
            ),
            enable_central_logger=True
        ),
        duration=300.0
    )
    dual_config.save("sr860_dual_example.json")
    
    print("Created example configurations:")
    print("  - sr860_single_example.json")
    print("  - sr860_dual_example.json")


def main():
    """Main configuration tool."""
    parser = argparse.ArgumentParser(
        description="SR860 Multi-Device Streaming Configuration Tool"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Create command
    create_parser = subparsers.add_parser('create', help='Create new configuration')
    create_parser.add_argument('--devices', type=int, required=True,
                              help='Number of SR860 devices')
    create_parser.add_argument('--output', '-o', default='sr860_config.json',
                              help='Output configuration file')
    create_parser.add_argument('--interactive', '-i', action='store_true',
                              help='Interactive configuration mode')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate configuration')
    validate_parser.add_argument('config', help='Configuration file to validate')
    
    # Examples command
    examples_parser = subparsers.add_parser('examples', help='Create example configs')
    
    # Show command
    show_parser = subparsers.add_parser('show', help='Display configuration')
    show_parser.add_argument('config', help='Configuration file to display')
    
    args = parser.parse_args()
    
    if args.command == 'create':
        builder = ConfigurationBuilder()
        config = builder.create_optimal_config(args.devices)
        
        # Validate
        issues = builder.validate_config(config)
        if issues:
            print("\nValidation results:")
            for issue in issues:
                print(f"  {issue}")
        
        # Save
        config.save(args.output)
        
    elif args.command == 'validate':
        builder = ConfigurationBuilder()
        config = MultiStreamConfig.load(args.config)
        issues = builder.validate_config(config)
        
        if not issues:
            print("✓ Configuration is valid")
        else:
            print("Validation results:")
            for issue in issues:
                print(f"  {issue}")
                
    elif args.command == 'examples':
        create_example_configs()
        
    elif args.command == 'show':
        config = MultiStreamConfig.load(args.config)
        print(json.dumps(config.to_dict(), indent=2))
        
    else:
        parser.print_help()


if __name__ == '__main__':
    main() 