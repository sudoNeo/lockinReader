#!/usr/bin/env python3
"""System optimization helper for SR860 multi-device streaming.

This module provides tools to configure Linux system settings for optimal
real-time streaming performance, including CPU isolation, network tuning,
and IRQ affinity management.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import psutil


class SystemOptimizer:
    """System optimization helper for real-time streaming."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.is_root = os.geteuid() == 0
        
    def check_prerequisites(self) -> Dict[str, bool]:
        """Check system prerequisites."""
        checks = {
            'root_access': self.is_root,
            'isolcpus_support': self._check_isolcpus(),
            'rss_support': self._check_rss_support(),
            'rfs_support': self._check_rfs_support(),
            'xps_support': self._check_xps_support(),
            'rt_scheduler': self._check_rt_scheduler(),
            'huge_pages': self._check_huge_pages()
        }
        return checks
    
    def _check_isolcpus(self) -> bool:
        """Check if CPU isolation is configured."""
        try:
            with open('/proc/cmdline', 'r') as f:
                return 'isolcpus=' in f.read()
        except:
            return False
    
    def _check_rss_support(self) -> bool:
        """Check RSS (Receive Side Scaling) support."""
        try:
            # Check if any network interface has multiple queues
            for iface in psutil.net_if_addrs():
                queue_path = f"/sys/class/net/{iface}/queues"
                if Path(queue_path).exists():
                    rx_queues = list(Path(queue_path).glob("rx-*"))
                    if len(rx_queues) > 1:
                        return True
            return False
        except:
            return False
    
    def _check_rfs_support(self) -> bool:
        """Check RFS (Receive Flow Steering) support."""
        return Path("/proc/sys/net/core/rps_sock_flow_entries").exists()
    
    def _check_xps_support(self) -> bool:
        """Check XPS (Transmit Packet Steering) support."""
        try:
            for iface in psutil.net_if_addrs():
                xps_path = f"/sys/class/net/{iface}/queues/tx-0/xps_cpus"
                if Path(xps_path).exists():
                    return True
            return False
        except:
            return False
    
    def _check_rt_scheduler(self) -> bool:
        """Check if real-time scheduler is available."""
        try:
            # Try to get RT priority limit
            result = subprocess.run(['ulimit', '-r'], 
                                  shell=True, capture_output=True, text=True)
            return result.returncode == 0
        except:
            return False
    
    def _check_huge_pages(self) -> bool:
        """Check if huge pages are available."""
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if line.startswith('HugePages_Total:'):
                        return int(line.split()[1]) > 0
            return False
        except:
            return False
    
    def optimize_network_interface(self, interface: str, num_queues: int = 4,
                                 rss_cpus: List[int] = None) -> List[str]:
        """Optimize network interface settings."""
        commands = []
        
        # Set number of combined queues
        cmd = f"ethtool -L {interface} combined {num_queues}"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        # Configure RSS CPU affinity
        if rss_cpus:
            for i in range(num_queues):
                irq = self._find_queue_irq(interface, f"rx-{i}")
                if irq:
                    cpumask = self._cpulist_to_mask(rss_cpus)
                    cmd = f"echo {cpumask} > /proc/irq/{irq}/smp_affinity"
                    commands.append(cmd)
                    if not self.dry_run and self.is_root:
                        subprocess.run(cmd, shell=True)
        
        # Enable interrupt coalescing optimization
        cmd = f"ethtool -C {interface} adaptive-rx on adaptive-tx on"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        # Increase ring buffer sizes
        cmd = f"ethtool -G {interface} rx 4096 tx 4096"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        return commands
    
    def configure_rfs(self, entries: int = 32768) -> List[str]:
        """Configure Receive Flow Steering."""
        commands = []
        
        # Set global RFS entries
        cmd = f"sysctl -w net.core.rps_sock_flow_entries={entries}"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        # Configure per-queue flow count
        for iface in psutil.net_if_addrs():
            for rx_queue in Path(f"/sys/class/net/{iface}/queues").glob("rx-*"):
                rps_flow_cnt = rx_queue / "rps_flow_cnt"
                if rps_flow_cnt.exists():
                    cmd = f"echo 4096 > {rps_flow_cnt}"
                    commands.append(cmd)
                    if not self.dry_run and self.is_root:
                        subprocess.run(cmd, shell=True)
        
        return commands
    
    def configure_xps(self, interface: str, cpu_mapping: Dict[int, List[int]]) -> List[str]:
        """Configure Transmit Packet Steering."""
        commands = []
        
        for queue, cpus in cpu_mapping.items():
            xps_path = f"/sys/class/net/{interface}/queues/tx-{queue}/xps_cpus"
            if Path(xps_path).exists():
                cpumask = self._cpulist_to_mask(cpus)
                cmd = f"echo {cpumask} > {xps_path}"
                commands.append(cmd)
                if not self.dry_run and self.is_root:
                    subprocess.run(cmd, shell=True)
        
        return commands
    
    def set_network_tunables(self) -> List[str]:
        """Set network stack tunables for high-performance streaming."""
        tunables = {
            # Core network settings
            'net.core.netdev_max_backlog': 10000,
            'net.core.netdev_budget': 600,
            'net.core.netdev_budget_usecs': 2000,
            
            # Socket buffer sizes
            'net.core.rmem_max': 134217728,  # 128MB
            'net.core.wmem_max': 134217728,  # 128MB
            'net.core.rmem_default': 67108864,  # 64MB
            'net.core.wmem_default': 67108864,  # 64MB
            
            # UDP specific
            'net.ipv4.udp_mem': '12582912 16777216 25165824',
            'net.ipv4.udp_rmem_min': 8192,
            'net.ipv4.udp_wmem_min': 8192,
            
            # Busy polling
            'net.core.busy_poll': 50,
            'net.core.busy_read': 50,
            
            # Disable reverse path filtering for multicast
            'net.ipv4.conf.all.rp_filter': 0,
            'net.ipv4.conf.default.rp_filter': 0,
        }
        
        commands = []
        for param, value in tunables.items():
            cmd = f"sysctl -w {param}={value}"
            commands.append(cmd)
            if not self.dry_run and self.is_root:
                subprocess.run(cmd.split(), capture_output=True)
        
        return commands
    
    def configure_irq_affinity(self, interface: str, cpus: List[int]) -> List[str]:
        """Configure IRQ affinity for network interface."""
        commands = []
        irqs = self._find_interface_irqs(interface)
        
        cpumask = self._cpulist_to_mask(cpus)
        for irq in irqs:
            cmd = f"echo {cpumask} > /proc/irq/{irq}/smp_affinity"
            commands.append(cmd)
            if not self.dry_run and self.is_root:
                subprocess.run(cmd, shell=True)
        
        return commands
    
    def disable_irqbalance(self) -> List[str]:
        """Disable irqbalance service."""
        commands = []
        
        # Stop irqbalance
        cmd = "systemctl stop irqbalance"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        # Disable irqbalance
        cmd = "systemctl disable irqbalance"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        return commands
    
    def set_cpu_performance_governor(self, cpus: List[int] = None) -> List[str]:
        """Set CPU frequency governor to performance mode."""
        commands = []
        
        if cpus is None:
            cpus = list(range(psutil.cpu_count()))
        
        for cpu in cpus:
            gov_path = f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor"
            if Path(gov_path).exists():
                cmd = f"echo performance > {gov_path}"
                commands.append(cmd)
                if not self.dry_run and self.is_root:
                    subprocess.run(cmd, shell=True)
        
        return commands
    
    def configure_huge_pages(self, pages: int = 1024) -> List[str]:
        """Configure huge pages for better memory performance."""
        commands = []
        
        # Set number of huge pages
        cmd = f"sysctl -w vm.nr_hugepages={pages}"
        commands.append(cmd)
        if not self.dry_run and self.is_root:
            subprocess.run(cmd.split(), capture_output=True)
        
        return commands
    
    def _find_interface_irqs(self, interface: str) -> List[int]:
        """Find IRQ numbers for network interface."""
        irqs = []
        try:
            result = subprocess.run(['grep', interface, '/proc/interrupts'],
                                  capture_output=True, text=True)
            for line in result.stdout.split('\n'):
                if line.strip():
                    irq = int(line.split(':')[0].strip())
                    irqs.append(irq)
        except:
            pass
        return irqs
    
    def _find_queue_irq(self, interface: str, queue: str) -> Optional[int]:
        """Find IRQ number for specific queue."""
        try:
            result = subprocess.run(['grep', f'{interface}-{queue}', '/proc/interrupts'],
                                  capture_output=True, text=True)
            if result.stdout:
                return int(result.stdout.split(':')[0].strip())
        except:
            pass
        return None
    
    def _cpulist_to_mask(self, cpus: List[int]) -> str:
        """Convert CPU list to hex mask."""
        mask = 0
        for cpu in cpus:
            mask |= (1 << cpu)
        return f"{mask:x}"
    
    def generate_optimization_script(self, interface: str = "eth0",
                                   isolated_cpus: List[int] = None,
                                   network_cpus: List[int] = None) -> str:
        """Generate a complete optimization script."""
        if isolated_cpus is None:
            isolated_cpus = list(range(2, psutil.cpu_count()))
        if network_cpus is None:
            network_cpus = [0, 1]
        
        script = """#!/bin/bash
# SR860 Multi-Device Streaming System Optimization Script
# Generated by sr860_system_optimizer.py

set -e

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root"
    exit 1
fi

echo "Applying SR860 streaming optimizations..."

"""
        
        # Disable irqbalance
        script += "# Disable irqbalance\n"
        script += "systemctl stop irqbalance 2>/dev/null || true\n"
        script += "systemctl disable irqbalance 2>/dev/null || true\n\n"
        
        # CPU governor
        script += "# Set CPU performance governor\n"
        for cpu in range(psutil.cpu_count()):
            script += f"echo performance > /sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor 2>/dev/null || true\n"
        script += "\n"
        
        # Network tunables
        script += "# Network stack tunables\n"
        for cmd in self.set_network_tunables():
            script += f"{cmd}\n"
        script += "\n"
        
        # Network interface optimization
        script += f"# Optimize network interface {interface}\n"
        for cmd in self.optimize_network_interface(interface, 4, network_cpus):
            script += f"{cmd} 2>/dev/null || true\n"
        script += "\n"
        
        # RFS configuration
        script += "# Configure Receive Flow Steering\n"
        for cmd in self.configure_rfs():
            script += f"{cmd}\n"
        script += "\n"
        
        # XPS configuration
        script += "# Configure Transmit Packet Steering\n"
        xps_mapping = {i: [network_cpus[i % len(network_cpus)]] for i in range(4)}
        for cmd in self.configure_xps(interface, xps_mapping):
            script += f"{cmd}\n"
        script += "\n"
        
        # IRQ affinity
        script += f"# Set IRQ affinity for {interface}\n"
        for cmd in self.configure_irq_affinity(interface, network_cpus):
            script += f"{cmd}\n"
        script += "\n"
        
        # Huge pages
        script += "# Configure huge pages\n"
        script += "sysctl -w vm.nr_hugepages=1024\n\n"
        
        script += "echo 'Optimizations applied successfully!'\n"
        script += "echo 'Note: CPU isolation requires kernel parameter isolcpus= and reboot'\n"
        
        return script


def print_system_info():
    """Print current system information."""
    print("System Information:")
    print(f"  CPU cores: {psutil.cpu_count()}")
    print(f"  Memory: {psutil.virtual_memory().total / (1024**3):.1f} GB")
    
    # Check current CPU isolation
    try:
        with open('/proc/cmdline', 'r') as f:
            cmdline = f.read()
            if 'isolcpus=' in cmdline:
                import re
                match = re.search(r'isolcpus=([^ ]+)', cmdline)
                if match:
                    print(f"  Isolated CPUs: {match.group(1)}")
            else:
                print("  Isolated CPUs: None")
    except:
        pass
    
    # Network interfaces
    print("\nNetwork Interfaces:")
    for iface, addrs in psutil.net_if_addrs().items():
        if iface not in ['lo']:
            print(f"  {iface}:")
            # Check RSS queues
            queue_path = f"/sys/class/net/{iface}/queues"
            if Path(queue_path).exists():
                rx_queues = list(Path(queue_path).glob("rx-*"))
                tx_queues = list(Path(queue_path).glob("tx-*"))
                print(f"    RX queues: {len(rx_queues)}")
                print(f"    TX queues: {len(tx_queues)}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="SR860 System Optimization Helper"
    )
    
    parser.add_argument('--check', action='store_true',
                       help='Check system prerequisites')
    parser.add_argument('--optimize', action='store_true',
                       help='Apply optimizations (requires root)')
    parser.add_argument('--generate-script', action='store_true',
                       help='Generate optimization script')
    parser.add_argument('--interface', default='eth0',
                       help='Network interface to optimize')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show commands without executing')
    
    args = parser.parse_args()
    
    optimizer = SystemOptimizer(dry_run=args.dry_run)
    
    if args.check:
        print_system_info()
        print("\nPrerequisite Check:")
        checks = optimizer.check_prerequisites()
        for check, status in checks.items():
            status_str = "✓" if status else "✗"
            print(f"  {check}: {status_str}")
            
    elif args.optimize:
        if not optimizer.is_root and not args.dry_run:
            print("Error: Optimization requires root privileges")
            sys.exit(1)
            
        print("Applying system optimizations...")
        
        # Apply optimizations
        commands = []
        commands.extend(optimizer.set_network_tunables())
        commands.extend(optimizer.optimize_network_interface(args.interface))
        commands.extend(optimizer.configure_rfs())
        commands.extend(optimizer.disable_irqbalance())
        commands.extend(optimizer.set_cpu_performance_governor())
        
        if args.dry_run:
            print("\nCommands that would be executed:")
            for cmd in commands:
                print(f"  {cmd}")
        else:
            print(f"Executed {len(commands)} optimization commands")
            
    elif args.generate_script:
        script = optimizer.generate_optimization_script(args.interface)
        filename = "optimize_sr860_system.sh"
        with open(filename, 'w') as f:
            f.write(script)
        os.chmod(filename, 0o755)
        print(f"Generated optimization script: {filename}")
        print("Run with: sudo ./optimize_sr860_system.sh")
        
    else:
        parser.print_help()


if __name__ == '__main__':
    main() 