#!/usr/bin/env python3
"""SR860 Delta-T Analyzer - Analyze timing between samples in binary files.

This script reads SR860 binary streaming files and analyzes the delta-t's
between samples, creating both histogram and time series plots.
"""

import argparse
import ast
import struct
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from datetime import datetime


class SR860DeltaTAnalyzer:
    """Analyze delta-t's between samples in SR860 binary files."""
    
    def __init__(self, filename: str):
        self.filename = Path(filename)
        self.header = None
        self.data = None
        self.sample_times = None
        self.delta_ts = None
        self._byte_order = None
        
    def read_header(self) -> Dict[str, Any]:
        """Read and parse file header."""
        with open(self.filename, 'rb') as f:
            # Read header size (4 bytes, little-endian)
            header_size = struct.unpack('<I', f.read(4))[0]
            
            # Validate header size and try big-endian if needed
            if header_size > 10000 or header_size < 10:
                # Try big-endian
                f.seek(0)
                header_size = struct.unpack('>I', f.read(4))[0]
                if header_size > 10000 or header_size < 10:
                    raise ValueError(f"Invalid header size: {header_size}")
                self._byte_order = '>'
                print("Note: Using big-endian byte order for header")
            else:
                self._byte_order = '<'
            
            # Read header data
            header_bytes = f.read(header_size)
            try:
                self.header = ast.literal_eval(header_bytes.decode('utf-8'))
            except:
                # Try other encodings
                try:
                    self.header = ast.literal_eval(header_bytes.decode('latin-1'))
                except:
                    raise ValueError("Could not decode header data")
            
            # Add computed fields
            self.header['header_size'] = header_size + 4
            self.header['data_offset'] = header_size + 4
            self.header['byte_order'] = self._byte_order
            
        return self.header
    
    def read_data(self) -> np.ndarray:
        """Read data from file."""
        if not self.header:
            self.read_header()
            
        # Determine data type and byte order
        if self.header['format'] == 0:
            dtype = np.float32
        else:
            dtype = np.int16
            
        # Get byte order - prioritize detected byte order from header
        byte_order = '<'  # Default to little-endian
        if 'detected_little_endian' in self.header and self.header['detected_little_endian'] is not None:
            # Use detected byte order from packet headers
            byte_order = '<' if self.header['detected_little_endian'] else '>'
            print(f"Using detected byte order from packets: {'little-endian' if self.header['detected_little_endian'] else 'big-endian'}")
        elif 'byte_order' in self.header:
            # Use byte order from header parsing
            byte_order = self.header['byte_order']
        
        bytes_per_sample = self.header['bytes_per_point'] * self.header['points_per_sample']
        
        with open(self.filename, 'rb') as f:
            # Skip header
            f.seek(self.header['data_offset'])
            
            # Read all data
            data_bytes = f.read()
            
            # Convert to numpy array with proper byte order
            if dtype == np.float32:
                if byte_order == '<':
                    data_flat = np.frombuffer(data_bytes, dtype='<f4')
                else:
                    data_flat = np.frombuffer(data_bytes, dtype='>f4')
            else:
                if byte_order == '<':
                    data_flat = np.frombuffer(data_bytes, dtype='<i2')
                else:
                    data_flat = np.frombuffer(data_bytes, dtype='>i2')
            
            # Check for data validity and try alternative byte order if needed
            if self.header['format'] == 0:  # float32
                invalid_count = np.sum(~np.isfinite(data_flat))
                if invalid_count > len(data_flat) * 0.5:  # More than 50% invalid
                    # Try opposite byte order
                    if byte_order == '<':
                        data_flat_alt = np.frombuffer(data_bytes, dtype='>f4')
                        byte_order_alt = '>'
                    else:
                        data_flat_alt = np.frombuffer(data_bytes, dtype='<f4')
                        byte_order_alt = '<'
                    
                    invalid_count_alt = np.sum(~np.isfinite(data_flat_alt))
                    if invalid_count_alt < invalid_count:
                        data_flat = data_flat_alt
                        byte_order = byte_order_alt
                        print(f"Note: Switched to {'little' if byte_order == '<' else 'big'}-endian byte order for better data validity")
            
            # Reshape based on channel configuration
            actual_samples = len(data_flat) // self.header['points_per_sample']
            self.data = data_flat[:actual_samples * self.header['points_per_sample']].reshape(
                actual_samples, self.header['points_per_sample']
            ).copy()
            
            # Final validation and cleaning
            if self.header['format'] == 0:  # float32
                # Replace any remaining NaN/inf with zeros for stability
                invalid_mask = ~np.isfinite(self.data)
                if np.any(invalid_mask):
                    print(f"Warning: Found {np.sum(invalid_mask)} invalid values, replacing with zeros")
                    self.data[invalid_mask] = 0.0
            
        return self.data
    
    def calculate_sample_times(self, sample_rate: Optional[float] = None) -> np.ndarray:
        """Calculate sample timestamps based on expected sample rate."""
        if self.data is None:
            self.read_data()
            
        n_samples = len(self.data)
        
        # Get sample rate from header or use provided rate
        if sample_rate is None:
            if 'actual_rate_hz' in self.header and self.header['actual_rate_hz'] is not None:
                sample_rate = self.header['actual_rate_hz']
            else:
                # Estimate from common SR860 rates
                sample_rate = 1e6  # Default to 1 MHz
                print(f"Warning: No sample rate in header, using {sample_rate:,.0f} Hz")
        
        # Calculate timestamps
        self.sample_times = np.arange(n_samples) / sample_rate
        
        return self.sample_times
    
    def calculate_delta_ts(self, sample_rate: Optional[float] = None) -> np.ndarray:
        """Calculate delta-t's between consecutive samples."""
        if self.sample_times is None:
            self.calculate_sample_times(sample_rate)
            
        # Calculate delta-t's (time differences between consecutive samples)
        self.delta_ts = np.diff(self.sample_times)
        
        return self.delta_ts
    
    def analyze_timing(self, sample_rate: Optional[float] = None) -> Dict[str, Any]:
        """Perform comprehensive timing analysis."""
        if self.delta_ts is None:
            self.calculate_delta_ts(sample_rate)
            
        # Get sample rate for analysis
        if sample_rate is None:
            if 'actual_rate_hz' in self.header and self.header['actual_rate_hz'] is not None:
                sample_rate = self.header['actual_rate_hz']
            else:
                sample_rate = 1e6
        
        expected_delta_t = 1.0 / sample_rate
        
        # Calculate statistics
        stats = {
            'n_samples': len(self.sample_times),
            'n_delta_ts': len(self.delta_ts),
            'expected_delta_t': expected_delta_t,
            'expected_delta_t_us': expected_delta_t * 1e6,
            'mean_delta_t': np.mean(self.delta_ts),
            'mean_delta_t_us': np.mean(self.delta_ts) * 1e6,
            'std_delta_t': np.std(self.delta_ts),
            'std_delta_t_us': np.std(self.delta_ts) * 1e6,
            'min_delta_t': np.min(self.delta_ts),
            'min_delta_t_us': np.min(self.delta_ts) * 1e6,
            'max_delta_t': np.max(self.delta_ts),
            'max_delta_t_us': np.max(self.delta_ts) * 1e6,
            'median_delta_t': np.median(self.delta_ts),
            'median_delta_t_us': np.median(self.delta_ts) * 1e6,
            'percentile_95': np.percentile(self.delta_ts, 95),
            'percentile_95_us': np.percentile(self.delta_ts, 95) * 1e6,
            'percentile_99': np.percentile(self.delta_ts, 99),
            'percentile_99_us': np.percentile(self.delta_ts, 99) * 1e6,
        }
        
        # Calculate timing accuracy
        stats['timing_accuracy_percent'] = ((stats['mean_delta_t'] - expected_delta_t) / expected_delta_t) * 100
        stats['timing_jitter_percent'] = (stats['std_delta_t'] / expected_delta_t) * 100
        
        # Detect anomalies (delta-t's significantly different from expected)
        threshold = expected_delta_t * 0.1  # 10% threshold
        anomalies = np.abs(self.delta_ts - expected_delta_t) > threshold
        stats['anomaly_count'] = np.sum(anomalies)
        stats['anomaly_percent'] = (stats['anomaly_count'] / len(self.delta_ts)) * 100
        
        # Detect gaps (delta-t's much larger than expected)
        gap_threshold = expected_delta_t * 2.0  # 2x expected
        gaps = self.delta_ts > gap_threshold
        stats['gap_count'] = np.sum(gaps)
        stats['gap_percent'] = (stats['gap_count'] / len(self.delta_ts)) * 100
        
        return stats
    
    def plot_histogram(self, sample_rate: Optional[float] = None, 
                      bins: int = 100, save_path: Optional[str] = None):
        """Plot histogram of delta-t's."""
        if self.delta_ts is None:
            self.calculate_delta_ts(sample_rate)
            
        stats = self.analyze_timing(sample_rate)
        
        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Main histogram
        ax1.hist(self.delta_ts * 1e6, bins=bins, alpha=0.7, color='blue', edgecolor='black')
        ax1.axvline(stats['expected_delta_t_us'], color='red', linestyle='--', 
                   label=f'Expected: {stats["expected_delta_t_us"]:.3f} μs')
        ax1.axvline(stats['mean_delta_t_us'], color='green', linestyle='--', 
                   label=f'Mean: {stats["mean_delta_t_us"]:.3f} μs')
        ax1.set_xlabel('Delta-T (μs)')
        ax1.set_ylabel('Count')
        ax1.set_title(f'Delta-T Histogram - {self.filename.name}')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Zoomed histogram around expected value
        expected = stats['expected_delta_t_us']
        std = stats['std_delta_t_us']
        x_min = max(0, expected - 3*std)
        x_max = expected + 3*std
        
        ax2.hist(self.delta_ts * 1e6, bins=bins, alpha=0.7, color='blue', edgecolor='black')
        ax2.axvline(stats['expected_delta_t_us'], color='red', linestyle='--', 
                   label=f'Expected: {stats["expected_delta_t_us"]:.3f} μs')
        ax2.axvline(stats['mean_delta_t_us'], color='green', linestyle='--', 
                   label=f'Mean: {stats["mean_delta_t_us"]:.3f} μs')
        ax2.set_xlim(x_min, x_max)
        ax2.set_xlabel('Delta-T (μs)')
        ax2.set_ylabel('Count')
        ax2.set_title('Zoomed View (±3σ)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Histogram saved to: {save_path}")
        else:
            plt.show()
        
        plt.close()
    
    def plot_time_series(self, sample_rate: Optional[float] = None, 
                        max_points: int = 10000, save_path: Optional[str] = None):
        """Plot delta-t's versus sample index."""
        if self.delta_ts is None:
            self.calculate_delta_ts(sample_rate)
            
        stats = self.analyze_timing(sample_rate)
        
        # Limit points for plotting
        if len(self.delta_ts) > max_points:
            # Sample evenly across the data
            step = len(self.delta_ts) // max_points
            plot_indices = np.arange(0, len(self.delta_ts), step)
            plot_delta_ts = self.delta_ts[plot_indices]
        else:
            plot_indices = np.arange(len(self.delta_ts))
            plot_delta_ts = self.delta_ts
        
        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        # Full time series
        ax1.plot(plot_indices, plot_delta_ts * 1e6, 'b-', alpha=0.7, linewidth=0.5)
        ax1.axhline(stats['expected_delta_t_us'], color='red', linestyle='--', 
                   label=f'Expected: {stats["expected_delta_t_us"]:.3f} μs')
        ax1.axhline(stats['mean_delta_t_us'], color='green', linestyle='--', 
                   label=f'Mean: {stats["mean_delta_t_us"]:.3f} μs')
        ax1.set_xlabel('Sample Index')
        ax1.set_ylabel('Delta-T (μs)')
        ax1.set_title(f'Delta-T Time Series - {self.filename.name}')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Zoomed view of first portion
        zoom_points = min(1000, len(plot_delta_ts))
        ax2.plot(plot_indices[:zoom_points], plot_delta_ts[:zoom_points] * 1e6, 
                'b-', alpha=0.7, linewidth=0.5)
        ax2.axhline(stats['expected_delta_t_us'], color='red', linestyle='--', 
                   label=f'Expected: {stats["expected_delta_t_us"]:.3f} μs')
        ax2.axhline(stats['mean_delta_t_us'], color='green', linestyle='--', 
                   label=f'Mean: {stats["mean_delta_t_us"]:.3f} μs')
        ax2.set_xlabel('Sample Index')
        ax2.set_ylabel('Delta-T (μs)')
        ax2.set_title(f'Zoomed View (First {zoom_points} samples)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Time series plot saved to: {save_path}")
        else:
            plt.show()
        
        plt.close()
    
    def print_analysis(self, sample_rate: Optional[float] = None):
        """Print comprehensive timing analysis."""
        if self.delta_ts is None:
            self.calculate_delta_ts(sample_rate)
            
        # Fix: Ensure sample_rate is not None for printing
        if sample_rate is None:
            if self.header and 'actual_rate_hz' in self.header and self.header['actual_rate_hz'] is not None:
                sample_rate = self.header['actual_rate_hz']
            else:
                sample_rate = 1e6

        stats = self.analyze_timing(sample_rate)
        
        print("\n" + "="*60)
        print("SR860 DELTA-T TIMING ANALYSIS")
        print("="*60)
        print(f"File: {self.filename.name}")
        print(f"Total samples: {stats['n_samples']:,}")
        print(f"Total delta-t's: {stats['n_delta_ts']:,}")
        
        print(f"\nExpected timing:")
        print(f"  Sample rate: {sample_rate:,.0f} Hz")
        print(f"  Expected delta-t: {stats['expected_delta_t']:.9f} s ({stats['expected_delta_t_us']:.3f} μs)")
        
        print(f"\nMeasured timing:")
        print(f"  Mean delta-t: {stats['mean_delta_t']:.9f} s ({stats['mean_delta_t_us']:.3f} μs)")
        print(f"  Median delta-t: {stats['median_delta_t']:.9f} s ({stats['median_delta_t_us']:.3f} μs)")
        print(f"  Std dev: {stats['std_delta_t']:.9f} s ({stats['std_delta_t_us']:.3f} μs)")
        print(f"  Min: {stats['min_delta_t']:.9f} s ({stats['min_delta_t_us']:.3f} μs)")
        print(f"  Max: {stats['max_delta_t']:.9f} s ({stats['max_delta_t_us']:.3f} μs)")
        
        print(f"\nPercentiles:")
        print(f"  95th percentile: {stats['percentile_95']:.9f} s ({stats['percentile_95_us']:.3f} μs)")
        print(f"  99th percentile: {stats['percentile_99']:.9f} s ({stats['percentile_99_us']:.3f} μs)")
        
        print(f"\nTiming accuracy:")
        print(f"  Accuracy: {stats['timing_accuracy_percent']:+.3f}%")
        print(f"  Jitter: {stats['timing_jitter_percent']:.3f}%")
        
        print(f"\nAnomalies:")
        print(f"  Anomalies (>10% from expected): {stats['anomaly_count']:,} ({stats['anomaly_percent']:.2f}%)")
        print(f"  Gaps (>2x expected): {stats['gap_count']:,} ({stats['gap_percent']:.2f}%)")
        
        # Performance assessment
        print(f"\n" + "="*50)
        print("PERFORMANCE ASSESSMENT")
        print("="*50)
        
        if stats['timing_jitter_percent'] < 1.0 and stats['anomaly_percent'] < 0.1:
            print("✅ EXCELLENT TIMING STABILITY")
            print("   • Very low jitter (<1%)")
            print("   • Very few anomalies (<0.1%)")
        elif stats['timing_jitter_percent'] < 5.0 and stats['anomaly_percent'] < 1.0:
            print("✅ GOOD TIMING STABILITY")
            print("   • Low jitter (<5%)")
            print("   • Few anomalies (<1%)")
        elif stats['timing_jitter_percent'] < 10.0 and stats['anomaly_percent'] < 5.0:
            print("⚠️  MODERATE TIMING STABILITY")
            print("   • Moderate jitter (<10%)")
            print("   • Some anomalies (<5%)")
        else:
            print("❌ POOR TIMING STABILITY")
            print("   • High jitter (>10%)")
            print("   • Many anomalies (>5%)")
            print("   • Consider checking SR860 configuration and network")
        
        if stats['gap_count'] > 0:
            print(f"\n⚠️  TIMING GAPS DETECTED")
            print(f"   • {stats['gap_count']} gaps found")
            print(f"   • May indicate packet loss or SR860 rate changes")
            print(f"   • Check SR860 time constant and filter settings")


def main():
    """Main application for delta-t analysis."""
    
    parser = argparse.ArgumentParser(
        description="SR860 Delta-T Analyzer - Analyze timing between samples",
        epilog="Creates histogram and time series plots of delta-t's between samples."
    )
    
    parser.add_argument("filename", help="SR860 binary file to analyze")
    parser.add_argument("--sample-rate", type=float, 
                       help="Sample rate in Hz (default: use header value)")
    parser.add_argument("--histogram", action="store_true", default=True,
                       help="Generate histogram plot (default: True)")
    parser.add_argument("--time-series", action="store_true", default=True,
                       help="Generate time series plot (default: True)")
    parser.add_argument("--save-plots", action="store_true",
                       help="Save plots to files instead of displaying")
    parser.add_argument("--output-dir", default=".",
                       help="Output directory for saved plots")
    parser.add_argument("--bins", type=int, default=100,
                       help="Number of histogram bins")
    parser.add_argument("--max-points", type=int, default=10000,
                       help="Maximum points for time series plot")
    
    args = parser.parse_args()
    
    # Check if file exists
    if not Path(args.filename).exists():
        print(f"Error: File '{args.filename}' not found")
        return
    
    # Create analyzer
    analyzer = SR860DeltaTAnalyzer(args.filename)
    
    try:
        # Read header and data
        print(f"Reading file: {args.filename}")
        analyzer.read_header()
        analyzer.read_data()
        
        # Print file info
        print(f"File info:")
        print(f"  Channels: {analyzer.header['points_per_sample']}")
        print(f"  Format: {'float32' if analyzer.header['format'] == 0 else 'int16'}")
        print(f"  Samples: {len(analyzer.data):,}")
        
        # Calculate delta-t's
        print("Calculating delta-t's...")
        analyzer.calculate_delta_ts(args.sample_rate)
        
        # Print analysis
        analyzer.print_analysis(args.sample_rate)
        
        # Generate plots
        if args.histogram or args.time_series:
            print("\nGenerating plots...")
            
            # Create output directory if needed
            output_dir = Path(args.output_dir)
            output_dir.mkdir(exist_ok=True)
            
            base_name = Path(args.filename).stem
            
            if args.histogram:
                hist_path = output_dir / f"{base_name}_delta_t_histogram.png" if args.save_plots else None
                analyzer.plot_histogram(args.sample_rate, args.bins, hist_path)
            
            if args.time_series:
                ts_path = output_dir / f"{base_name}_delta_t_timeseries.png" if args.save_plots else None
                analyzer.plot_time_series(args.sample_rate, args.max_points, ts_path)
        
        print("\nAnalysis complete!")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
 