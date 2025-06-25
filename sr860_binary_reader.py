#!/usr/bin/env python3
"""Binary file reader for SR860 configured stream output files.

This reader is specifically designed for files created by sr860_configured_stream.py
which contain only raw data (no packet headers).
"""

import argparse
import ast
import struct
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from datetime import datetime


class SR860BinaryReader:
    """Read SR860 binary streaming files from configured_stream.py."""
    
    # Channel configurations
    CHANNEL_NAMES = {
        0: ['X'],
        1: ['X', 'Y'],
        2: ['R', 'Theta'],
        3: ['X', 'Y', 'R', 'Theta']
    }
    
    def __init__(self, filename: str):
        self.filename = filename
        self.header = None
        self.data = None
        self._file_size = Path(filename).stat().st_size
        
    def read_header(self) -> Dict[str, Any]:
        """Read and parse file header."""
        with open(self.filename, 'rb') as f:
            # Read header size (4 bytes, little-endian)
            header_size = struct.unpack('<I', f.read(4))[0]
            
            # Read header data
            header_bytes = f.read(header_size)
            self.header = ast.literal_eval(header_bytes.decode('utf-8'))
            
            # Add computed fields
            self.header['header_size'] = header_size + 4
            self.header['data_offset'] = header_size + 4
            self.header['channel_names'] = self.CHANNEL_NAMES[self.header['channel']]
            
        return self.header
        
    def get_info(self) -> Dict[str, Any]:
        """Get comprehensive file information."""
        if not self.header:
            self.read_header()
            
        data_size = self._file_size - self.header['data_offset']
        bytes_per_sample = self.header['bytes_per_point'] * self.header['points_per_sample']
        n_samples = data_size // bytes_per_sample
        
        # Estimate sample rate (this is approximate since actual rate may vary)
        # You would need to know the actual streaming rate for accurate duration
        estimated_rate = 1e6  # Assume 1 MHz for estimation
        estimated_duration = n_samples / estimated_rate
        
        # Parse timestamp
        timestamp_str = datetime.fromtimestamp(self.header['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            'filename': Path(self.filename).name,
            'file_size_mb': self._file_size / 1e6,
            'header_size_bytes': self.header['header_size'],
            'data_size_mb': data_size / 1e6,
            'n_samples': n_samples,
            'bytes_per_sample': bytes_per_sample,
            'channel_config': self.CHANNEL_NAMES[self.header['channel']],
            'n_channels': self.header['points_per_sample'],
            'data_format': 'float32' if self.header['format'] == 0 else 'int16',
            'timestamp': timestamp_str,
            'duration_estimate_sec': estimated_duration,
            'samples_per_mb': 1e6 / bytes_per_sample
        }
        
    def read_data(self, start_sample: int = 0, n_samples: Optional[int] = None) -> np.ndarray:
        """Read data from file.
        
        Args:
            start_sample: Starting sample index
            n_samples: Number of samples to read (None = all remaining)
            
        Returns:
            numpy array of shape (n_samples, n_channels)
        """
        if not self.header:
            self.read_header()
            
        # Determine data type
        if self.header['format'] == 0:
            dtype = np.float32
        else:
            dtype = np.int16
            
        bytes_per_sample = self.header['bytes_per_point'] * self.header['points_per_sample']
        
        with open(self.filename, 'rb') as f:
            # Seek to start position
            f.seek(self.header['data_offset'] + start_sample * bytes_per_sample)
            
            # Read data
            if n_samples:
                data_bytes = f.read(n_samples * bytes_per_sample)
            else:
                data_bytes = f.read()
                
            # Convert to numpy array
            data_flat = np.frombuffer(data_bytes, dtype=dtype)
            
            # Reshape based on channel configuration
            actual_samples = len(data_flat) // self.header['points_per_sample']
            self.data = data_flat[:actual_samples * self.header['points_per_sample']].reshape(
                actual_samples, self.header['points_per_sample']
            )
            
        return self.data
        
    def get_statistics(self, max_samples: int = 100000) -> Dict[str, Any]:
        """Calculate statistics for each channel."""
        # Read up to max_samples for statistics
        data = self.read_data(n_samples=max_samples)
        
        stats = {}
        for i, name in enumerate(self.header['channel_names']):
            channel_data = data[:, i]
            stats[name] = {
                'mean': float(np.mean(channel_data)),
                'std': float(np.std(channel_data)),
                'min': float(np.min(channel_data)),
                'max': float(np.max(channel_data)),
                'rms': float(np.sqrt(np.mean(channel_data**2)))
            }
            
            # For int16 data, also show the range utilization
            if self.header['format'] == 1:  # int16
                # Range utilization: what percentage of the available dynamic range is being used
                # int16 range is -32768 to 32767 (65536 total values)
                stats[name]['range_utilization'] = (stats[name]['max'] - stats[name]['min']) / 65536 * 100
                
        return stats
        
    def detect_signal_frequency(self, channel_idx: int = 0, n_samples: int = 100000) -> float:
        """Detect the dominant frequency in a channel using FFT."""
        data = self.read_data(n_samples=min(n_samples, 1000000))
        
        if data.shape[0] < 1024:
            return 0.0
            
        # Use power of 2 for FFT
        n_fft = 2**int(np.log2(data.shape[0]))
        n_fft = min(n_fft, 65536)  # Limit FFT size
        
        # Apply window to reduce spectral leakage
        windowed_data = data[:n_fft, channel_idx] * np.hanning(n_fft)
        
        # Compute FFT
        fft_data = np.fft.fft(windowed_data)
        magnitude = np.abs(fft_data[:n_fft//2])
        
        # Find peak frequency (skip DC)
        peak_idx = np.argmax(magnitude[1:]) + 1
        
        # Assuming 1 MHz sample rate - this would need to be adjusted based on actual rate
        # In practice, you'd need to know the actual sample rate from the streaming configuration
        assumed_sample_rate = 1e6
        peak_freq = peak_idx * assumed_sample_rate / n_fft
        
        return peak_freq
        
    def estimate_experiment_duration(self) -> Dict[str, float]:
        """Estimate the actual experiment duration based on data patterns."""
        info = self.get_info()
        n_samples = info['n_samples']
        
        # Try to detect the actual sample rate by looking at the signal frequency
        # and assuming it should be a round number
        detected_freq = self.detect_signal_frequency()
        
        # Common SR860 streaming rates based on time constant
        common_rates = [
            1.25e6,   # 1 μs time constant
            625e3,    # 2 μs
            312.5e3,  # 4 μs
            156.25e3, # 8 μs
            78.125e3, # 16 μs
            39.0625e3,# 32 μs
            19.53125e3,# 64 μs
            9.765625e3,# 128 μs
            4.8828125e3,# 256 μs
            2.44140625e3,# 512 μs
            1.220703125e3,# 1 ms
            610.3515625,  # 2 ms
            305.17578125, # 4 ms
        ]
        
        # Find the closest common rate
        # First, try to estimate based on file size and typical efficiency
        estimated_rate = 1e6  # Default assumption
        
        # If we have a reasonable number of samples, estimate based on typical rates
        if n_samples > 10000:
            # Estimate based on closest common rate
            for rate in common_rates:
                # Assume experiments typically run for 1-60 seconds
                duration = n_samples / rate
                if 0.5 <= duration <= 120:  # Reasonable duration range
                    estimated_rate = rate
                    break
        
        return {
            'n_samples': n_samples,
            'estimated_sample_rate': estimated_rate,
            'estimated_duration': n_samples / estimated_rate,
            'detected_signal_freq': detected_freq,
        }
        
    def analyze_sampling(self, n_samples: int = 100000) -> Dict[str, Any]:
        """Analyze the sampling characteristics of the data."""
        data = self.read_data(n_samples=min(n_samples, 1000000))
        
        # Analyze first differences to detect sampling issues
        analysis = {}
        
        for i, name in enumerate(self.header['channel_names']):
            channel_data = data[:, i]
            
            # Calculate first differences
            diff = np.diff(channel_data)
            
            # Detect constant values (possible undersampling)
            n_zeros = np.sum(diff == 0)
            zero_ratio = n_zeros / len(diff)
            
            # Detect large jumps (possible aliasing)
            std_diff = np.std(diff)
            large_jumps = np.sum(np.abs(diff) > 5 * std_diff)
            
            analysis[name] = {
                'consecutive_duplicates': n_zeros,
                'duplicate_ratio': zero_ratio * 100,
                'large_jumps': large_jumps,
                'diff_std': float(std_diff),
                'looks_undersampled': zero_ratio > 0.1,  # More than 10% duplicates
                'possible_aliasing': large_jumps > len(diff) * 0.01  # More than 1% large jumps
            }
            
        return analysis
        
    def plot_time_series(self, n_samples: int = 10000, start_sample: int = 0):
        """Plot time series data."""
        data = self.read_data(start_sample=start_sample, n_samples=n_samples)
        
        n_channels = len(self.header['channel_names'])
        fig, axes = plt.subplots(n_channels, 1, figsize=(12, 3*n_channels), sharex=True)
        
        if n_channels == 1:
            axes = [axes]
            
        # Create time axis (assuming 1 MHz for display purposes)
        time_ms = np.arange(n_samples) / 1000  # Convert to milliseconds
        
        for i, (ax, name) in enumerate(zip(axes, self.header['channel_names'])):
            ax.plot(time_ms, data[:, i], linewidth=0.5)
            ax.set_ylabel(f'{name}\n({self._get_units(name)})')
            ax.grid(True, alpha=0.3)
            ax.set_xlim(time_ms[0], time_ms[-1])
            
            # Add statistics to plot
            mean_val = np.mean(data[:, i])
            std_val = np.std(data[:, i])
            ax.axhline(mean_val, color='red', linestyle='--', alpha=0.5, label=f'Mean: {mean_val:.3e}')
            ax.fill_between(time_ms, mean_val-std_val, mean_val+std_val, alpha=0.2, color='red')
            ax.legend(loc='upper right')
            
        axes[-1].set_xlabel('Time (ms)')
        plt.suptitle(f'SR860 Time Series - {Path(self.filename).name}')
        plt.tight_layout()
        return fig
        
    def plot_spectrum(self, n_samples: int = 65536, start_sample: int = 0, 
                     sample_rate: float = 1e6):
        """Plot frequency spectrum using FFT."""
        data = self.read_data(start_sample=start_sample, n_samples=n_samples)
        
        # Use power of 2 for FFT efficiency
        n_fft = min(n_samples, 2**int(np.log2(n_samples)))
        
        n_channels = len(self.header['channel_names'])
        fig, axes = plt.subplots(n_channels, 1, figsize=(12, 3*n_channels), sharex=True)
        
        if n_channels == 1:
            axes = [axes]
            
        # Frequency axis
        freqs = np.fft.fftfreq(n_fft, 1/sample_rate)[:n_fft//2]
        
        for i, (ax, name) in enumerate(zip(axes, self.header['channel_names'])):
            # Apply window to reduce spectral leakage
            windowed_data = data[:n_fft, i] * np.hanning(n_fft)
            
            # Compute FFT
            fft_data = np.fft.fft(windowed_data)
            magnitude = np.abs(fft_data[:n_fft//2]) * 2 / n_fft
            
            # Convert to dB
            magnitude_db = 20 * np.log10(magnitude + 1e-12)  # Add small value to avoid log(0)
            
            ax.semilogx(freqs[1:], magnitude_db[1:])  # Skip DC
            ax.set_ylabel(f'{name}\n(dB)')
            ax.grid(True, alpha=0.3, which='both')
            ax.set_xlim(1, sample_rate/2)
            ax.set_ylim(bottom=-120)
            
        axes[-1].set_xlabel('Frequency (Hz)')
        plt.suptitle(f'SR860 Frequency Spectrum - {Path(self.filename).name}')
        plt.tight_layout()
        return fig
        
    def _get_units(self, channel_name: str) -> str:
        """Get appropriate units for channel."""
        if channel_name in ['X', 'Y', 'R']:
            return 'V' if self.header['format'] == 0 else 'counts'
        elif channel_name == 'Theta':
            return 'deg' if self.header['format'] == 0 else 'counts'
        return ''
        
    def export_to_csv(self, output_file: str, n_samples: Optional[int] = None,
                     delimiter: str = ','):
        """Export data to CSV file."""
        data = self.read_data(n_samples=n_samples)
        
        # Create reader_output directory if it doesn't exist
        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_dir = Path("reader_output")
            output_dir.mkdir(exist_ok=True)
            output_path = output_dir / output_file
        
        # Create header
        header_line = delimiter.join(['Sample'] + self.header['channel_names'])
        
        # Save to CSV
        with open(output_path, 'w') as f:
            f.write(header_line + '\n')
            for i, row in enumerate(data):
                f.write(f"{i}{delimiter}{delimiter.join(map(str, row))}\n")
                
        print(f"Exported {len(data)} samples to {output_path}")

    def plot_sampling_analysis(self, n_samples: int = 10000):
        """Create plots to analyze sampling characteristics."""
        data = self.read_data(n_samples=min(n_samples, 100000))
        n_channels = len(self.header['channel_names'])
        
        fig, axes = plt.subplots(n_channels, 3, figsize=(15, 3*n_channels))
        if n_channels == 1:
            axes = axes.reshape(1, -1)
            
        for i, name in enumerate(self.header['channel_names']):
            channel_data = data[:, i]
            
            # 1. Histogram of values
            ax1 = axes[i, 0]
            ax1.hist(channel_data, bins=100, alpha=0.7, edgecolor='black')
            ax1.set_xlabel(f'{name} Value')
            ax1.set_ylabel('Count')
            ax1.set_title(f'{name} Distribution')
            ax1.grid(True, alpha=0.3)
            
            # 2. First differences histogram
            ax2 = axes[i, 1]
            diff = np.diff(channel_data)
            ax2.hist(diff, bins=100, alpha=0.7, edgecolor='black', color='orange')
            ax2.set_xlabel(f'Δ{name}')
            ax2.set_ylabel('Count')
            ax2.set_title(f'{name} First Differences')
            ax2.grid(True, alpha=0.3)
            
            # Add text showing duplicate ratio
            n_zeros = np.sum(diff == 0)
            zero_ratio = n_zeros / len(diff) * 100
            ax2.text(0.95, 0.95, f'Duplicates: {zero_ratio:.1f}%', 
                    transform=ax2.transAxes, ha='right', va='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            # 3. Autocorrelation
            ax3 = axes[i, 2]
            # Compute autocorrelation for first 1000 lags
            n_lags = min(1000, len(channel_data)//2)
            autocorr = np.correlate(channel_data - np.mean(channel_data), 
                                   channel_data - np.mean(channel_data), mode='full')
            autocorr = autocorr[len(autocorr)//2:len(autocorr)//2 + n_lags]
            autocorr = autocorr / autocorr[0]  # Normalize
            
            ax3.plot(autocorr[:100])  # Show first 100 lags
            ax3.set_xlabel('Lag')
            ax3.set_ylabel('Autocorrelation')
            ax3.set_title(f'{name} Autocorrelation')
            ax3.grid(True, alpha=0.3)
            ax3.axhline(0, color='black', linewidth=0.5)
            
        plt.suptitle(f'Sampling Analysis - {Path(self.filename).name}')
        plt.tight_layout()
        return fig

    def save_plots(self, base_filename: Optional[str] = None, sample_rate: float = 1e6):
        """Save all analysis plots to the reader_output folder."""
        # Create reader_output directory if it doesn't exist
        output_dir = Path("reader_output")
        output_dir.mkdir(exist_ok=True)
        
        # Use input filename as base if not specified
        if base_filename is None:
            base_filename = Path(self.filename).stem
        
        info = self.get_info()
        
        # Time series plot
        fig1 = self.plot_time_series(n_samples=min(10000, info['n_samples']))
        fig1.savefig(output_dir / f"{base_filename}_time_series.png", dpi=150, bbox_inches='tight')
        plt.close(fig1)
        
        # Frequency spectrum plot
        if info['n_samples'] >= 1024:
            fig2 = self.plot_spectrum(sample_rate=sample_rate)
            fig2.savefig(output_dir / f"{base_filename}_spectrum.png", dpi=150, bbox_inches='tight')
            plt.close(fig2)
        
        # Sampling analysis plot
        fig3 = self.plot_sampling_analysis(n_samples=min(10000, info['n_samples']))
        fig3.savefig(output_dir / f"{base_filename}_sampling_analysis.png", dpi=150, bbox_inches='tight')
        plt.close(fig3)
        
        print(f"Saved plots to {output_dir}/")


def analyze_file(filename: str, plot: bool = True, sample_rate: float = 1e6):
    """Comprehensive analysis of SR860 binary file."""
    
    reader = SR860BinaryReader(filename)
    
    # Print file information
    print("\n" + "="*60)
    print("SR860 BINARY FILE ANALYSIS")
    print("="*60)
    
    info = reader.get_info()
    print("\nFile Information:")
    print("-" * 20)
    for key, value in info.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.3f}")
        else:
            print(f"  {key}: {value}")
    
    # Estimate experiment duration and sample rate
    print("\nExperiment Timing:")
    print("-" * 20)
    duration_info = reader.estimate_experiment_duration()
    print(f"  Total samples: {duration_info['n_samples']:,}")
    print(f"  Estimated sample rate: {duration_info['estimated_sample_rate']:,.0f} Hz")
    print(f"  Estimated duration: {duration_info['estimated_duration']:.3f} seconds")
    if duration_info['detected_signal_freq'] > 0:
        print(f"  Detected signal frequency: {duration_info['detected_signal_freq']:.1f} Hz")
    
    # Calculate and print statistics
    print("\nChannel Statistics:")
    print("-" * 20)
    stats = reader.get_statistics()
    
    for channel, channel_stats in stats.items():
        print(f"\n{channel} Channel:")
        for stat_name, value in channel_stats.items():
            if stat_name == 'range_utilization':
                print(f"  {stat_name}: {value:.1f}% (portion of ±32767 range used)")
            else:
                print(f"  {stat_name}: {value:.6e}")
    
    # Sampling analysis
    print("\nSampling Analysis:")
    print("-" * 20)
    sampling_analysis = reader.analyze_sampling()
    
    for channel, analysis in sampling_analysis.items():
        print(f"\n{channel} Channel:")
        print(f"  Consecutive duplicates: {analysis['consecutive_duplicates']:,} ({analysis['duplicate_ratio']:.1f}%)")
        print(f"  Large jumps (>5σ): {analysis['large_jumps']:,}")
        if analysis['looks_undersampled']:
            print("  ⚠️  WARNING: High duplicate ratio suggests possible undersampling")
        if analysis['possible_aliasing']:
            print("  ⚠️  WARNING: Many large jumps suggest possible aliasing")
    
    # Generate plots if requested
    if plot:
        print("\nGenerating plots...")
        
        # Time series plot
        reader.plot_time_series(n_samples=min(10000, info['n_samples']))
        
        # Frequency spectrum plot
        if info['n_samples'] >= 1024:
            reader.plot_spectrum(sample_rate=sample_rate)
        
        # Sampling analysis plot
        reader.plot_sampling_analysis(n_samples=min(10000, info['n_samples']))
        
        plt.show()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="SR860 Binary File Reader for configured_stream output files"
    )
    parser.add_argument("filename", help="Binary file to analyze")
    parser.add_argument("--no-plot", action="store_true", 
                       help="Skip plotting (only show statistics)")
    parser.add_argument("--save-plots", action="store_true",
                       help="Save plots to reader_output folder")
    parser.add_argument("--export-csv", help="Export data to CSV file")
    parser.add_argument("--n-samples", type=int, 
                       help="Number of samples to process (default: all)")
    parser.add_argument("--sample-rate", type=float, default=1e6,
                       help="Assumed sample rate in Hz (default: 1e6)")
    
    args = parser.parse_args()
    
    # Check file exists
    if not Path(args.filename).exists():
        print(f"Error: File '{args.filename}' not found")
        return
    
    # Analyze file
    analyze_file(args.filename, plot=not args.no_plot and not args.save_plots, sample_rate=args.sample_rate)
    
    # Save plots if requested
    if args.save_plots:
        reader = SR860BinaryReader(args.filename)
        reader.save_plots(sample_rate=args.sample_rate)
    
    # Export to CSV if requested
    if args.export_csv:
        reader = SR860BinaryReader(args.filename)
        reader.export_to_csv(args.export_csv, n_samples=args.n_samples)


if __name__ == "__main__":
    main() 