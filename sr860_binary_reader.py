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
        self._byte_order = '<'  # Default to little-endian
        
    def read_header(self) -> Dict[str, Any]:
        """Read and parse file header."""
        with open(self.filename, 'rb') as f:
            # Read header size (4 bytes, little-endian)
            header_size = struct.unpack('<I', f.read(4))[0]
            
            # Validate header size
            if header_size > 10000 or header_size < 10:
                # Try big-endian
                f.seek(0)
                header_size = struct.unpack('>I', f.read(4))[0]
                if header_size > 10000 or header_size < 10:
                    raise ValueError(f"Invalid header size: {header_size}")
                self._byte_order = '>'
                print("Note: Using big-endian byte order for header")
            
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
            self.header['channel_names'] = self.CHANNEL_NAMES[self.header['channel']]
            self.header['byte_order'] = self._byte_order
            
        return self.header
        
    def get_info(self) -> Dict[str, Any]:
        """Get comprehensive file information."""
        if not self.header:
            self.read_header()
            
        data_size = self._file_size - self.header['data_offset']
        bytes_per_sample = self.header['bytes_per_point'] * self.header['points_per_sample']
        n_samples = data_size // bytes_per_sample
        
        # Get actual sample rate if available in header
        actual_rate = None
        if 'actual_rate_hz' in self.header and self.header['actual_rate_hz'] is not None:
            actual_rate = self.header['actual_rate_hz']
            duration = n_samples / actual_rate
        else:
            # Fallback estimation
            estimated_rate = 1e6  # Assume 1 MHz for estimation
            duration = n_samples / estimated_rate
        
        # Parse timestamp
        timestamp_str = datetime.fromtimestamp(self.header['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        
        info = {
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
            'duration_estimate_sec': duration,
            'samples_per_mb': 1e6 / bytes_per_sample
        }
        
        # Add rate information if available
        if actual_rate:
            info['actual_sample_rate_hz'] = actual_rate
            info['duration_actual_sec'] = duration
            info['rate_source'] = 'header'
        else:
            info['rate_source'] = 'estimated'
            
        return info
        
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
            
        # Determine data type and byte order
        if self.header['format'] == 0:
            dtype = np.float32
        else:
            dtype = np.int16
            
        # Get byte order from header if available, otherwise try to detect
        byte_order = self.header.get('byte_order', '<')
        
        bytes_per_sample = self.header['bytes_per_point'] * self.header['points_per_sample']
        
        with open(self.filename, 'rb') as f:
            # Seek to start position
            f.seek(self.header['data_offset'] + start_sample * bytes_per_sample)
            
            # Read data
            if n_samples:
                data_bytes = f.read(n_samples * bytes_per_sample)
            else:
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
                        print(f"Note: Switched to {byte_order} byte order for better data validity")
                        # Update header
                        self.header['byte_order'] = byte_order
            
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
                
                # Check for extreme values that might indicate byte order issues
                max_abs_val = np.max(np.abs(self.data))
                if max_abs_val > 1e30:  # Unreasonably large values
                    print(f"Warning: Found extremely large values (max abs: {max_abs_val:.2e})")
                    print("This might indicate a byte order or format mismatch")
                    
                    # Try to detect if this is actually int16 data
                    if len(data_bytes) >= 4:
                        test_bytes = data_bytes[:4]
                        try_int16_le = np.frombuffer(test_bytes, dtype='<i2')
                        try_int16_be = np.frombuffer(test_bytes, dtype='>i2')
                        
                        if np.all(np.abs(try_int16_le) < 1e6) and np.all(np.abs(try_int16_be) < 1e6):
                            print("Note: Data might be int16 format instead of float32")
            
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
        
        # First, check if we have the actual rate in the header (new format)
        if self.header and 'actual_rate_hz' in self.header and self.header['actual_rate_hz'] is not None:
            actual_rate = self.header['actual_rate_hz']
            duration = n_samples / actual_rate
            return {
                'n_samples': n_samples,
                'estimated_sample_rate': actual_rate,
                'actual_sample_rate': actual_rate,  # This is the real rate
                'estimated_duration': duration,
                'actual_duration': duration,  # This is the real duration
                'detected_signal_freq': self.detect_signal_frequency(),
                'rate_source': 'header'
            }
        
        # Fallback: Try to detect the actual sample rate by looking at the signal frequency
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
        
        # Try to estimate based on file size and reasonable duration ranges
        # Start with the assumption that experiments typically run for 1-600 seconds
        estimated_rate = None
        
        # Check each common rate to see if it gives a reasonable duration
        for rate in common_rates:
            duration = n_samples / rate
            # Accept durations from 0.5 seconds to 10 minutes (600 seconds)
            if 0.5 <= duration <= 600:
                estimated_rate = rate
                break
        
        # If no reasonable rate found, use the highest rate as fallback
        if estimated_rate is None:
            estimated_rate = 1.25e6
            print(f"Warning: Could not determine sample rate from {n_samples:,} samples")
            print(f"Assuming {estimated_rate:,.0f} Hz (may be inaccurate)")
        
        duration = n_samples / estimated_rate
        
        return {
            'n_samples': n_samples,
            'estimated_sample_rate': estimated_rate,
            'estimated_duration': duration,
            'detected_signal_freq': detected_freq,
            'rate_source': 'estimated'
        }
        
    def analyze_sampling(self, n_samples: int = 10000) -> Dict[str, Any]:
        """Analyze the sampling characteristics of the data."""
        data = self.read_data(n_samples=min(n_samples, 100000))
        
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
        
    def plot_time_series(self, n_samples: Optional[int] = None, start_sample: int = 0):
        """Plot time series data."""
        # If n_samples is None, plot all data
        if n_samples is None:
            info = self.get_info()
            n_samples = info['n_samples'] - start_sample
        
        data = self.read_data(start_sample=start_sample, n_samples=n_samples)
        
        n_channels = len(self.header['channel_names'])
        fig, axes = plt.subplots(n_channels, 1, figsize=(12, 3*n_channels), sharex=True)
        
        if n_channels == 1:
            axes = [axes]
        
        # Get actual sample rate from header or estimate
        duration_info = self.estimate_experiment_duration()
        if 'actual_sample_rate' in duration_info:
            sample_rate = duration_info['actual_sample_rate']
        else:
            sample_rate = duration_info['estimated_sample_rate']
        
        # Create proper time axis
        time_seconds = np.arange(n_samples) / sample_rate
        
        for i, (ax, name) in enumerate(zip(axes, self.header['channel_names'])):
            ax.plot(time_seconds, data[:, i], linewidth=0.5)
            ax.set_ylabel(f'{name}\n({self._get_units(name)})')
            ax.grid(True, alpha=0.3)
            ax.set_xlim(time_seconds[0], time_seconds[-1])
            
            # Add statistics to plot
            mean_val = np.mean(data[:, i])
            std_val = np.std(data[:, i])
            ax.axhline(mean_val, color='red', linestyle='--', alpha=0.5, label=f'Mean: {mean_val:.3e}')
            ax.fill_between(time_seconds, mean_val-std_val, mean_val+std_val, alpha=0.2, color='red')
            ax.legend(loc='upper right')
            
        axes[-1].set_xlabel('Time (seconds)')
        plt.suptitle(f'SR860 Time Series - {Path(self.filename).name}\n'
                    f'Sample Rate: {sample_rate:,.0f} Hz, Duration: {time_seconds[-1]:.1f}s')
        plt.tight_layout()
        return fig
        
    def plot_spectrum(self, n_samples: int = 65536, start_sample: int = 0, 
                     sample_rate: Optional[float] = None):
        """Plot frequency spectrum using FFT."""
        data = self.read_data(start_sample=start_sample, n_samples=n_samples)
        
        # Use power of 2 for FFT efficiency
        n_fft = min(n_samples, 2**int(np.log2(n_samples)))
        
        # Get actual sample rate if not provided
        if sample_rate is None:
            duration_info = self.estimate_experiment_duration()
            if 'actual_sample_rate' in duration_info:
                sample_rate = duration_info['actual_sample_rate']
            else:
                sample_rate = duration_info['estimated_sample_rate']
        
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
        plt.suptitle(f'SR860 Frequency Spectrum - {Path(self.filename).name}\n'
                    f'Sample Rate: {sample_rate:,.0f} Hz, FFT Size: {n_fft:,}')
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
                     delimiter: str = ',', include_computed: bool = True):
        """Export data to CSV file with optional R and Theta computation.
        
        Args:
            output_file: Output CSV filename
            n_samples: Number of samples to export (None = all)
            delimiter: CSV delimiter
            include_computed: If True and data has X,Y channels, compute R and Theta
        """
        data = self.read_data(n_samples=n_samples)
        
        # Create reader_output directory if it doesn't exist
        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_dir = Path("reader_output")
            output_dir.mkdir(exist_ok=True)
            output_path = output_dir / output_file
        
        # Prepare headers
        headers = ['Sample', 'Time_ms'] + self.header['channel_names']
        
        # Check if we can compute R and Theta
        has_xy = False
        x_idx = y_idx = None
        if 'X' in self.header['channel_names'] and 'Y' in self.header['channel_names']:
            has_xy = True
            x_idx = self.header['channel_names'].index('X')
            y_idx = self.header['channel_names'].index('Y')
            
            # Add computed columns if requested and not already present
            if include_computed:
                if 'R' not in self.header['channel_names']:
                    headers.append('R_computed')
                if 'Theta' not in self.header['channel_names']:
                    headers.append('Theta_deg_computed')
        
        # Estimate sample rate for time column
        estimated_rate = self.estimate_experiment_duration()['estimated_sample_rate']
        time_step_ms = 1000.0 / estimated_rate  # Time step in milliseconds
        
        # Save to CSV
        with open(output_path, 'w') as f:
            # Write header
            f.write(delimiter.join(headers) + '\n')
            
            # Write data
            for i, row in enumerate(data):
                # Sample index and time
                line_parts = [str(i), f"{i * time_step_ms:.6f}"]
                
                # Original channel data - handle NaN/inf values
                for val in row:
                    if np.isfinite(val):
                        line_parts.append(f"{val:.9e}")
                    else:
                        line_parts.append("NaN")
                
                # Compute R and Theta if applicable
                if has_xy and include_computed:
                    x_val = row[x_idx]
                    y_val = row[y_idx]
                    
                    if np.isfinite(x_val) and np.isfinite(y_val):
                        # R = sqrt(X^2 + Y^2)
                        r_computed = np.sqrt(x_val**2 + y_val**2)
                        
                        # Theta = atan2(Y, X) in degrees
                        theta_computed = np.degrees(np.arctan2(y_val, x_val))
                        
                        if 'R' not in self.header['channel_names']:
                            line_parts.append(f"{r_computed:.9e}")
                        if 'Theta' not in self.header['channel_names']:
                            line_parts.append(f"{theta_computed:.6f}")
                    else:
                        if 'R' not in self.header['channel_names']:
                            line_parts.append("NaN")
                        if 'Theta' not in self.header['channel_names']:
                            line_parts.append("NaN")
                
                f.write(delimiter.join(line_parts) + '\n')
                
        print(f"Exported {len(data)} samples to {output_path}")
        if has_xy and include_computed:
            print("  Added computed R and Theta columns from X,Y data")
            print("  Note: Y is treated as the imaginary component (X + jY)")
            
    def diagnose_data_issues(self, n_samples: int = 10000) -> Dict[str, Any]:
        """Diagnose potential data format issues."""
        if not self.header:
            self.read_header()
            
        diagnosis = {
            'header_byte_order': self.header.get('byte_order', 'unknown'),
            'format': 'float32' if self.header['format'] == 0 else 'int16',
            'channels': self.header['channel_names']
        }
        
        # Try reading with different byte orders
        bytes_per_sample = self.header['bytes_per_point'] * self.header['points_per_sample']
        
        with open(self.filename, 'rb') as f:
            f.seek(self.header['data_offset'])
            test_bytes = f.read(min(n_samples * bytes_per_sample, 1000 * bytes_per_sample))
        
        # Test different interpretations
        if self.header['format'] == 0:  # float32
            # Little-endian
            data_le = np.frombuffer(test_bytes, dtype='<f4')
            invalid_le = np.sum(~np.isfinite(data_le))
            
            # Big-endian
            data_be = np.frombuffer(test_bytes, dtype='>f4')
            invalid_be = np.sum(~np.isfinite(data_be))
            
            diagnosis['little_endian_invalid'] = invalid_le
            diagnosis['big_endian_invalid'] = invalid_be
            diagnosis['little_endian_invalid_pct'] = 100 * invalid_le / len(data_le)
            diagnosis['big_endian_invalid_pct'] = 100 * invalid_be / len(data_be)
            
            # Check value ranges
            finite_le = data_le[np.isfinite(data_le)]
            finite_be = data_be[np.isfinite(data_be)]
            
            if len(finite_le) > 0:
                diagnosis['little_endian_range'] = (float(np.min(finite_le)), float(np.max(finite_le)))
                diagnosis['little_endian_typical'] = float(np.median(np.abs(finite_le)))
            
            if len(finite_be) > 0:
                diagnosis['big_endian_range'] = (float(np.min(finite_be)), float(np.max(finite_be)))
                diagnosis['big_endian_typical'] = float(np.median(np.abs(finite_be)))
            
            # Recommend byte order
            if invalid_le < invalid_be:
                diagnosis['recommended_byte_order'] = 'little-endian'
            else:
                diagnosis['recommended_byte_order'] = 'big-endian'
        
        return diagnosis
        
    def force_byte_order_and_format(self, byte_order: str = '<', force_format: Optional[str] = None):
        """Force specific byte order and format for data reading.
        
        Args:
            byte_order: '<' for little-endian, '>' for big-endian
            force_format: 'float32' or 'int16' to override header format
        """
        if not self.header:
            self.read_header()
            
        self.header['byte_order'] = byte_order
        if force_format:
            if force_format == 'float32':
                self.header['format'] = 0
                self.header['bytes_per_point'] = 4
            elif force_format == 'int16':
                self.header['format'] = 1
                self.header['bytes_per_point'] = 2
                
        print(f"Forced byte order: {byte_order}, format: {force_format or 'from header'}")
        
    def plot_xy_data(self, n_samples: int = 10000, start_sample: int = 0,
                     plot_type: str = 'both'):
        """Plot X-Y data in various representations.
        
        Args:
            n_samples: Number of samples to plot
            start_sample: Starting sample index
            plot_type: 'parametric', 'polar', or 'both'
        """
        if not self.header:
            self.read_header()
            
        # Check if we have X and Y data
        if 'X' not in self.header['channel_names'] or 'Y' not in self.header['channel_names']:
            print("Error: X and Y channels not found in data")
            return None
            
        # Read data
        data = self.read_data(start_sample=start_sample, n_samples=n_samples)
        
        # Get X and Y indices
        x_idx = self.header['channel_names'].index('X')
        y_idx = self.header['channel_names'].index('Y')
        
        x_data = data[:, x_idx]
        y_data = data[:, y_idx]
        
        # Check for extreme values and clip for plotting
        max_plot_val = 1e6  # Reasonable plotting range
        x_plot = np.clip(x_data, -max_plot_val, max_plot_val)
        y_plot = np.clip(y_data, -max_plot_val, max_plot_val)
        
        # Compute R and Theta (use clipped values for stability)
        r_data = np.sqrt(x_plot**2 + y_plot**2)
        theta_data_rad = np.arctan2(y_plot, x_plot)
        theta_data_deg = np.degrees(theta_data_rad)
        
        # Create appropriate plot(s)
        if plot_type == 'both':
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        else:
            fig, ax = plt.subplots(1, 1, figsize=(8, 8))
            ax1 = ax2 = ax
            
        # Parametric X-Y plot
        if plot_type in ['parametric', 'both']:
            ax = ax1 if plot_type == 'both' else ax1
            
            # Plot trajectory with color gradient showing time evolution
            points = ax.scatter(x_plot, y_plot, c=np.arange(len(x_plot)), 
                               cmap='viridis', alpha=0.6, s=1)
            
            # Mark start and end points
            ax.plot(x_plot[0], y_plot[0], 'go', markersize=10, label='Start')
            ax.plot(x_plot[-1], y_plot[-1], 'ro', markersize=10, label='End')
            
            ax.set_xlabel('X (Real)')
            ax.set_ylabel('Y (Imaginary)')
            ax.set_title('X-Y Parametric Plot (Complex Plane)')
            ax.grid(True, alpha=0.3)
            ax.axis('equal')
            ax.legend()
            
            # Add colorbar to show time progression
            if plot_type == 'both':
                plt.colorbar(points, ax=ax, label='Sample Index')
                
        # Polar plot
        if plot_type in ['polar', 'both']:
            ax = ax2 if plot_type == 'both' else ax1
            
            # Create polar subplot
            if plot_type != 'both':
                plt.clf()
                ax = plt.subplot(111, projection='polar')
            else:
                # Convert to polar axes
                ax.remove()
                ax = fig.add_subplot(122, projection='polar')
                
            # Plot in polar coordinates
            scatter = ax.scatter(theta_data_rad, r_data, c=np.arange(len(r_data)), 
                                cmap='viridis', alpha=0.6, s=1)
            
            # Mark start and end
            ax.plot(theta_data_rad[0], r_data[0], 'go', markersize=10, label='Start')
            ax.plot(theta_data_rad[-1], r_data[-1], 'ro', markersize=10, label='End')
            
            ax.set_title('Polar Plot (R, θ)')
            ax.legend(loc='upper right', bbox_to_anchor=(1.1, 1.1))
            
        plt.suptitle(f'SR860 X-Y Data Visualization - {Path(self.filename).name}\n'
                    f'{n_samples} samples, Mean R: {np.mean(r_data):.3e}')
        plt.tight_layout()
        
        return fig
        
    def plot_complex_analysis(self, n_samples: int = 10000, start_sample: int = 0):
        """Create comprehensive complex signal analysis plots."""
        if not self.header:
            self.read_header()
            
        # Check if we have X and Y data
        if 'X' not in self.header['channel_names'] or 'Y' not in self.header['channel_names']:
            print("Error: X and Y channels not found in data")
            return None
            
        # Read data
        data = self.read_data(start_sample=start_sample, n_samples=n_samples)
        
        # Get X and Y indices
        x_idx = self.header['channel_names'].index('X')
        y_idx = self.header['channel_names'].index('Y')
        
        x_data = data[:, x_idx]
        y_data = data[:, y_idx]
        
        # Check for extreme values and clip for plotting
        max_plot_val = 1e6  # Reasonable plotting range
        x_plot = np.clip(x_data, -max_plot_val, max_plot_val)
        y_plot = np.clip(y_data, -max_plot_val, max_plot_val)
        
        # Compute R and Theta (use clipped values for stability)
        r_data = np.sqrt(x_plot**2 + y_plot**2)
        theta_data_rad = np.arctan2(y_plot, x_plot)
        theta_data_deg = np.degrees(theta_data_rad)
        
        # Create time axis
        estimated_rate = self.estimate_experiment_duration()['estimated_sample_rate']
        time_ms = np.arange(n_samples) * 1000.0 / estimated_rate
        
        # Create figure with subplots
        fig = plt.figure(figsize=(15, 10))
        
        # 1. X and Y time series
        ax1 = plt.subplot(3, 2, 1)
        ax1.plot(time_ms, x_plot, 'b-', linewidth=0.5, label='X (Real)')
        ax1.plot(time_ms, y_plot, 'r-', linewidth=0.5, label='Y (Imaginary)')
        ax1.set_xlabel('Time (ms)')
        ax1.set_ylabel('Amplitude')
        ax1.set_title('X and Y Components')
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        # 2. R (magnitude) time series
        ax2 = plt.subplot(3, 2, 2)
        ax2.plot(time_ms, r_data, 'g-', linewidth=0.5)
        ax2.set_xlabel('Time (ms)')
        ax2.set_ylabel('R (Magnitude)')
        ax2.set_title(f'Magnitude: Mean={np.mean(r_data):.3e}, Std={np.std(r_data):.3e}')
        ax2.grid(True, alpha=0.3)
        
        # 3. Theta (phase) time series
        ax3 = plt.subplot(3, 2, 3)
        ax3.plot(time_ms, theta_data_deg, 'purple', linewidth=0.5)
        ax3.set_xlabel('Time (ms)')
        ax3.set_ylabel('θ (degrees)')
        ax3.set_title('Phase Angle')
        ax3.grid(True, alpha=0.3)
        ax3.set_ylim(-180, 180)
        
        # 4. X-Y parametric plot
        ax4 = plt.subplot(3, 2, 4)
        scatter = ax4.scatter(x_plot[::10], y_plot[::10], c=time_ms[::10], 
                             cmap='viridis', alpha=0.5, s=1)
        ax4.set_xlabel('X (Real)')
        ax4.set_ylabel('Y (Imaginary)')
        ax4.set_title('X-Y Parametric Plot')
        ax4.grid(True, alpha=0.3)
        ax4.axis('equal')
        plt.colorbar(scatter, ax=ax4, label='Time (ms)')
        
        # 5. Histogram of R values
        ax5 = plt.subplot(3, 2, 5)
        try:
            # Filter out extreme values for histogram
            r_filtered = r_data[np.isfinite(r_data) & (r_data < max_plot_val)]
            if len(r_filtered) > 0:
                ax5.hist(r_filtered, bins=100, alpha=0.7, edgecolor='black')
            else:
                ax5.text(0.5, 0.5, 'No valid R values for histogram', 
                        transform=ax5.transAxes, ha='center', va='center')
        except Exception as e:
            ax5.text(0.5, 0.5, f'Error plotting histogram: {str(e)}', 
                    transform=ax5.transAxes, ha='center', va='center')
        ax5.set_xlabel('R (Magnitude)')
        ax5.set_ylabel('Count')
        ax5.set_title('Magnitude Distribution')
        ax5.grid(True, alpha=0.3)
        
        # 6. Histogram of Theta values
        ax6 = plt.subplot(3, 2, 6)
        try:
            theta_filtered = theta_data_deg[np.isfinite(theta_data_deg)]
            if len(theta_filtered) > 0:
                ax6.hist(theta_filtered, bins=100, alpha=0.7, edgecolor='black', color='purple')
            else:
                ax6.text(0.5, 0.5, 'No valid Theta values for histogram', 
                        transform=ax6.transAxes, ha='center', va='center')
        except Exception as e:
            ax6.text(0.5, 0.5, f'Error plotting histogram: {str(e)}', 
                    transform=ax6.transAxes, ha='center', va='center')
        ax6.set_xlabel('θ (degrees)')
        ax6.set_ylabel('Count')
        ax6.set_title('Phase Distribution')
        ax6.grid(True, alpha=0.3)
        ax6.set_xlim(-180, 180)
        
        plt.suptitle(f'SR860 Complex Signal Analysis - {Path(self.filename).name}')
        plt.tight_layout()
        
        return fig

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
            try:
                # Check if data has sufficient range
                data_range = np.ptp(channel_data)
                if data_range == 0:
                    ax1.text(0.5, 0.5, f'Constant value: {channel_data[0]:.3e}', 
                            transform=ax1.transAxes, ha='center', va='center')
                else:
                    # Use fewer bins if data range is small
                    n_bins = min(100, max(10, int(np.sqrt(len(channel_data)))))
                    ax1.hist(channel_data, bins=n_bins, alpha=0.7, edgecolor='black')
            except Exception as e:
                ax1.text(0.5, 0.5, f'Error: {str(e)}', 
                        transform=ax1.transAxes, ha='center', va='center')
            ax1.set_xlabel(f'{name} Value')
            ax1.set_ylabel('Count')
            ax1.set_title(f'{name} Distribution')
            ax1.grid(True, alpha=0.3)
            
            # 2. First differences histogram
            ax2 = axes[i, 1]
            diff = np.diff(channel_data)
            try:
                diff_range = np.ptp(diff)
                if diff_range == 0:
                    ax2.text(0.5, 0.5, 'No variation in differences', 
                            transform=ax2.transAxes, ha='center', va='center')
                else:
                    n_bins = min(100, max(10, int(np.sqrt(len(diff)))))
                    ax2.hist(diff, bins=n_bins, alpha=0.7, edgecolor='black', color='orange')
            except Exception as e:
                ax2.text(0.5, 0.5, f'Error: {str(e)}', 
                        transform=ax2.transAxes, ha='center', va='center')
            ax2.set_xlabel(f'Δ{name}')
            ax2.set_ylabel('Count')
            ax2.set_title(f'{name} First Differences')
            ax2.grid(True, alpha=0.3)
            
            # Add text showing duplicate ratio
            n_zeros = np.sum(diff == 0)
            zero_ratio = n_zeros / len(diff) * 100 if len(diff) > 0 else 0
            ax2.text(0.95, 0.95, f'Duplicates: {zero_ratio:.1f}%', 
                    transform=ax2.transAxes, ha='right', va='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            # 3. Autocorrelation
            ax3 = axes[i, 2]
            try:
                # Check if data has variation
                if np.std(channel_data) == 0:
                    ax3.text(0.5, 0.5, 'No variation for autocorrelation', 
                            transform=ax3.transAxes, ha='center', va='center')
                else:
                    # Compute autocorrelation for first 1000 lags
                    n_lags = min(1000, len(channel_data)//2)
                    # Normalize the data
                    normalized_data = channel_data - np.mean(channel_data)
                    # Use numpy's correlate for autocorrelation
                    autocorr = np.correlate(normalized_data, normalized_data, mode='full')
                    autocorr = autocorr[len(autocorr)//2:len(autocorr)//2 + n_lags]
                    if autocorr[0] != 0:
                        autocorr = autocorr / autocorr[0]  # Normalize
                    
                    ax3.plot(autocorr[:100])  # Show first 100 lags
            except Exception as e:
                ax3.text(0.5, 0.5, f'Error: {str(e)}', 
                        transform=ax3.transAxes, ha='center', va='center')
            ax3.set_xlabel('Lag')
            ax3.set_ylabel('Autocorrelation')
            ax3.set_title(f'{name} Autocorrelation')
            ax3.grid(True, alpha=0.3)
            ax3.axhline(0, color='black', linewidth=0.5)
            
        plt.suptitle(f'Sampling Analysis - {Path(self.filename).name}')
        plt.tight_layout()
        return fig

    def save_plots(self, base_filename: Optional[str] = None, sample_rate: float = 1e6,
                   force_byte_order: Optional[str] = None, force_format: Optional[str] = None):
        """Save all analysis plots to the reader_output folder."""
        # Create reader_output directory if it doesn't exist
        output_dir = Path("reader_output")
        output_dir.mkdir(exist_ok=True)
        
        # Apply forced settings if specified
        if force_byte_order or force_format:
            byte_order = '<' if force_byte_order == 'little' else '>' if force_byte_order == 'big' else '<'
            self.force_byte_order_and_format(byte_order, force_format)
        
        # Use input filename as base if not specified
        if base_filename is None:
            base_filename = Path(self.filename).stem
        
        info = self.get_info()
        
        try:
            # Time series plot
            fig1 = self.plot_time_series(n_samples=min(10000, info['n_samples']))
            fig1.savefig(output_dir / f"{base_filename}_time_series.png", dpi=150, bbox_inches='tight')
            plt.close(fig1)
        except Exception as e:
            print(f"Warning: Could not save time series plot: {e}")
        
        # Frequency spectrum plot
        if info['n_samples'] >= 1024:
            try:
                fig2 = self.plot_spectrum(sample_rate=sample_rate)
                fig2.savefig(output_dir / f"{base_filename}_spectrum.png", dpi=150, bbox_inches='tight')
                plt.close(fig2)
            except Exception as e:
                print(f"Warning: Could not save spectrum plot: {e}")
        
        # Sampling analysis plot
        try:
            fig3 = self.plot_sampling_analysis(n_samples=min(10000, info['n_samples']))
            fig3.savefig(output_dir / f"{base_filename}_sampling_analysis.png", dpi=150, bbox_inches='tight')
            plt.close(fig3)
        except Exception as e:
            print(f"Warning: Could not save sampling analysis plot: {e}")
        
        # X-Y plots if we have X and Y channels
        if 'X' in self.header['channel_names'] and 'Y' in self.header['channel_names']:
            # X-Y parametric and polar plots
            try:
                fig4 = self.plot_xy_data(n_samples=min(10000, info['n_samples']))
                if fig4:
                    fig4.savefig(output_dir / f"{base_filename}_xy_plots.png", dpi=150, bbox_inches='tight')
                    plt.close(fig4)
            except Exception as e:
                print(f"Warning: Could not save X-Y plots: {e}")
            
            # Complex analysis plots
            try:
                fig5 = self.plot_complex_analysis(n_samples=min(10000, info['n_samples']))
                if fig5:
                    fig5.savefig(output_dir / f"{base_filename}_complex_analysis.png", dpi=150, bbox_inches='tight')
                    plt.close(fig5)
            except Exception as e:
                print(f"Warning: Could not save complex analysis plot: {e}")
        
        print(f"Saved plots to {output_dir}/")


def analyze_file(filename: str, plot: bool = True, sample_rate: Optional[float] = None,
                force_byte_order: Optional[str] = None, force_format: Optional[str] = None):
    """Comprehensive analysis of SR860 binary file."""
    
    reader = SR860BinaryReader(filename)
    
    # Apply forced settings if specified
    if force_byte_order or force_format:
        byte_order = '<' if force_byte_order == 'little' else '>' if force_byte_order == 'big' else '<'
        reader.force_byte_order_and_format(byte_order, force_format)
    
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
    
    if 'actual_sample_rate' in duration_info:
        print(f"  Actual sample rate: {duration_info['actual_sample_rate']:,.0f} Hz (from header)")
        print(f"  Actual duration: {duration_info['actual_duration']:.3f} seconds")
        # Use actual sample rate for plotting
        if sample_rate is None:
            sample_rate = duration_info['actual_sample_rate']
    else:
        print(f"  Estimated sample rate: {duration_info['estimated_sample_rate']:,.0f} Hz (estimated)")
        print(f"  Estimated duration: {duration_info['estimated_duration']:.3f} seconds")
        print(f"  Note: Rate was estimated - may not be accurate")
        # Use estimated sample rate for plotting
        if sample_rate is None:
            sample_rate = duration_info['estimated_sample_rate']
    
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
        
        # Time series plot - show full experiment duration
        reader.plot_time_series()
        
        # Frequency spectrum plot
        if info['n_samples'] >= 1024:
            reader.plot_spectrum(sample_rate=sample_rate)
        
        # Sampling analysis plot (limited to first 10k samples for performance)
        reader.plot_sampling_analysis(n_samples=min(10000, info['n_samples']))
        
        # X-Y plots if available (limited to first 10k samples for performance)
        if 'X' in reader.header['channel_names'] and 'Y' in reader.header['channel_names']:
            print("\nGenerating X-Y plots...")
            reader.plot_xy_data(n_samples=min(10000, info['n_samples']))
            reader.plot_complex_analysis(n_samples=min(10000, info['n_samples']))
        
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
    parser.add_argument("--sample-rate", type=float,
                       help="Override sample rate in Hz (default: use rate from header)")
    parser.add_argument("--xy-plot", choices=['parametric', 'polar', 'both'],
                       help="Generate specific X-Y plot type")
    parser.add_argument("--no-computed", action="store_true",
                       help="Don't add computed R and Theta columns to CSV")
    parser.add_argument("--force-byte-order", choices=['little', 'big'],
                       help="Force specific byte order for data interpretation")
    parser.add_argument("--force-format", choices=['float32', 'int16'],
                       help="Force specific data format (override header)")
    
    args = parser.parse_args()
    
    # Check file exists
    if not Path(args.filename).exists():
        print(f"Error: File '{args.filename}' not found")
        return
    
    # Analyze file
    analyze_file(args.filename, plot=not args.no_plot and not args.save_plots, sample_rate=args.sample_rate,
                force_byte_order=args.force_byte_order, force_format=args.force_format)
    
    # Save plots if requested
    if args.save_plots:
        reader = SR860BinaryReader(args.filename)
        reader.save_plots(sample_rate=args.sample_rate,
                         force_byte_order=args.force_byte_order,
                         force_format=args.force_format)
    
    # Export to CSV if requested
    if args.export_csv:
        reader = SR860BinaryReader(args.filename)
        
        # Apply forced settings if specified
        if args.force_byte_order or args.force_format:
            byte_order = '<' if args.force_byte_order == 'little' else '>' if args.force_byte_order == 'big' else '<'
            reader.force_byte_order_and_format(byte_order, args.force_format)
            
        reader.export_to_csv(args.export_csv, n_samples=args.n_samples, 
                           include_computed=not args.no_computed)
        
    # Show specific X-Y plot if requested
    if args.xy_plot:
        reader = SR860BinaryReader(args.filename)
        reader.read_header()
        
        # Apply forced settings if specified
        if args.force_byte_order or args.force_format:
            byte_order = '<' if args.force_byte_order == 'little' else '>' if args.force_byte_order == 'big' else '<'
            reader.force_byte_order_and_format(byte_order, args.force_format)
            
        if 'X' in reader.header['channel_names'] and 'Y' in reader.header['channel_names']:
            fig = reader.plot_xy_data(n_samples=args.n_samples or 10000, plot_type=args.xy_plot)
            if fig:
                plt.show()
        else:
            print("Error: X and Y channels not found in data")


if __name__ == "__main__":
    main() 