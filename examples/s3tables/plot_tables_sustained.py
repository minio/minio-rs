#!/usr/bin/env python3
"""
Visualization script for tables_sustained_load.csv

Generates a 6-panel time-series dashboard showing:
1. Throughput over time (with trend line)
2. Latency percentiles over time (with confidence band)
3. Error rate progression
4. Cumulative operations
5. Rolling average throughput
6. Summary statistics

Usage:
    python plot_tables_sustained.py [csv_file]

Default: tables_sustained_load.csv
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from pathlib import Path
from scipy import stats


def load_data(csv_file='tables_sustained_load.csv'):
    """Load and validate CSV data."""
    if not Path(csv_file).exists():
        print(f"Error: CSV file '{csv_file}' not found.")
        print("Run the stress test first:")
        print("  cargo run --example tables_stress_sustained_load")
        sys.exit(1)

    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} data points from {csv_file}")
    print(f"Test duration: {df['elapsed_secs'].max():.0f} seconds ({df['elapsed_secs'].max()/60:.1f} minutes)")
    return df


def analyze_throughput_stability(df):
    """Analyze throughput stability over time."""
    throughput = df['window_throughput']
    mean_throughput = throughput.mean()
    std_throughput = throughput.std()
    cv = (std_throughput / mean_throughput) * 100 if mean_throughput > 0 else 0

    # Linear regression for trend
    x = df['elapsed_secs'].values
    y = throughput.values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    print(f"\nThroughput Stability Analysis:")
    print(f"  Mean throughput: {mean_throughput:.2f} ops/sec")
    print(f"  Std deviation: {std_throughput:.2f} ops/sec")
    print(f"  Coefficient of variation: {cv:.1f}%")
    print(f"  Trend slope: {slope*60:.4f} ops/sec per minute")
    print(f"  R-squared: {r_value**2:.4f}")

    return cv, slope, r_value**2


def analyze_latency_trends(df):
    """Analyze latency trends over time."""
    p99 = df['latency_p99_ms']

    # Linear regression for P99 latency trend
    x = df['elapsed_secs'].values
    y = p99.values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    print(f"\nLatency Trend Analysis:")
    print(f"  Initial P99: {p99.iloc[0]:.0f}ms")
    print(f"  Final P99: {p99.iloc[-1]:.0f}ms")
    print(f"  Mean P99: {p99.mean():.0f}ms")
    print(f"  Max P99: {p99.max():.0f}ms")
    print(f"  Trend slope: {slope*60:.2f} ms/minute")

    return slope


def analyze_error_progression(df):
    """Analyze error rate progression."""
    error_rate = df['error_rate'] * 100

    print(f"\nError Rate Analysis:")
    print(f"  Mean error rate: {error_rate.mean():.2f}%")
    print(f"  Max error rate: {error_rate.max():.2f}%")
    print(f"  Final cumulative: {df['cumulative_error_rate'].iloc[-1]*100:.2f}%")


def create_visualizations(df, output_file='tables_sustained_load_analysis.png'):
    """Create comprehensive visualization dashboard."""
    fig = plt.figure(figsize=(16, 12))
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.3, wspace=0.3)

    # Convert elapsed_secs to minutes for readability
    df = df.copy()
    df['elapsed_mins'] = df['elapsed_secs'] / 60

    # Plot 1: Throughput over time with trend line
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(df['elapsed_mins'], df['window_throughput'], 'b-', linewidth=1, alpha=0.7,
             label='Window throughput')

    # Add trend line
    x = df['elapsed_secs'].values
    y = df['window_throughput'].values
    slope, intercept, _, _, _ = stats.linregress(x, y)
    trend_y = slope * x + intercept
    ax1.plot(df['elapsed_mins'], trend_y, 'r--', linewidth=2, label=f'Trend ({slope*60:.2f} ops/min)')

    ax1.set_xlabel('Elapsed Time (minutes)', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Throughput (ops/sec)', fontsize=11, fontweight='bold')
    ax1.set_title('Throughput Over Time', fontsize=13, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot 2: Latency percentiles over time with confidence band
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.fill_between(df['elapsed_mins'], df['latency_p50_ms'], df['latency_p99_ms'],
                     alpha=0.3, color='red', label='P50-P99 range')
    ax2.plot(df['elapsed_mins'], df['latency_p50_ms'], 'g-', label='P50', linewidth=1.5)
    ax2.plot(df['elapsed_mins'], df['latency_p95_ms'], 'orange', label='P95', linewidth=1.5)
    ax2.plot(df['elapsed_mins'], df['latency_p99_ms'], 'r-', label='P99', linewidth=1.5)
    ax2.set_xlabel('Elapsed Time (minutes)', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Latency (ms)', fontsize=11, fontweight='bold')
    ax2.set_title('Latency Percentiles Over Time', fontsize=13, fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Plot 3: Error rate over time
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.plot(df['elapsed_mins'], df['error_rate'] * 100, 'r-', linewidth=1.5)
    ax3.fill_between(df['elapsed_mins'], 0, df['error_rate'] * 100, alpha=0.3, color='red')
    ax3.set_xlabel('Elapsed Time (minutes)', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Error Rate (%)', fontsize=11, fontweight='bold')
    ax3.set_title('Error Rate Over Time', fontsize=13, fontweight='bold')
    ax3.grid(True, alpha=0.3)

    # Plot 4: Cumulative operations
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.plot(df['elapsed_mins'], df['cumulative_ops'], 'b-', linewidth=2)
    ax4.fill_between(df['elapsed_mins'], 0, df['cumulative_ops'], alpha=0.2, color='blue')
    ax4.set_xlabel('Elapsed Time (minutes)', fontsize=11, fontweight='bold')
    ax4.set_ylabel('Cumulative Operations', fontsize=11, fontweight='bold')
    ax4.set_title('Total Operations Over Time', fontsize=13, fontweight='bold')
    ax4.grid(True, alpha=0.3)

    # Format y-axis with K/M suffix
    ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:.0f}K' if x < 1e6 else f'{x/1e6:.1f}M'))

    # Plot 5: Rolling average throughput (noise reduction)
    ax5 = fig.add_subplot(gs[2, 0])
    window_size = min(5, len(df))
    rolling_throughput = df['window_throughput'].rolling(window=window_size, center=True).mean()
    std_rolling = df['window_throughput'].rolling(window=window_size, center=True).std()

    ax5.plot(df['elapsed_mins'], df['window_throughput'], 'b-', alpha=0.3, linewidth=1,
             label='Raw')
    ax5.plot(df['elapsed_mins'], rolling_throughput, 'b-', linewidth=2,
             label=f'Rolling avg ({window_size} samples)')
    ax5.fill_between(df['elapsed_mins'],
                     rolling_throughput - std_rolling,
                     rolling_throughput + std_rolling,
                     alpha=0.2, color='blue', label='+/- 1 std dev')
    ax5.set_xlabel('Elapsed Time (minutes)', fontsize=11, fontweight='bold')
    ax5.set_ylabel('Throughput (ops/sec)', fontsize=11, fontweight='bold')
    ax5.set_title('Smoothed Throughput with Confidence Band', fontsize=13, fontweight='bold')
    ax5.legend()
    ax5.grid(True, alpha=0.3)

    # Plot 6: Summary statistics
    ax6 = fig.add_subplot(gs[2, 1])
    ax6.axis('off')

    total_ops = df['cumulative_ops'].iloc[-1]
    total_time = df['elapsed_secs'].iloc[-1]
    avg_throughput = total_ops / total_time if total_time > 0 else 0
    cv = (df['window_throughput'].std() / df['window_throughput'].mean() * 100) if df['window_throughput'].mean() > 0 else 0

    summary_data = [
        ['Metric', 'Value'],
        ['', ''],
        ['Test Duration', f"{total_time/60:.1f} minutes"],
        ['Total Operations', f"{total_ops:,.0f}"],
        ['Avg Throughput', f"{avg_throughput:.1f} ops/sec"],
        ['Throughput CV', f"{cv:.1f}%"],
        ['Mean P99 Latency', f"{df['latency_p99_ms'].mean():.0f} ms"],
        ['Max P99 Latency', f"{df['latency_p99_ms'].max():.0f} ms"],
        ['Final Error Rate', f"{df['cumulative_error_rate'].iloc[-1]*100:.2f}%"],
    ]

    table = ax6.table(cellText=summary_data, cellLoc='left', loc='center',
                      colWidths=[0.6, 0.4])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)

    for i in range(len(summary_data)):
        if i == 0:
            table[(i, 0)].set_facecolor('#4CAF50')
            table[(i, 1)].set_facecolor('#4CAF50')
            table[(i, 0)].set_text_props(weight='bold', color='white')
            table[(i, 1)].set_text_props(weight='bold', color='white')
        elif i == 1:
            table[(i, 0)].set_facecolor('#f0f0f0')
            table[(i, 1)].set_facecolor('#f0f0f0')

    ax6.set_title('Summary Statistics', fontsize=13, fontweight='bold', pad=20)

    plt.suptitle('S3 Tables Sustained Load Analysis', fontsize=16, fontweight='bold', y=0.98)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nVisualization saved to: {output_file}")

    plt.show()


def main():
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'tables_sustained_load.csv'

    print("=" * 70)
    print("S3 Tables Sustained Load Analysis")
    print("=" * 70)

    df = load_data(csv_file)

    analyze_throughput_stability(df)
    analyze_latency_trends(df)
    analyze_error_progression(df)

    print("\n" + "=" * 70)
    print("Generating Visualizations...")
    print("=" * 70)
    create_visualizations(df)

    print("\n" + "=" * 70)
    print("Analysis Complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
