#!/usr/bin/env python3
"""
Visualization script for tables_throughput_saturation.csv

Generates a 5-panel dashboard showing:
1. Throughput vs clients
2. Latency percentiles
3. Scaling efficiency
4. Latency spread (P50 to P99 range)
5. Summary statistics

Usage:
    python plot_tables_saturation.py [csv_file]

Default: tables_throughput_saturation.csv
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path


def load_data(csv_file='tables_throughput_saturation.csv'):
    """Load and validate CSV data."""
    if not Path(csv_file).exists():
        print(f"Error: CSV file '{csv_file}' not found.")
        print("Run the stress test first:")
        print("  cargo run --example tables_stress_throughput_saturation")
        sys.exit(1)

    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} data points from {csv_file}")
    print(f"Client range: {df['concurrent_clients'].min()} to {df['concurrent_clients'].max()}")
    return df


def analyze_scaling(df):
    """Analyze scaling behavior."""
    df = df.copy()
    df['throughput_per_client'] = df['throughput'] / df['concurrent_clients']

    mean_tpc = df['throughput_per_client'].mean()
    std_tpc = df['throughput_per_client'].std()
    cv = (std_tpc / mean_tpc) * 100 if mean_tpc > 0 else 0

    print(f"\nScaling Analysis:")
    print(f"  Mean throughput/client: {mean_tpc:.2f} ops/sec/client")
    print(f"  Std deviation: {std_tpc:.2f}")
    print(f"  Coefficient of variation: {cv:.1f}%")

    return mean_tpc, std_tpc, cv


def analyze_latency(df):
    """Analyze latency characteristics."""
    print(f"\nLatency Analysis:")
    print(f"  P50 range: {df['latency_p50_ms'].min():.0f} - {df['latency_p50_ms'].max():.0f} ms")
    print(f"  P95 range: {df['latency_p95_ms'].min():.0f} - {df['latency_p95_ms'].max():.0f} ms")
    print(f"  P99 range: {df['latency_p99_ms'].min():.0f} - {df['latency_p99_ms'].max():.0f} ms")


def create_visualizations(df, output_file='tables_saturation_analysis.png'):
    """Create comprehensive visualization dashboard."""
    fig = plt.figure(figsize=(14, 10))
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

    clients = df['concurrent_clients']

    # Plot 1: Throughput vs Concurrent Clients
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(clients, df['throughput'], 'b-o', linewidth=2, markersize=6)
    ax1.set_xlabel('Concurrent Clients', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Throughput (ops/sec)', fontsize=11, fontweight='bold')
    ax1.set_title('Throughput vs Concurrent Clients', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)

    # Plot 2: Latency Percentiles (lines only, no fill)
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.plot(clients, df['latency_p50_ms'], 'g-o', label='P50', linewidth=2, markersize=5)
    ax2.plot(clients, df['latency_p95_ms'], 'orange', marker='s', label='P95', linewidth=2, markersize=5)
    ax2.plot(clients, df['latency_p99_ms'], 'r-^', label='P99', linewidth=2, markersize=5)
    ax2.set_xlabel('Concurrent Clients', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Latency (ms)', fontsize=11, fontweight='bold')
    ax2.set_title('Latency Percentiles', fontsize=13, fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Plot 3: Scaling Efficiency (Throughput per Client) - no fill
    ax3 = fig.add_subplot(gs[1, 0])
    throughput_per_client = df['throughput'] / clients
    mean_tpc = throughput_per_client.mean()

    ax3.plot(clients, throughput_per_client, 'm-o', linewidth=2, markersize=6)
    ax3.axhline(y=mean_tpc, color='blue', linestyle='--', alpha=0.7,
                label=f'Mean: {mean_tpc:.2f}')
    ax3.set_xlabel('Concurrent Clients', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Throughput per Client (ops/sec)', fontsize=11, fontweight='bold')
    ax3.set_title('Scaling Efficiency', fontsize=13, fontweight='bold')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # Plot 4: Summary Statistics Table
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.axis('off')

    tpc = df['throughput'] / clients
    cv = (tpc.std() / tpc.mean()) * 100 if tpc.mean() > 0 else 0

    summary_data = [
        ['Metric', 'Value'],
        ['', ''],
        ['Client Range', f"{clients.min():.0f} - {clients.max():.0f}"],
        ['Peak Throughput', f"{df['throughput'].max():.1f} ops/s"],
        ['P50 Latency Range', f"{df['latency_p50_ms'].min():.0f} - {df['latency_p50_ms'].max():.0f} ms"],
        ['P99 Latency Range', f"{df['latency_p99_ms'].min():.0f} - {df['latency_p99_ms'].max():.0f} ms"],
        ['Throughput/Client', f"{tpc.mean():.2f} ops/s/client"],
        ['Scaling CV', f"{cv:.1f}%"],
    ]

    table = ax4.table(cellText=summary_data, cellLoc='left', loc='center',
                      colWidths=[0.6, 0.4])
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.2)

    for i in range(len(summary_data)):
        if i == 0:
            table[(i, 0)].set_facecolor('#4CAF50')
            table[(i, 1)].set_facecolor('#4CAF50')
            table[(i, 0)].set_text_props(weight='bold', color='white')
            table[(i, 1)].set_text_props(weight='bold', color='white')
        elif i == 1:
            table[(i, 0)].set_facecolor('#f0f0f0')
            table[(i, 1)].set_facecolor('#f0f0f0')

    ax4.set_title('Summary Statistics', fontsize=13, fontweight='bold', pad=20)

    plt.suptitle('S3 Tables Throughput Saturation Analysis', fontsize=16, fontweight='bold', y=0.98)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nVisualization saved to: {output_file}")

    plt.show()


def main():
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'tables_throughput_saturation.csv'

    print("=" * 70)
    print("S3 Tables Throughput Saturation Analysis")
    print("=" * 70)

    df = load_data(csv_file)

    analyze_scaling(df)
    analyze_latency(df)

    print("\n" + "=" * 70)
    print("Generating Visualizations...")
    print("=" * 70)
    create_visualizations(df)

    print("\n" + "=" * 70)
    print("Analysis Complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
