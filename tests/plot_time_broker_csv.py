import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os


# ==========================================
# 2. DATA PROCESSING
# ==========================================

csv_file_path = '/Users/gsampaio/Programming/PyCharm/asense_api_local/tests/csvs/ASENSE00000022_asense_table_acc_1764684000000_1764770400000_FULL_DATA.csv'

print(f"Loading {csv_file_path}...")
df = pd.read_csv(csv_file_path)

# Calculate Latency in Seconds (as requested)
# (Broker - Sensor) / 1000
df['latency_s'] = (df['time_broker'] - df['time']) / 1000.0

# Readable Date for X-Axis
df['datetime_readable'] = pd.to_datetime(df['time'], unit='ms')

# Stats (in Seconds)
mean_lat = df['latency_s'].mean()
p99_lat = df['latency_s'].quantile(0.99)

print(f"Stats: Mean={mean_lat:.4f}s, 99th%={p99_lat:.4f}s")

# ==========================================
# 3. VISUALIZATION
# ==========================================
print("Generating visualization...")

# Layout: Top chart is bigger (50%), bottom two are smaller
fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.06,
    subplot_titles=(
        f"Latency over Time (Outliers > {p99_lat:.3f}s)",
        "Network Jitter (Moving Average)",
        "Latency Distribution"
    ),
    row_heights=[0.5, 0.25, 0.25],
    specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"type": "histogram"}]]
)

# --- CHART 1: Scatter (Latency vs Time) ---
fig.add_trace(
    go.Scattergl(
        x=df['datetime_readable'],
        y=df['latency_s'],
        mode='markers',
        marker=dict(
            size=3,
            color=df['latency_s'],
            colorscale='Turbo',  # Turbo is great for highlighting outliers
            showscale=True,
            # --- COLORBAR FIX ---
            colorbar=dict(
                title="Lat (s)",
                len=0.5,  # Length relative to plot height (0.5 matches row height approx)
                y=1,  # Position at the very top
                yanchor="top",  # Anchor point
                x=1.02,  # Slight offset to the right
            )
        ),
        name='Packet Latency',
        hovertemplate='<b>Time:</b> %{x}<br><b>Latency:</b> %{y:.4f}s<extra></extra>'
    ),
    row=1, col=1
)

# --- CHART 2: Moving Average ---
df['rolling_avg'] = df['latency_s'].rolling(window=100).mean()

fig.add_trace(
    go.Scattergl(
        x=df['datetime_readable'],
        y=df['rolling_avg'],
        mode='lines',
        line=dict(color='#00F0FF', width=1.5),  # Cyan color
        name='100-pkt Moving Avg'
    ),
    row=2, col=1
)

# --- CHART 3: Histogram ---
fig.add_trace(
    go.Histogram(
        x=df['latency_s'],
        nbinsx=150,
        marker_color='#9D00FF',  # Neon Purple
        name='Distribution'
    ),
    row=3, col=1
)

# --- LAYOUT STYLING ---
fig.update_layout(
    title_text=f"<b>IoT Sensor Analytics ({len(df):,} packets)</b>",
    height=1000,
    template="plotly_dark",
    hovermode="x unified",
    showlegend=False  # Hiding legend as titles/colorbar explain enough
)

# Update Axis Labels to Seconds
fig.update_yaxes(title_text="Latency (s)", row=1, col=1)
fig.update_yaxes(title_text="Avg Latency (s)", row=2, col=1)
fig.update_yaxes(title_text="Packet Count", row=3, col=1)
fig.update_xaxes(title_text="Latency (s)", row=3, col=1)

# Add 99% Threshold Line (Red Dashed)
fig.add_hline(y=p99_lat, line_dash="dash", line_color="#FF3333",
              annotation_text=f"99%: {p99_lat:.3f}s",
              annotation_position="top left",
              row=1, col=1)

output_file = "latency_analytics_v2.html"
fig.write_html(output_file)
print(f"Done! Open {output_file} in your browser.")
# fig.show()