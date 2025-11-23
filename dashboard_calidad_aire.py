import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import numpy as np
import seaborn as sns
from datetime import datetime

# --- Configuration & Style ---
# Using 'darkgrid' for better contrast in plots
plt.style.use('seaborn-v0_8-darkgrid') 
sns.set_palette("viridis") # A professional color palette

# ============================================================================
# LOAD CLEAN DATA (Keep paths generic for simplicity)
# ============================================================================

def load_clean_data():
    """
    Loads the processed data.
    """
    print("Loading clean data from ETL pipeline...")
    
    # Use the file names provided by the user
    try:
        # Assuming the final file is the hourly data
        df = pd.read_csv('air_quality_output/air_quality_final.csv') 
        # Assuming the daily file is the aggregated data
        df_daily = pd.read_csv('air_quality_output/air_quality_daily.csv') 
        print(f"Data loaded: {len(df)} hourly records, {len(df_daily)} daily records")
    except FileNotFoundError as e:
        # Fallback in a real-world scenario, here we'll assume files exist
        raise FileNotFoundError(f"One of the required CSV files was not found: {e}")

    # Convert date columns
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    if 'date' not in df.columns:
        df['date'] = df['timestamp'].dt.date

    df['hour'] = df['timestamp'].dt.hour
    
    # Ensure all required columns are available for the new plots
    required_cols = ['pm25', 'pm10', 'no2', 'o3', 'co', 'station', 'day_of_week', 'aqi_pm25']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        print(f"Warning: Missing columns for full dashboard functionality: {missing}")
        # Drop rows with NaN in critical columns if they exist
        df.dropna(subset=['pm25', 'station', 'aqi_pm25'], inplace=True) 

    # Day of week mapping for English labels and correct order
    day_mapping = {
        0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 
        4: 'Fri', 5: 'Sat', 6: 'Sun'
    }
    df['day_name'] = df['day_of_week'].map(day_mapping)
    df['day_name'] = pd.Categorical(df['day_name'], categories=day_mapping.values(), ordered=True)

    return df, df_daily

# ============================================================================
# ENHANCED PLOTS GENERATION (Translated to English)
# ============================================================================

def create_dashboard_plots(df):
    """
    Generates the improved and translated dashboard plots.
    """
    print("\nGenerating enhanced dashboard plots...")
    
    # --- Figure setup for a multi-plot layout ---
    fig = plt.figure(figsize=(18, 14))
    fig.suptitle("AIR QUALITY MONITORING DASHBOARD (PM2.5 Focus)", fontsize=22, fontweight='bold', y=0.995)
    
    # Define the grid - Adding row for KPIs at the top
    gs = GridSpec(4, 4, figure=fig, height_ratios=[0.5, 1.5, 1, 1])
    
    # KPI Section (Row 0)
    ax_kpi = fig.add_subplot(gs[0, :])
    ax_kpi.axis('off')  # Hide axes for KPI section
    
    # Calculate KPIs
    avg_pm25 = df['pm25'].mean()
    max_pm25 = df['pm25'].max()
    days_unhealthy = ((df.groupby('date')['pm25'].mean() > 35.4).sum())  # Days exceeding "Unhealthy for Sensitive" threshold
    total_days = df['date'].nunique()
    pct_unhealthy = (days_unhealthy / total_days) * 100
    worst_station = df.groupby('station')['pm25'].mean().idxmax()
    
    # Display KPIs with boxes
    kpi_text = f"""
    KEY PERFORMANCE INDICATORS (KPIs)
    
    Average PM2.5: {avg_pm25:.1f} μg/m³  |  Maximum PM2.5: {max_pm25:.1f} μg/m³  |  Days Unhealthy: {days_unhealthy}/{total_days} ({pct_unhealthy:.1f}%)  |  Worst Station: {worst_station}
    """
    
    ax_kpi.text(0.5, 0.5, kpi_text, 
                ha='center', va='center', 
                fontsize=13, 
                weight='bold',
                bbox=dict(boxstyle='round,pad=1', facecolor='lightblue', alpha=0.8, edgecolor='navy', linewidth=2))
    
    # Define the axes for each plot (adjusted for new grid)
    ax1 = fig.add_subplot(gs[1, :])    # Large plot for Time Series (Row 1, all columns)
    ax2 = fig.add_subplot(gs[2, 0:2])  # Mid-size plot for Daily Distribution (Row 2, Cols 0-1)
    ax3 = fig.add_subplot(gs[2, 2:])   # Mid-size plot for Station Comparison (Row 2, Cols 2-3)
    ax4 = fig.add_subplot(gs[3, 0:2])  # Mid-size plot for Hourly Pattern (Row 3, Cols 0-1)
    ax5 = fig.add_subplot(gs[3, 2:])   # Mid-size plot for Correlation Matrix (Row 3, Cols 2-3)
    
    # ------------------------------------------------------------------------
    # 1. TIME SERIES ANALYSIS: PM2.5 over time (Smoothed) - FIXED VERSION
    # ------------------------------------------------------------------------
    ax1.set_title('1. PM2.5 Concentration Over Time (Smoothed 6h Rolling Mean)', fontsize=16)
    
    # Sort by timestamp first
    df_sorted = df.sort_values('timestamp').copy()
    
    # Calculate rolling mean for each station separately and plot
    stations = df_sorted['station'].unique()
    colors = plt.cm.viridis(np.linspace(0, 1, len(stations)))
    
    for i, station in enumerate(stations):
        station_data = df_sorted[df_sorted['station'] == station].copy()
        station_data = station_data.set_index('timestamp')
        
        # Calculate rolling mean
        pm25_rolling = station_data['pm25'].rolling(window=6, center=True).mean()
        
        # Plot the smoothed line
        ax1.plot(pm25_rolling.index, pm25_rolling.values, 
                label=station, lw=2, alpha=0.8, color=colors[i])
    
    # Formatting X-axis (dates)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax1.tick_params(axis='x', rotation=30)
    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('PM2.5 Concentration (μg/m³)', fontsize=12)
    ax1.legend(title='Station', loc='upper right')
    
    # Add a horizontal line for the WHO guideline
    ax1.axhline(y=25, color='r', linestyle='--', alpha=0.7, label='AQI Moderate Threshold (24h)') 
    ax1.text(ax1.get_xlim()[1], 25, ' Moderate Threshold ', va='center', ha="right", backgroundcolor='w')


    # ------------------------------------------------------------------------
    # 2. DAILY AQI DISTRIBUTION: PM2.5 by Day of Week (Violin Plot)
    # ------------------------------------------------------------------------
    ax2.set_title('2. PM2.5 Distribution by Day of Week', fontsize=16)
    sns.violinplot(
        x='day_name', 
        y='pm25', 
        data=df, 
        ax=ax2, 
        inner='quartile', 
        palette='Spectral',
        hue='day_name',
        legend=False
    )
    ax2.set_xlabel('Day of Week', fontsize=12)
    ax2.set_ylabel('PM2.5 Concentration (μg/m³)', fontsize=12)
    # Highlight Weekend vs Weekday
    ax2.axvspan(4.5, 6.5, color='gray', alpha=0.1, zorder=0)
    ax2.text(5.5, ax2.get_ylim()[1] * 0.95, 'Weekend', ha='center', va='top', fontsize=10)


    # ------------------------------------------------------------------------
    # 3. STATION COMPARISON: Average PM2.5
    # ------------------------------------------------------------------------
    station_avg = df.groupby('station')['pm25'].mean().sort_values(ascending=False).reset_index()
    
    ax3.set_title('3. Average PM2.5 Concentration by Station', fontsize=16)
    sns.barplot(
        x='station', 
        y='pm25', 
        data=station_avg, 
        ax=ax3, 
        palette='cubehelix',
        hue='station',
        legend=False
    )
    ax3.set_xlabel('Monitoring Station', fontsize=12)
    ax3.set_ylabel('Average PM2.5 (μg/m³)', fontsize=12)
    ax3.tick_params(axis='x', rotation=0)

    # Annotate bars with the average value
    for p in ax3.patches:
        ax3.annotate(f'{p.get_height():.1f}', 
                     (p.get_x() + p.get_width() / 2., p.get_height()), 
                     ha='center', va='center', 
                     xytext=(0, 9), 
                     textcoords='offset points',
                     fontsize=10)


    # ------------------------------------------------------------------------
    # 4. HOURLY PATTERN: Average PM2.5 by Hour
    # ------------------------------------------------------------------------
    hourly_pattern = df.groupby('hour')['pm25'].mean().reset_index()
    
    ax4.set_title('4. Average PM2.5 Concentration by Hour of Day', fontsize=16)
    sns.lineplot(
        x='hour', 
        y='pm25', 
        data=hourly_pattern, 
        ax=ax4, 
        marker='o', 
        color='darkorange', 
        lw=3
    )
    ax4.set_xlabel('Hour of Day (0-23)', fontsize=12)
    ax4.set_ylabel('Average PM2.5 (μg/m³)', fontsize=12)
    ax4.set_xticks(range(0, 24, 2))
    ax4.grid(True, linestyle='--', alpha=0.6)
    
    # Highlight Rush Hours (e.g., 7-9 and 17-19)
    ax4.axvspan(7, 10, color='red', alpha=0.1)
    ax4.axvspan(17, 20, color='red', alpha=0.1)
    ax4.text(8.5, ax4.get_ylim()[1] * 0.9, 'Morning Rush', ha='center', va='top', fontsize=9)
    ax4.text(18.5, ax4.get_ylim()[1] * 0.9, 'Evening Rush', ha='center', va='top', fontsize=9)


    # ------------------------------------------------------------------------
    # 5. POLLUTANT CORRELATION MATRIX (Heatmap)
    # ------------------------------------------------------------------------
    pollutant_cols = ['pm25', 'pm10', 'no2', 'o3', 'co', 'temperature', 'humidity', 'aqi_pm25']
    # Only use columns that exist
    available_cols = [col for col in pollutant_cols if col in df.columns]
    corr_matrix = df[available_cols].corr()
    
    ax5.set_title('5. Correlation Matrix of Key Variables', fontsize=16)
    # Create the heatmap
    sns.heatmap(
        corr_matrix, 
        annot=True, 
        fmt=".2f", 
        cmap='coolwarm', 
        ax=ax5, 
        cbar_kws={'label': 'Correlation Coefficient'},
        linewidths=.5, 
        linecolor='black'
    )
    ax5.tick_params(axis='x', rotation=45)
    ax5.tick_params(axis='y', rotation=0)

    # --- Final Polish ---
    plt.tight_layout(rect=[0, 0, 1, 0.98]) # Adjust layout to make space for suptitle
    
    # Save the figure
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"air_quality_dashboard_enhanced_{timestamp_str}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close() # Close the plot figure to free memory
    
    print(f"\n✅ Dashboard successfully generated and saved to: {output_path}")


# ============================================================================
# EXECUTION
# ============================================================================
if __name__ == '__main__':
    try:
        df_hourly, df_daily = load_clean_data()
        create_dashboard_plots(df_hourly)
    except FileNotFoundError as e:
        print(f"Error: Could not run the dashboard. Please ensure 'air_quality_final.csv' and 'air_quality_daily.csv' are in the same directory. Details: {e}")
    except Exception as e:
        print(f"Error generating dashboard: {e}")
        import traceback
        traceback.print_exc()