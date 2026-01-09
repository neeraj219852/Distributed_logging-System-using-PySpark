"""
Streamlit Dashboard for Log Processing System
Modern, interactive dashboard for visualizing log analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import json
import glob
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Distributed Log Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed" # Hide default sidebar
)

# Custom CSS for "Cyberpunk/Modern" Dark UI
st.markdown("""
<style>
    /* Global Styles */
    .stApp {
        background-color: #F8FAFC; /* Slate-50 */
        font-family: 'Inter', sans-serif;
    }
    
    /* Remove top padding */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
    }

    /* --- Header --- */
    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 10px 0 20px 0;
        border-bottom: 1px solid #E2E8F0;
        margin-bottom: 20px;
    }
    .app-title {
        color: #1E293B;
        font-size: 1.5rem;
        font-weight: 700;
        margin: 0;
    }
    .app-subtitle {
        color: #64748B;
        font-size: 0.875rem;
        margin: 0;
    }
    .header-icons {
        display: flex;
        gap: 15px;
        color: #0891B2;
        font-size: 1.2rem;
    }
    .icon-btn {
        cursor: pointer;
        padding: 8px;
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 36px;
        height: 36px;
        transition: all 0.2s;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    .icon-btn:hover {
        background: #F1F5F9;
        color: #06B6D4;
        border-color: #CBD5E1;
    }

    /* --- Cards --- */
    .kpi-card {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 20px;
        height: 100%;
        position: relative;
        overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .kpi-icon-box {
        width: 40px;
        height: 40px;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.2rem;
        margin-bottom: 15px;
    }
    .kpi-label {
        color: #64748B;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
        margin-bottom: 5px;
    }
    .kpi-value {
        color: #1E293B;
        font-size: 1.75rem;
        font-weight: 700;
        line-height: 1;
    }
    .trend-indicator {
        position: absolute;
        top: 20px;
        right: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        display: flex;
        align-items: center;
        gap: 4px;
    }

    /* --- Filter Panel (Right Sidebar) --- */
    .filter-panel {
        background-color: #FFFFFF;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #E2E8F0;
        height: 100%;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .filter-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
        border-bottom: 1px solid #E2E8F0;
        padding-bottom: 15px;
    }
    .filter-title {
        color: #1E293B;
        font-weight: 600;
        font-size: 1.1rem;
    }
    .reset-btn {
        color: #0891B2;
        font-size: 0.8rem;
        cursor: pointer;
        text-decoration: none;
    }
    .filter-section-label {
        color: #64748B;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        margin-top: 20px;
        margin-bottom: 10px;
    }

    /* --- Charts Container --- */
    .chart-container {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .chart-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 15px;
    }
    .chart-title {
        color: #1E293B;
        font-size: 1.1rem;
        font-weight: 600;
    }

    /* --- List/Table Items --- */
    .list-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 12px 0;
        border-bottom: 1px solid #E2E8F0;
    }
    .list-item:last-child { border-bottom: none; }
    
    .progress-bg {
        background-color: #E2E8F0;
        height: 6px;
        border-radius: 3px;
        width: 100%;
        margin-top: 8px;
    }
    .progress-fill {
        height: 100%;
        border-radius: 3px;
    }

    /* Overrides for Streamlit widgets to match theme */
    div[data-baseweb="select"] > div {
        background-color: #FFFFFF;
        border-color: #CBD5E1;
        color: #1E293B;
    }
    div[data-testid="stCheckbox"] label {
        color: #475569;
    }
    input[type="text"] {
        background-color: #FFFFFF;
        color: #1E293B;
        border-color: #CBD5E1;
    }

</style>
""", unsafe_allow_html=True)


# --- Helper Functions (Loaders) ---

@st.cache_data
def load_raw_data(data_dir: str = "data/processed") -> pd.DataFrame:
    """Load processed log data"""
    try:
        # Try Parquet first
        parquet_path = data_dir
        if os.path.exists(parquet_path):
            if os.path.isfile(parquet_path) and parquet_path.endswith('.parquet'):
                df = pd.read_parquet(parquet_path)
                return df.sort_values('timestamp', ascending=False) if 'timestamp' in df.columns else df
            elif os.path.isdir(parquet_path):
                parquet_files = glob.glob(os.path.join(parquet_path, "*.parquet"))
                if parquet_files:
                    df_list = [pd.read_parquet(f) for f in parquet_files]
                    if df_list:
                        df = pd.concat(df_list, ignore_index=True)
                        return df.sort_values('timestamp', ascending=False) if 'timestamp' in df.columns else df
        
        # Fallback to CSV
        csv_dir = "data/raw_logs"
        if not os.path.exists(csv_dir):
            return pd.DataFrame()
            
        all_files = glob.glob(os.path.join(csv_dir, "*.csv"))
        if not all_files:
            return pd.DataFrame()
            
        df_list = []
        for filename in all_files:
            try:
                df = pd.read_csv(filename, header=0, quotechar='"')
                df.columns = df.columns.str.lower()
                
                if 'timestamp' not in df.columns and 'date' in df.columns and 'time' in df.columns:
                     try:
                        combined = df['date'].astype(str) + ' ' + df['time'].astype(str).str.replace(',', '.')
                        df['timestamp'] = pd.to_datetime(combined, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
                     except: pass
                
                if 'level' in df.columns: df.rename(columns={'level': 'log_level'}, inplace=True)
                if 'content' in df.columns: df.rename(columns={'content': 'message'}, inplace=True)
                if 'eventtemplate' in df.columns: df.rename(columns={'eventtemplate': 'error_type'}, inplace=True)
                
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    df = df.dropna(subset=['timestamp'])
                    
                df_list.append(df)
            except Exception: continue
                
        if not df_list: return pd.DataFrame()
        final_df = pd.concat(df_list, ignore_index=True)
        return final_df.sort_values('timestamp', ascending=False) if 'timestamp' in final_df.columns else final_df
    except Exception as e:
        return pd.DataFrame()

def filter_data(df: pd.DataFrame, date_range, search_query: str, selected_levels: list, service_source: str) -> pd.DataFrame:
    """Apply filters"""
    if df.empty: return df
    filtered_df = df.copy()
    
    # Time Range
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        # Convert to datetime64[ns] to match df
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date) + timedelta(days=1) - timedelta(seconds=1)
        if 'timestamp' in filtered_df.columns:
            filtered_df = filtered_df[(filtered_df['timestamp'] >= start_ts) & (filtered_df['timestamp'] <= end_ts)]
        
    # Log Levels
    if 'log_level' in filtered_df.columns and selected_levels:
        # Filter if selected, if none selected imply ALL? Or None? Usually ALL if empty or check "All". 
        # But here we have explicit checkboxes.
        if selected_levels:
            # Case insensitive
            filtered_df = filtered_df[filtered_df['log_level'].astype(str).str.upper().isin(selected_levels)]
            
    # Search (now exact match via dropdown)
    if search_query and search_query != "All":
        # Check if we should filter by error_type or message
        # We assume if the user selected something, it came from the list generated in render_filters
        target_col = 'error_type' if 'error_type' in filtered_df.columns else 'message'
        
        if target_col in filtered_df.columns:
             filtered_df = filtered_df[filtered_df[target_col] == search_query]
        
    return filtered_df

def load_alerts() -> list:
    # ... logic to load alerts ...
    # Mocking for UI visual parity
    return [
        {"title": "High CPU Usage", "message": "Server 01 CPU > 90%", "type": "Critical", "time_ago": "2m ago"},
        {"title": "Database Latency", "message": "Query time > 500ms", "type": "Warning", "time_ago": "15m ago"},
        {"title": "API Rate Limit", "message": "Client X exceeds quota", "type": "Warning", "time_ago": "1h ago"},
        {"title": "Service Restart", "message": "Auth service restarted", "type": "Info", "time_ago": "2h ago"}
    ]

# --- UI Renderers ---

def render_kpi(title, value, unit, color, icon, trend, trend_val):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="trend-indicator" style="color: {color};">
            {trend_val} {trend}
        </div>
        <div class="kpi-icon-box" style="background-color: {color}20; color: {color};">
            {icon}
        </div>
        <div class="kpi-label">{title}</div>
        <div class="kpi-value">{value}<span style="font-size: 1rem; color: #64748B; margin-left: 4px;">{unit}</span></div>
    </div>
    """, unsafe_allow_html=True)

def render_progress_bar(label, value, max_val, color):
    # Safe division
    pct = (value / max_val * 100) if max_val > 0 else 0
    st.markdown(f"""
    <div style="margin-bottom: 15px;">
        <div style="display: flex; justify-content: space-between; font-size: 0.85rem; color: #475569; margin-bottom: 5px;">
            <span>{label}</span>
            <span style="font-weight: 600;">{value}</span>
        </div>
        <div class="progress-bg">
            <div class="progress-fill" style="width: {pct}%; background-color: {color};"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# --- Main App ---

def render_filters(df: pd.DataFrame):
    with st.container():
        st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
        st.markdown("""
        <div class="filter-header">
            <div class="filter-title">Filters</div>
            <a class="reset-btn">RESET</a>
        </div>
        """, unsafe_allow_html=True)
        
        # Date Filter Mode
        st.markdown('<div class="filter-section-label">DATE FILTER</div>', unsafe_allow_html=True)
        filter_mode = st.radio("Filter Mode", ["All Time", "Custom Range"], horizontal=True, label_visibility="collapsed")

        date_range = None
        if filter_mode == "Custom Range":
            default_start = datetime.now().date() - timedelta(days=7)
            default_end = datetime.now().date()
            c_start, c_end = st.columns(2)
            with c_start:
                start_date = st.date_input("Start Date", value=default_start)
            with c_end:
                end_date = st.date_input("End Date", value=default_end)
            date_range = (start_date, end_date)
        
        st.markdown('<div class="filter-section-label">LOG LEVELS</div>', unsafe_allow_html=True)
        checks = {}
        # Use columns for layout if desired, but vertical list is standard
        checks['INFO'] = st.checkbox("INFO", value=True)
        checks['WARN'] = st.checkbox("WARN", value=True)
        checks['ERROR'] = st.checkbox("ERROR", value=True)
        checks['DEBUG'] = st.checkbox("DEBUG", value=False)
        selected_levels = [lvl for lvl, checked in checks.items() if checked]
        
        st.markdown('<div class="filter-section-label">SERVICE SOURCE</div>', unsafe_allow_html=True)
        service_source = st.selectbox("Service Source", ["All Services", "Auth", "Payment", "Database"], label_visibility="collapsed")
        
        st.markdown('<div class="filter-section-label">LOG PATTERN / TYPE</div>', unsafe_allow_html=True)
        
        # dynamic log suggestions based on Error Type/Template
        log_options = ["All"]
        # Prefer error_type (EventTemplate) if available, otherwise fallback to message or partial message
        target_col = 'error_type' if 'error_type' in df.columns else 'message'
        
        if not df.empty and target_col in df.columns:
            # unique values
            distinct_logs = df[target_col].dropna().unique().tolist()
            log_options.extend(sorted(distinct_logs)[:500])
            
        search_query = st.selectbox("Select Log Type", log_options, label_visibility="collapsed")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        if st.button("Apply Filters", type="primary", width="stretch"):
            st.session_state['apply_clicked'] = True
            
        st.markdown('</div>', unsafe_allow_html=True)
        return date_range, selected_levels, search_query, service_source

def main():
    # Header
    st.markdown(f"""
    <div class="header-container">
        <div>
            <h1 class="app-title">Python-Based Distributed Log Processing System</h1>
            <p class="app-subtitle">Interactive Log Analytics Dashboard</p>
        </div>
        <div class="header-icons">
            <div class="icon-btn">🔔</div>
            <div class="icon-btn">⚙️</div>
            <div class="icon-btn">JS</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Layout: Main Content (3) vs Filters (1)
    col_main, col_filters = st.columns([3.5, 1])

    # -- Data Processing --
    df = load_raw_data()
    
    with col_filters:
        time_range, selected_levels, search_query, service_source = render_filters(df)
    
    # Apply Filters
    filtered_df = filter_data(df, time_range, search_query, selected_levels, service_source)
    
    # KPIs
    total_logs = len(filtered_df)
    total_errors = len(filtered_df[filtered_df['log_level'] == 'ERROR']) if not filtered_df.empty and 'log_level' in filtered_df.columns else 0
    
    # Mock some data if empty to show the UI (for development/demo)
    if df.empty:
        total_logs = 1245392
        total_errors = 423
        warn_count = 1029
        err_rate = 0.034
    else:
        warn_count = len(filtered_df[filtered_df['log_level'] == 'WARN']) if 'log_level' in filtered_df.columns else 0
        err_rate = (total_errors / total_logs * 100) if total_logs > 0 else 0

    with col_main:
        # KPI Row
        k1, k2, k3, k4 = st.columns(4)
        with k1: render_kpi("Total Logs", f"{total_logs:,}", "", "#06B6D4", "⊞", "↗", "+12%") # Cyan
        with k2: render_kpi("Total Errors", f"{total_errors}", "", "#EF4444", "!", "↗", "+5%") # Red
        with k3: render_kpi("Warning Count", f"{warn_count:,}", "", "#EAB308", "⚠️", "↗", "+2%") # Yellow
        with k4: render_kpi("Error Rate", f"{err_rate:.3f}%", "", "#06B6D4", "%", "↘", "-1%") # Cyan

        st.markdown("<br>", unsafe_allow_html=True)

        # Charts Row
        c1, c2 = st.columns([2, 1])
        
        with c1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("""
            <div class="chart-header">
                <div class="chart-title">Error Trends Over Time</div>
                <div style="color: #94A3B8;">•••</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Trend Chart
            if not filtered_df.empty and 'timestamp' in filtered_df.columns:
                err_df = filtered_df[filtered_df['log_level'] == 'ERROR'].copy()
                if not err_df.empty:
                    hourly_counts = err_df.set_index('timestamp').resample('h').size().reset_index(name='count')
                    
                    fig = px.area(hourly_counts, x='timestamp', y='count')
                    fig.update_traces(line_color='#22D3EE', fillcolor='rgba(34, 211, 238, 0.1)')
                    fig.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        margin=dict(l=0, r=0, t=0, b=0),
                        xaxis=dict(showgrid=False, title=None, tickfont=dict(color='#64748B')),
                        yaxis=dict(showgrid=True, gridcolor='#E2E8F0', title=None, tickfont=dict(color='#64748B')),
                        height=250
                    )
                    st.plotly_chart(fig, config={'displayModeBar': False}, width="stretch")
                else:
                    st.info("No error data for selected period")
            else:
                 # Empty state instead of mock data
                 st.markdown('<div style="height:250px; display:flex; align-items:center; justify-content:center; color:#64748B;">No Data Available</div>', unsafe_allow_html=True)

            st.markdown('</div>', unsafe_allow_html=True)
            
        with c2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="chart-title" style="margin-bottom:20px;">Log Level Distribution</div>', unsafe_allow_html=True)
            
            # Simple Bar Chart for Levels
            levels_data = pd.DataFrame({
                'Level': ['INFO', 'WARN', 'ERROR'],
                'Count': [total_logs - total_errors - warn_count, warn_count, total_errors]
            })
            colors = ['#06B6D4', '#EAB308', '#EF4444']
            
            if total_logs > 0:
                fig_bar = go.Figure(data=[go.Bar(
                    x=levels_data['Level'],
                    y=levels_data['Count'],
                    marker_color=colors,
                    width=0.4
                )])
                fig_bar.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=0, r=0, t=0, b=0),
                    xaxis=dict(showgrid=False, tickfont=dict(color='#64748B')),
                    yaxis=dict(showgrid=False, showticklabels=False),
                    height=250,
                    bargap=0.2
                )
                st.plotly_chart(fig_bar, config={'displayModeBar': False}, width="stretch")
            else:
                 st.markdown('<div style="height:250px; display:flex; align-items:center; justify-content:center; color:#64748B;">No Data</div>', unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)

        # Bottom Section
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title" style="margin-bottom:20px;">Top Frequent Errors</div>', unsafe_allow_html=True)
        
        if not filtered_df.empty and 'message' in filtered_df.columns:
            # Calculate top errors dynamically
            error_counts = filtered_df[filtered_df['log_level'] == 'ERROR']['message'].value_counts().head(5)
            if not error_counts.empty:
                 max_val = error_counts.max()
                 for message, count in error_counts.items():
                     # Truncate long messages
                     display_msg = (message[:75] + '...') if len(message) > 75 else message
                     render_progress_bar(display_msg, count, max_val, "#EF4444")
            else:
                st.info("No errors found in the current selection.")
        else:
             st.markdown('<div style="color:#64748B; padding: 20px 0;">No data to analyze.</div>', unsafe_allow_html=True)
            
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()


