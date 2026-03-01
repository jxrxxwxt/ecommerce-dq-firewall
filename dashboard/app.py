import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px

# Configure page settings
st.set_page_config(page_title="Data Quality Dashboard", layout="wide")

def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host=db_host,
            port="5432"
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def load_data():
    """
    Fetch metrics data from the database and return as a Pandas DataFrame.
    """
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = "SELECT * FROM data_quality_metrics ORDER BY execution_time DESC;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def main():
    # Build the Dashboard UI
    st.title("Data Quality Firewall Dashboard")
    st.markdown("Monitoring pipeline health and data validation metrics.")

    df = load_data()

    if df.empty:
        st.warning("No data quality metrics found. Please ensure the pipeline has been executed.")
        return

    # Calculate Key Performance Indicators (KPIs)
    total_runs = len(df)
    latest_run = df.iloc[0]
    success_rate = (len(df[df['status'] == 'PASSED']) / total_runs) * 100

    # Display KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Pipeline Runs", total_runs)
    col2.metric("Overall Success Rate", f"{success_rate:.1f}%")
    
    # Color-code the latest status metric
    status_color = "normal" if latest_run['status'] == 'PASSED' else "inverse"
    col3.metric("Latest Run Status", latest_run['status'], delta_color=status_color)

    st.markdown("---")

    # Data Visualizations
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.subheader("Failed Rules Trend")
        fig_trend = px.line(
            df, 
            x='execution_time', 
            y='failed_rules_count', 
            markers=True, 
            title="Number of Failed Rules Over Time"
        )
        fig_trend.update_layout(xaxis_title="Execution Time", yaxis_title="Failed Rules Count")
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_chart2:
        st.subheader("Validation Status Distribution")
        status_counts = df['status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        fig_pie = px.pie(
            status_counts, 
            names='status', 
            values='count', 
            title="Pass vs Fail Ratio", 
            color='status',
            color_discrete_map={'PASSED': '#00CC96', 'FAILED': '#EF553B'}
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # Raw Data Table
    st.subheader("Recent Execution Logs")
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()