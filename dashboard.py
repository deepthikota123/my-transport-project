import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
import plotly.express as px
from sqlalchemy.engine import URL

# Database Connection
def get_connection():
    # Matches your ETL config
    password = os.getenv("DB_PASSWORD", "Deepthi@3399")
    connection_url = URL.create(
        drivername="mysql+mysqlconnector",
        username="root",
        password=password,
        host="localhost",
        port=3306,
        database="city_transport_db"
    )
    db_connection = create_engine(connection_url)
    return db_connection

st.set_page_config(page_title="Hyderabad Transport Analytics", layout="wide")

st.title("🚊 Hyderabad Transport Command Center")
st.markdown("Real-time metrics from GTFS and Sensor Data")

try:
    engine = get_connection()
    
    # 1. KPI Metrics
    with engine.connect() as conn:
        # Fetch summary stats
        kpi_query = text("SELECT AVG(ridership) as avg_ride, AVG(congestion_index) as avg_cong, AVG(average_speed_kmh) as avg_spd FROM unified_transport_metrics")
        kpi = pd.read_sql(kpi_query, conn)
        
    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Ridership", f"{int(kpi['avg_ride'][0])}")
    col2.metric("Avg Congestion", f"{round(kpi['avg_cong'][0], 2)}")
    col3.metric("Avg Speed (km/h)", f"{round(kpi['avg_spd'][0], 1)}")

    # 2. Detailed Data
    query = "SELECT * FROM unified_transport_metrics ORDER BY timestamp DESC LIMIT 500"
    df = pd.read_sql(query, engine)

    # 3. Visualizations
    st.subheader("Congestion vs Speed Analysis")
    fig = px.scatter(df, x="average_speed_kmh", y="congestion_index", color="transport_type", size="ridership")
    st.plotly_chart(fig, width='stretch')

    st.subheader("Ridership Trends by Transport Mode")
    fig2 = px.bar(df.groupby('transport_type')['ridership'].sum().reset_index(), x='transport_type', y='ridership')
    st.plotly_chart(fig2, width='stretch')

except Exception as e:
    st.error(f"Database Connection Failed: {e}")