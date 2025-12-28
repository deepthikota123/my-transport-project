import pandas as pd
import json
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


import urllib.parse  # <--- 1. Add this import at the top

# ... inside your DB_CONFIG ...
DB_CONFIG = {
    "user": "root",
    "password": "Deepthi@3399", 
    "host": "localhost",
    "port": 3306,
    "database": "city_transport_db"
}

def get_db_engine(target_db=None):
    # 2. Encode the password to handle the '@' symbol safely
    safe_password = urllib.parse.quote_plus(DB_CONFIG["password"]) 
    
    # 3. Construct the connection string manually to be 100% safe
    connection_str = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{safe_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    
    if target_db:
        connection_str += f"/{target_db}"
        
    return create_engine(connection_str)
# File Paths
CSV_FILE = "hyderabad_transport_usage.csv"
JSON_FILE = "traffic_sensors.json"
SUMMARY_OUTPUT = "final_route_summary.csv"

def get_db_engine(target_db=None):
    """
    Creates the SQLAlchemy engine using URL.create().
    This is the safest way to handle passwords with '@' or other symbols.
    """
    connection_url = URL.create(
        drivername="mysql+mysqlconnector",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=target_db  
    )
    return create_engine(connection_url)

def setup_database():
    """Creates the database if it doesn't exist."""
    print("--- SETTING UP DATABASE ---")
    
    engine = get_db_engine(target_db=None)
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}"))
            print(f" Database '{DB_CONFIG['database']}' checked/created.")
    except Exception as e:
        print(f" Database Setup Failed: {e}")
        print("   Ensure XAMPP/MySQL is running.")
        exit()

# STEP 1: EXTRACT
def extract_data():
    print("\n--- STEP 1: EXTRACTION ---")
    
    if not os.path.exists(CSV_FILE) or not os.path.exists(JSON_FILE):
        print(" Error: Input files missing.")
        exit()

    df_usage = pd.read_csv(CSV_FILE)
    print(f"-> Loaded CSV: {len(df_usage)} rows")

    with open(JSON_FILE, 'r') as f:
        df_sensors = pd.DataFrame(json.load(f))
    print(f"-> Loaded JSON: {len(df_sensors)} rows")
    
    return df_usage, df_sensors

# STEP 2: TRANSFORM
def transform_data(df_usage, df_sensors):
    print("\n--- STEP 2: TRANSFORMATION ---")
    
    # Standardize Dates
    df_usage['timestamp'] = pd.to_datetime(df_usage['timestamp'], errors='coerce')
    df_sensors['timestamp'] = pd.to_datetime(df_sensors['timestamp'], errors='coerce')

    # Merge
    unified_df = pd.merge(
        df_usage, 
        df_sensors, 
        on=['timestamp', 'route_id', 'transport_type'], 
        how='inner'
    )
    
    # Clean
    unified_df = unified_df.dropna(subset=['timestamp'])
    unified_df = unified_df[(unified_df['ridership'] >= 0) & (unified_df['average_speed_kmh'] >= 0)]
    
    # Impute Congestion
    if unified_df['congestion_index'].isnull().any():
        median_val = unified_df['congestion_index'].median()
        unified_df['congestion_index'] = unified_df['congestion_index'].fillna(median_val)

    print(f"-> Cleaned Data: {len(unified_df)} rows ready.")
    return unified_df

# STEP 3: LOAD (With Chunking Fix)
def load_to_mysql(df):
    print("\n--- STEP 3: LOADING TO MYSQL ---")
    print(f"   (Inserting {len(df)} rows in batches of 5000...)")
    
    # Connect specifically to our target database
    engine = get_db_engine(target_db=DB_CONFIG["database"])
    
    try:
        # Use a transaction block
        with engine.begin() as connection:
            df.to_sql(
                "unified_transport_metrics", 
                connection, 
                if_exists='replace', 
                index=False,
                chunksize=5000  
            )
        print(f" Successfully inserted {len(df)} rows into MySQL.")
    except Exception as e:
        print(f" Load Failed: {e}")

# STEP 4: REPORT
def create_summary_report():
    print("\n--- STEP 4: SUMMARY REPORT ---")
    engine = get_db_engine(target_db=DB_CONFIG["database"])
    
    query = """
    SELECT route_name, transport_type, 
           ROUND(AVG(ridership), 0) as avg_ridership,
           ROUND(AVG(congestion_index), 2) as avg_congestion,
           ROUND(AVG(average_speed_kmh), 1) as avg_speed
    FROM unified_transport_metrics
    GROUP BY route_name, transport_type
    ORDER BY avg_ridership DESC;
    """
    try:
        df = pd.read_sql(query, engine)
        if df.empty:
            print(" Warning: Query returned 0 rows.")
        else:
            df.to_csv(SUMMARY_OUTPUT, index=False)
            print(f" CSV Generated: {os.path.abspath(SUMMARY_OUTPUT)}")
            print(df.head())
    except Exception as e:
        print(f" Report Failed: {e}")

if __name__ == "__main__":
    setup_database()
    usage, sensors = extract_data()
    clean_data = transform_data(usage, sensors)
    
    if not clean_data.empty:
        load_to_mysql(clean_data)
        create_summary_report()
    else:

        print("❌ Transformation resulted in empty dataset.")
