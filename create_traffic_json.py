import pandas as pd
import numpy as np
import json

# Configuration
INPUT_CSV = "hyderabad_transport_usage.csv"
OUTPUT_JSON = "traffic_sensors.json"

def generate_traffic_json():
    print("--- Simulating Traffic Sensor API Data ---")
    
    # 1. Read the Transport CSV to get the Route IDs and Times
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        print(f"Error: Could not find {INPUT_CSV}. Run the previous script first!")
        return

    # 2. Select only the columns we need to link the data
    traffic_data = df[['timestamp', 'route_id', 'transport_type']].copy()
    
    # 3. Simulate Sensor Metrics
    
    speeds = []
    congestion_indexes = []
    
    for index, row in traffic_data.iterrows():
        transport_type = row['transport_type']
        
        # Parse hour to check for peak traffic
        time_obj = pd.to_datetime(row['timestamp'])
        is_peak = (8 <= time_obj.hour <= 10) or (17 <= time_obj.hour <= 19)
        
        if 'Metro' in transport_type or 'MMTS' in transport_type:
            # Trains have no traffic, high speed
            speed = np.random.randint(35, 60) 
            congestion = 0.0  # No road congestion
        else:
            # Buses/Roads are affected by peak hours
            if is_peak:
                speed = np.random.randint(5, 15)   # Very slow
                congestion = np.random.uniform(0.7, 0.95) # High congestion
            else:
                speed = np.random.randint(25, 45)  # Normal speed
                congestion = np.random.uniform(0.1, 0.4) # Low congestion
                
        speeds.append(speed)
        congestion_indexes.append(round(congestion, 2))

    traffic_data['average_speed_kmh'] = speeds
    traffic_data['congestion_index'] = congestion_indexes
    
    # 4. Convert to JSON format
    json_output = traffic_data.to_dict(orient='records')
    
    # 5. Save the file
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(json_output, f, indent=4)
        
    print(f"Success! Created {OUTPUT_JSON} with {len(json_output)} records.")
    print("You can now use this file as your 'API Source' for the ETL pipeline.")

if __name__ == "__main__":
    generate_traffic_json()