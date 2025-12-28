import pandas as pd
import numpy as np
import os

# Configuration: These need to match the folder names exactly!
METRO_FOLDER = "Telangana_opendata_gtfs_hmrl_04_November_2025"
MMTS_FOLDER = "open_data_mmts_hyd"
OUTPUT_FILE = "hyderabad_transport_usage.csv"

#Reading the raw text files from the GTFS folder and links them togetherto get a clean list of Routes and their Stops.
def load_gtfs_routes(folder_path, system_name):
    print(f"--- Working on {system_name} data found in: {folder_path} ---")
    
    if not os.path.exists(folder_path):
        print(f"Oops! Couldn't find the folder '{folder_path}'. Double check the name.")
        return pd.DataFrame()

    try:
        routes = pd.read_csv(f"{folder_path}/routes.txt")
        
        #standardize column names into one common 'route_name' column.
        if 'route_long_name' in routes.columns:
            routes['route_name'] = routes['route_long_name']
        elif 'route_short_name' in routes.columns:
            routes['route_name'] = routes['route_short_name']
        else:
            routes['route_name'] = "Route " + routes['route_id'].astype(str)
        
        routes = routes[['route_id', 'route_name']]

        #Get Trips and Stops
        trips = pd.read_csv(f"{folder_path}/trips.txt")[['trip_id', 'route_id']]
        stops = pd.read_csv(f"{folder_path}/stops.txt")[['stop_id', 'stop_name']]
        stop_times = pd.read_csv(f"{folder_path}/stop_times.txt")[['trip_id', 'stop_id']]


        #Dropping duplicates to save processing time.
        unique_trips = trips.drop_duplicates(subset=['route_id'])
        
        #joining everything together: Route -> Trip -> Stop Sequence -> Stop Name
        merged = pd.merge(unique_trips, stop_times, on='trip_id')
        merged = pd.merge(merged, stops, on='stop_id')
        merged = pd.merge(merged, routes, on='route_id')
        
        #Final cleanup
        final_df = merged[['route_id', 'route_name', 'stop_name']].copy()
        final_df['transport_type'] = system_name
        
        print(f"Done! Successfully mapped {len(final_df)} stops for {system_name}.")
        return final_df

    except Exception as e:
        print(f"Something went wrong reading files in {folder_path}. Error: {e}")
        return pd.DataFrame()

#generates realistic synthetic data for ridership and congestion based on the time of day
def generate_usage_data(df):
    print("\n--- Generating Synthetic Ridership & Traffic Stats ---")
    
    timestamps = pd.date_range(start='2024-11-21 00:00:00', periods=24, freq='h')
    
    expanded_data = []

    for t in timestamps:
        hour = t.hour
        
        is_peak_hour = (8 <= hour <= 10) or (17 <= hour <= 19)
        
        hourly_batch = df.copy()
        hourly_batch['timestamp'] = t
        
        if is_peak_hour:
            hourly_batch['ridership'] = np.random.randint(500, 1500, size=len(df))
            hourly_batch['traffic_congestion_index'] = np.random.uniform(0.7, 1.0, size=len(df)).round(2)
        else:
            hourly_batch['ridership'] = np.random.randint(50, 400, size=len(df))
            hourly_batch['traffic_congestion_index'] = np.random.uniform(0.1, 0.5, size=len(df)).round(2)
            
        expanded_data.append(hourly_batch)
    
    return pd.concat(expanded_data)

# Main Execution Flow
if __name__ == "__main__":
    # 1. Process the Metro files
    metro_data = load_gtfs_routes(METRO_FOLDER, "Metro")
    
    # 2. Process the MMTS (Train) files
    mmts_data = load_gtfs_routes(MMTS_FOLDER, "MMTS_Train")
    
    # 3. Combine them into one network
    full_network = pd.concat([metro_data, mmts_data])
    
    # Check if we actually got data back before proceeding
    if not full_network.empty:
        # 4. Add the synthetic sensor data
        final_csv = generate_usage_data(full_network)
        
        # 5. Save to disk
        final_csv.to_csv(OUTPUT_FILE, index=False)
        print(f"\nSuccess! The dataset is ready: '{OUTPUT_FILE}'")
        print(f"Total rows generated: {len(final_csv)}")
        print("Here's a sneak peek of the data:")
        print(final_csv.head())
    else:
        print("\nFailed. No data was loaded. Please verify the folder names and paths.")