import pandas as pd
import psycopg2
from datetime import datetime

# Setup
time0 = datetime(2025, 7, 28, 9, 0).timestamp()
time1 = datetime(2025, 8, 4, 17, 0).timestamp()
pref_7 = "all_sensor_02_"
parameter = [
    "temperature", "humidity", "light_intensity", "voc", "nox",
    "pm_1_0", "pm_2_5", "pm_4_0", "pm_10_0", "decibel",
    "co2", "tvoc", "pressure", "formaldehyde", "voc_index"
]

# Connect to database
connection, cursor = makeDBConnection()

# Fetch and merge data
merged_df = None
for i, param in enumerate(parameter):
    metric_name = pref_7 + param
    df = fetch_raw_data(cursor, metric_name, time0, time1)
    
    if not df.empty:
        print(f"Parameter {i+1}/{len(parameter)}: {param} - {len(df)} rows")
        df = df.rename(columns={'state': param})
        
        # Group by rounded timestamp and take mean (or first) to handle multiple readings per minute
        df_grouped = df.groupby('timestamp_rounded').agg({
            'timestamp': 'first',  # Keep original timestamp
            param: 'mean'  # Average if multiple readings per minute
        }).reset_index()
        
        if merged_df is None:
            merged_df = df_grouped
        else:
            merged_df = pd.merge(merged_df, df_grouped[['timestamp_rounded', param]], 
                               on='timestamp_rounded', how='outer')
            print(f"After merging {param}: {len(merged_df)} rows")

# Close database connection
cursor.close()
connection.close()

# Save to CSV
if merged_df is not None:
    merged_df = merged_df.sort_values('timestamp_rounded')
    
    # Convert to datetime for final output
    merged_df['datetime'] = pd.to_datetime(merged_df['timestamp_rounded'], unit='s')
    
    # Reorder columns
    cols = ['datetime', 'timestamp_rounded'] + [col for col in merged_df.columns 
                                               if col not in ['datetime', 'timestamp_rounded', 'timestamp']]
    merged_df = merged_df[cols]
    
    filename = "sensor_data_merged.csv"
    merged_df.to_csv(filename, index=False)
    print(f"Data saved to: {filename}")
    print(f"Final shape: {merged_df.shape}")
    print(merged_df.head())
    print(merged_df.tail())
else:
    print("No data found!")