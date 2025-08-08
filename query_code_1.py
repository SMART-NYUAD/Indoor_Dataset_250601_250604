
# Setup
time0 = datetime(2025, 6, 1, 17, 0).timestamp()
time1 = datetime(2025, 6, 4, 9, 0).timestamp()
pref_7 = "all_sensor_07_"
parameter = [
    "temperature", "humidity", "light_intensity", "voc", "nox",
    "pm_1_0", "pm_2_5", "pm_4_0", "pm_10_0", "decibel",
    "co2", "tvoc", "pressure", "formaldehyde", "voc_index"
]

# Connect to database
connection, cursor = makeDBConnection_02()

# Fetch and merge data
merged_df = None
for param in parameter:
    df = fetch_raw_data(cursor, pref_7 + param, time0, time1)
    
    if not df.empty:
        df = df.rename(columns={'state': param})
        
        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on='timestamp', how='outer')

# Close database connection
cursor.close()
connection.close()

# Save to CSV
if merged_df is not None:
    merged_df = merged_df.sort_values('timestamp')
    merged_df['datetime'] = merged_df['timestamp'].apply(datetime.fromtimestamp)
    
    # Reorder columns
    cols = ['datetime', 'timestamp'] + [col for col in merged_df.columns if col not in ['datetime', 'timestamp']]
    merged_df = merged_df[cols]
    
    filename = "sensor_data_merged.csv"
    merged_df.to_csv(filename, index=False)
    print(f"Data saved to: {filename}")
    print(f"Shape: {merged_df.shape}")
    print(merged_df.head())
else:
    print("No data found!")