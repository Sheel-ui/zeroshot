import pandas as pd
import pyarrow.parquet as pq
import sys
from pathlib import Path


def analyze_sensor_data(df):
    """Analyze sensor data and print statistics."""
    # Get unique sensor types
    sensor_types = df['sensor_type'].unique()
    
    print("\nSensor Types Found:", sensor_types)
    
    # Print overall statistics for each sensor type
    for sensor_type in sensor_types:
        sensor_data = df[df['sensor_type'] == sensor_type]
        print(f"\n=== {sensor_type} Overview ===")
        print(f"Count: {len(sensor_data)}")
        
        # Calculate frequency
        if len(sensor_data) > 1:
            time_diff = sensor_data['timestamp'].diff().mean()
            freq = 1 / time_diff if time_diff > 0 else 0
            print(f"Average Frequency: {freq:.2f} Hz")

    # Show chronological data
    print("\nChronological Data Sample (first 20 rows):")
    columns = ['timestamp', 'episode_index', 'sensor_type']
    
    # Add sensor-specific columns
    imu_columns = ['angular_velocity_x', 'angular_velocity_y', 'angular_velocity_z',
                  'linear_acceleration_x', 'linear_acceleration_y', 'linear_acceleration_z']
    hall_columns = ['hall_value']
    camera_columns = ['frame_index']
    
    # Sort by timestamp and show first 20 rows
    sorted_data = df.sort_values('timestamp')
    
    # For each row, show relevant columns based on sensor type
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.width', None)        # Don't wrap wide displays
    pd.set_option('display.float_format', lambda x: '%.9f' % x)  # Show full timestamp precision
    
    # camera_data = sorted_data[sorted_data['sensor_type'] == 'camera']
    
    # camera_data[columns + imu_columns + hall_columns + camera_columns].to_csv('output/camera_data.csv', index=False)
    
    # print(camera_data[columns + imu_columns + hall_columns + camera_columns].head(20).to_string())
    
    print(sorted_data[columns + imu_columns + hall_columns + camera_columns].head(20).to_string())

    # Show value ranges
    print("\nValue Ranges:")
    print("\nIMU Data:")
    for col in imu_columns:
        imu_data = df[df['sensor_type'] == 'imu']
        min_val = imu_data[col].min()
        max_val = imu_data[col].max()
        print(f"{col}: {min_val:.3f} to {max_val:.3f}")
    
    print("\nHall Sensor Data:")
    hall_data = df[df['sensor_type'] == 'hall_sensor']
    min_val = hall_data['hall_value'].min()
    max_val = hall_data['hall_value'].max()
    print(f"hall_value: {min_val:.3f} to {max_val:.3f}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python inspect_parquet.py /path/to/sensor_data.parquet")
        sys.exit(1)

    parquet_file_path = Path(sys.argv[1])
    
    if not parquet_file_path.exists():
        print(f"Error: File not found: {parquet_file_path}")
        sys.exit(1)

    try:
        # Read the Parquet file
        print(f"Reading Parquet file: {parquet_file_path}")
        table = pq.read_table(parquet_file_path)
        df = table.to_pandas()

        # Basic information
        print("\nDataset Overview:")
        print(f"Total rows: {len(df)}")
        print(f"Time range: {df['timestamp'].min():.3f} to {df['timestamp'].max():.3f}")
        print(f"Duration: {df['timestamp'].max() - df['timestamp'].min():.2f} seconds")
        
        # Analyze sensor data
        analyze_sensor_data(df)

        # Show column names and their non-null counts
        print("\nColumn Overview:")
        print(df.info(show_counts=True))

    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        raise


if __name__ == "__main__":
    main()