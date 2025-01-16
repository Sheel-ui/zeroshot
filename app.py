from utils.parquet_inspector import analyze_sensor_data
from pathlib import Path
import pyarrow.parquet as pq

VIDEO_LEN = 6
SENSORS = ["left","right"]
DATA_PATH = 'data/20250116_021605'

def main():
    parquet_file_path = Path(DATA_PATH)
    if not parquet_file_path.exists():
        print(f"Error: File not found: {parquet_file_path}")
        return


    for i in range(1,VIDEO_LEN+1):
        for sensor in SENSORS:    
            try:
                print(f"Reading Parquet file: {parquet_file_path}")
                table = pq.read_table(f"{parquet_file_path}/{str(i)}/{sensor}/sensor_data.parquet")
                df = table.to_pandas()

                print("\nDataset Overview:")
                print(f"Total rows: {len(df)}")
                print(f"Time range: {df['timestamp'].min():.3f} to {df['timestamp'].max():.3f}")
                print(f"Duration: {df['timestamp'].max() - df['timestamp'].min():.2f} seconds")
                
                analyze_sensor_data(df)

                print("\nColumn Overview:")
                print(df.info(show_counts=True))

            except Exception as e:
                print(f"Error reading Parquet file: {e}")
                return

if __name__ == "__main__":
    main()