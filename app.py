from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

VIDEO_LEN = 6
DATA_PATH = 'data/20250116_021605'

def clean_df(df):
    columns = ['timestamp', 'episode_index', 'sensor_type']
    camera_columns = ['frame_index']
    
    # Sort by timestamp and filter camera data
    sorted_data = df.sort_values('timestamp')
    camera_data = sorted_data[sorted_data['sensor_type'] == 'camera'].copy()  # Explicitly create a copy

    # Cast 'frame_index' to int
    camera_data['frame_index'] = camera_data['frame_index'].astype(int)
    
    # Convert DataFrame to list
    camera_data_list = camera_data[columns + camera_columns].to_dict(orient='records')
    
    return camera_data_list

def increment_idx(df1, df2):
    # TODO: check earlier frame too
    idx=0
    while df1[idx]['timestamp']<df2[0]['timestamp']:
        idx+=1
        
    return idx-1

def find_anchor(left_df,right_df):
    left_df.to_csv('output/camera_data_left.csv', index=False)
    right_df.to_csv('output/camera_data_right.csv', index=False)
    anchor = (0,0)
    left_df.to_csv
    left_df = clean_df(left_df)
    right_df = clean_df(right_df)
    
    if left_df[0]['timestamp']<right_df[0]['timestamp']:
        anchor = (increment_idx(left_df,right_df),0)
    elif left_df[0]['timestamp']<right_df[0]['timestamp']:
        anchor = (0,increment_idx(left_df,right_df))
        
    # if left_df[left_idx][] 
    print(left_df[anchor[0]],right_df[anchor[1]])
    return anchor


def main():
    parquet_file_path = Path(DATA_PATH)
    if not parquet_file_path.exists():
        print(f"Error: File not found: {parquet_file_path}")
        return

    for i in range(1,VIDEO_LEN+1):
        try:
            print(f"Reading Parquet file: {parquet_file_path}")
            left_table = pq.read_table(f"{parquet_file_path}/{str(i)}/left/sensor_data.parquet")
            left_df = left_table.to_pandas()
            
            right_table = pq.read_table(f"{parquet_file_path}/{str(i)}/right/sensor_data.parquet")
            right_df = right_table.to_pandas()
            
            
            # STEP 1: Find anchor frame
            anchor = find_anchor(left_df,right_df)
            print(anchor)
            
            # STEP 2: Modify Parquet
            
            # STEP 3: Modify info
            
            # STEP 4: reconstruct the video
            
            # STEP 5: Upload to GC & Send message to a queue
            
            # STEP 6: Function app that listens to the queue and processes the video and uploads it to the GC
            
            
            
            # print("\nDataset Overview:")
            # print(f"Total rows: {len(df)}")
            # print(f"Time range: {df['timestamp'].min():.3f} to {df['timestamp'].max():.3f}")
            # print(f"Duration: {df['timestamp'].max() - df['timestamp'].min():.2f} seconds")
            
            # analyze_sensor_data(df)

            # print("\nColumn Overview:")
            # print(df.info(show_counts=True))

        except Exception as e:
            print(f"Error reading Parquet file: {e}")
            return

if __name__ == "__main__":
    main()