from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
import traceback
import cv2

VIDEO_LEN = 1
DATA_PATH = 'data/20250116_021605'
def skip_frames_and_save_new_video(input_video_path, output_video_path, skip_count=100):
    # Open the input video file
    video_capture = cv2.VideoCapture(input_video_path)
    
    if not video_capture.isOpened():
        print("Error: Could not open video.")
        return
    
    # Get the video's frame rate (fps) and frame size
    fps = video_capture.get(cv2.CAP_PROP_FPS)
    frame_width = int(video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    
    # Create a VideoWriter object to save the new video
    fourcc = cv2.VideoWriter_fourcc(*'XVID')  # You can change this codec if needed
    video_writer = cv2.VideoWriter(output_video_path, fourcc, fps, (frame_width, frame_height))
    
    # Skip the first 100 frames
    video_capture.set(cv2.CAP_PROP_POS_FRAMES, skip_count)
    
    while True:
        # Read the next frame
        ret, frame = video_capture.read()
        
        if not ret:
            break  # End of video
        
        # Write the frame to the new video
        video_writer.write(frame)
    
    # Release resources
    video_capture.release()
    video_writer.release()
    print(f"New video saved as {output_video_path}")

def get_total_frames(video_path):
    # Open the video file
    video_capture = cv2.VideoCapture(video_path)
    
    if not video_capture.isOpened():
        print("Error: Could not open video.")
        return
    
    # Get the total number of frames
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    
    video_capture.release()
    return total_frames

def clean_df(df,name):
    columns = ['timestamp', 'episode_index', 'sensor_type']
    camera_columns = ['frame_index']
    
    # Sort by timestamp and filter camera data
    sorted_data = df.sort_values('timestamp')
    camera_data = sorted_data[sorted_data['sensor_type'] == 'camera'].copy()  # Explicitly create a copy

    # Cast 'frame_index' to int
    camera_data['frame_index'] = camera_data['frame_index'].astype(int)
    
    camera_data.to_csv(f"output/camera_data_{name}.csv", index=False)
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

    anchor = (0,0)
    left_df.to_csv
    left_df = clean_df(left_df,"left")
    right_df = clean_df(right_df,"right")
    
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
            
            
            video_paths = ['data/20250116_021605/1/ego/camera_video.mp4', 'data/20250116_021605/1/left/camera_video.mp4', 'data/20250116_021605/1/right/camera_video.mp4']
            output_path = 'combined_video.mp4'
            output_size = (1280, 720)
            # combine_videos_with_positions(video_paths, output_path, output_size)
            # STEP 2: Modify Parquet
            
            # STEP 3a: Modify info
            
            # STEP 3b: skip frames of video based on anchor
            video_path = 'data/20250116_021605/1/right/camera_video.mp4'
            output_path = 'output_video.mp4'
            skip_frames_and_save_new_video(video_path,output_path)
            print(get_total_frames(video_path))
            # STEP 3c: Accuracte framepacing for videos
            
            
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
            print(traceback.format_exc())
            return

if __name__ == "__main__":
    main()