from pathlib import Path
import pyarrow.parquet as pq
from extract_video import extract_video_frames
from combine_video import combine_videos_horizontally
from upload_file import upload_mp4_to_gcs
import traceback
import cv2
import json

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
    
    # Read frames after skipping
    frame_count = skip_count
    while True:
        ret, frame = video_capture.read()
        
        if not ret:
            print("End of video reached.")
            break  # End of video
        
        # Write the frame to the new video
        video_writer.write(frame)
        frame_count += 1
    
    # Release resources
    video_capture.release()
    video_writer.release()
    print(f"New video saved as {output_video_path} with {frame_count - skip_count} frames.")

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
    # TODO: improved function with a window for mismatch, find earliest occurance
    idx=0
    while df1[idx]['timestamp']<df2[0]['timestamp']:
        idx+=1
        
    return idx-1

# def find_anchor(left_list, right_list, max_loss_ratio=0.1):
#     # allow skipping max 10% of the video to find the anchor
#     left_idx = 0
#     right_idx = 0
#     closest_diff = float('inf')
#     anchor = (0, 0)

#     while left_idx < len(left_list) and right_idx < len(right_list):
#         left_time = left_list[left_idx]['timestamp']
#         right_time = right_list[right_idx]['timestamp']

#         diff = abs(left_time - right_time)

#         # Check if this anchor is the best so far
#         if diff < closest_diff:
#             left_loss_ratio = left_idx / len(left_list)
#             right_loss_ratio = right_idx / len(right_list)

#             # Update anchor only if within allowed loss ratio
#             if max(left_loss_ratio, right_loss_ratio) <= max_loss_ratio:
#                 closest_diff = diff
#                 anchor = (left_idx, right_idx)

#         # Move pointers
#         if left_time < right_time:
#             left_idx += 1
#         else:
#             right_idx += 1

#     return anchor

# def find_anchor(left_list, right_list, range=150):
#     # two pointer: entire list first 150 frames
#     left_list = left_list[:range]
#     right_list = right_list[:range] 
#     left_idx = 0
#     right_idx = 0
#     closest_diff = float('inf')
#     anchor = (0, 0)

#     while left_idx < len(left_list) and right_idx < len(right_list):
#         left_time = left_list[left_idx]['timestamp']
#         right_time = right_list[right_idx]['timestamp']

#         diff = abs(left_time - right_time)

#         if diff < closest_diff:
#             closest_diff = diff
#             anchor = (left_idx, right_idx)

#         if left_time < right_time:
#             left_idx += 1
#         else:
#             right_idx += 1

#     return anchor

def find_anchor(left_list, right_list):
    # two pointer: entire list
    left_idx = 0
    right_idx = 0
    closest_diff = float('inf')
    anchor = (0, 0)

    while left_idx < len(left_list) and right_idx < len(right_list):
        left_time = left_list[left_idx]['timestamp']
        right_time = right_list[right_idx]['timestamp']

        diff = abs(left_time - right_time)

        if diff < closest_diff:
            closest_diff = diff
            anchor = (left_idx, right_idx)

        if left_time < right_time:
            left_idx += 1
        else:
            right_idx += 1

    return anchor


def main():
    parquet_file_path = Path(DATA_PATH)
    if not parquet_file_path.exists():
        print(f"Error: File not found: {parquet_file_path}")
        return

    # all the directories
    for i in range(1,VIDEO_LEN+1):
        try:
            # STEP 1: Read parquet files 
            print(f"Reading Parquet file under {parquet_file_path}/{str(i)}")
            
            left_path = f"{parquet_file_path}/{str(i)}/left/sensor_data.parquet"
            right_path = f"{parquet_file_path}/{str(i)}/right/sensor_data.parquet"
            
            left_table = pq.read_table(left_path)
            left_df = left_table.to_pandas()
            
            right_table = pq.read_table(right_path)
            right_df = right_table.to_pandas()
            
            
            # DEBUG: SAVE DFS IN CSV
            left_df.to_csv('output/camera_data_left.csv', index=False)
            right_df.to_csv('output/camera_data_right.csv', index=False)
            
            # STEP 2: Clean DF
            left_df = clean_df(left_df,"left")
            right_df = clean_df(right_df,"right")
            
            print(json.dumps(left_df[0:5],indent=4))
            print(json.dumps(right_df[0:5],indent=4))
            # #STEP 3: Find anchor frame idx
            # anchor = find_anchor(left_df,right_df)
            # print(anchor)
            
            # # STEP 4: Calculate start and end frames
            # left_frames = get_total_frames(f"{parquet_file_path}/{str(i)}/left/camera_video.mp4") - anchor[0]
            # right_frames = get_total_frames(f"{parquet_file_path}/{str(i)}/right/camera_video.mp4") - anchor[1]
            
            # min_frames = min(left_frames,right_frames)
            # frames_to_extract = {
            #     "left": (anchor[0],min_frames+anchor[0]),
            #     "right": (anchor[1],min_frames+anchor[1])
            # }

            # print(frames_to_extract)


            # # STEP 5: Generate temp video with relevant frames
            # # TODO: frame pacing
            # extract_video_frames(f"{parquet_file_path}/{str(i)}/left/camera_video.mp4",frames_to_extract["left"][0],frames_to_extract["left"][1],"temp/left.mp4")
            # extract_video_frames(f"{parquet_file_path}/{str(i)}/right/camera_video.mp4",frames_to_extract["right"][0],frames_to_extract["right"][1],"temp/right.mp4")
            
            # # TODO: STEP 6: Modify Parquet SKIPPED
            
            # # TODO:  STEP 7: Modify info SKIPPED
            
            # # STEP 8: reconstruct the video
            # combine_videos_horizontally("temp/left.mp4","temp/right.mp4",output_video_path="temp/output.mp4")
            
            # # STEP 9: UPLOAD VIDEO TO CLOUD
            # upload_mp4_to_gcs('temp/output.mp4', 'test/combined.mp4')
            
            # # ================ ALTERNATE FLOW ================
            
            # # TODO: STEP 9: Upload to GC & Send message to a queue
            
            # # TODO: STEP 10: Function app that listens to the queue and processes the video and uploads it to the GC

        except Exception as e:
            print(traceback.format_exc())
            print(f"Error reading Parquet file: {e}")

if __name__ == "__main__":
    main()