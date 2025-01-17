import cv2

def get_total_frames_using_cap_prop(video_path):
    video_capture = cv2.VideoCapture(video_path)
    
    if not video_capture.isOpened():
        print("Error: Could not open video.")
        return
    
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    
    video_capture.release()
    return total_frames

def get_total_frames_by_iterating(video_path):
    video_capture = cv2.VideoCapture(video_path)
    
    if not video_capture.isOpened():
        print("Error: Could not open video.")
        return
    
    frame_count = 0
    while True:
        ret, frame = video_capture.read()
        if not ret:
            break
        frame_count += 1
    
    video_capture.release()
    return frame_count


def get_total_frames_using_seek(video_path):
    video_capture = cv2.VideoCapture(video_path)
    
    if not video_capture.isOpened():
        print("Error: Could not open video.")
        return

    video_capture.set(cv2.CAP_PROP_POS_FRAMES, 0)
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    
    video_capture.release()
    return total_frames


import subprocess
import json

import subprocess
import json

def get_total_frames_using_ffprobe(video_path):
    cmd = (
        f"ffmpeg -i {video_path} -map 0:v:0 -c copy -f null - 2>&1 | "
        "grep 'frame=' | tail -n 1"
    )

    try:
        # Run the command using shell=True to enable piping
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        output = result.stdout.decode('utf-8')
        
        # Extract the number of frames from the output (assuming 'frame=' appears as 'frame=xxxx')
        frame_count = int(output.split('frame=')[1].strip().split(' ')[0])
        
        return frame_count
    except subprocess.CalledProcessError as e:
        print(f"Error with ffprobe: {e}")
        return None

# # Example usage
# video_path = 'output_video.mp4'

# print(get_total_frames_using_cap_prop(video_path))
# print(get_total_frames_by_iterating(video_path))
# print(get_total_frames_using_seek(video_path))
# print(get_total_frames_using_ffprobe(video_path))
# extract_frames_from_video(video_path, output_video_path, start_frame, end_frame)
