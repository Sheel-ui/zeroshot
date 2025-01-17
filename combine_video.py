import subprocess

def combine_videos_horizontally(input_video_path_1, input_video_path_2, output_video_path):
    # Construct the ffmpeg command to combine two videos horizontally
    cmd = [
        "ffmpeg",
        "-i", input_video_path_1,
        "-i", input_video_path_2,
        "-filter_complex", "hstack=2",
        output_video_path
    ]

    try:
        # Execute the ffmpeg command
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        
        # Output the result of the command
        print(f"Command executed successfully. Output saved to {output_video_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error with ffmpeg: {e}")
        print(f"stderr: {e.stderr.decode()}")
        print(f"stdout: {e.stdout.decode()}")

# # Usage Example:
# input_video_path_1 = 'data/20250116_021605/1/left/camera_video.mp4'
# input_video_path_2 = 'data/20250116_021605/1/right/camera_video.mp4'
# output_video_path = 'combined_video_left_right.mp4'

# combine_videos_horizontally(input_video_path_1, input_video_path_2, output_video_path)
