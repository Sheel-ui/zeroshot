import subprocess
import json

def adjust_video_duration(video_path, output_path, num_frames):
    frames_per_second = 30
    target_duration = (num_frames - 1) / frames_per_second
    probe_cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0", "-show_entries", "stream=nb_frames,duration",
        "-of", "json", video_path
    ]
    result = subprocess.run(probe_cmd, stdout=subprocess.PIPE, text=True, check=True)
    probe_data = json.loads(result.stdout)
    
    nb_frames = int(probe_data['streams'][0]['nb_frames'])
    original_duration = float(probe_data['streams'][0]['duration'])

    target_fps = nb_frames / target_duration

    adjust_cmd = [
        "ffmpeg", "-i", video_path,
        "-filter:v", f"fps=fps={target_fps}",
        output_path
    ]
    subprocess.run(adjust_cmd, check=True)
    print(f"Video adjusted and saved to {output_path}")

# num_frames = 1224
# adjust_video_duration("temp/left.mp4", "temp/left_adjusted.mp4", num_frames)
# adjust_video_duration("temp/right.mp4", "temp/right_adjusted.mp4", num_frames)
