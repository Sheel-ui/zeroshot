import subprocess

def adjust_to_30hz(video_path, output_path):
    try:
        cmd = [
            "ffmpeg", "-i", video_path,
            "-filter:v", "fps=fps=30",
            output_path
        ]
        subprocess.run(cmd, check=True)
        print(f"Video adjusted to 30 Hz and saved to {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error adjusting video to 30 Hz: {e}")

# adjust_to_30hz("input_video1.mp4", "output_video1_30hz.mp4")
# adjust_to_30hz("input_video2.mp4", "output_video2_30hz.mp4")
