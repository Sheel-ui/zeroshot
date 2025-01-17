import cv2
import numpy as np

def add_black_frames(video_path, indices, output_path):
    indices = sorted(indices)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print("Error: Could not open video.")
        return

    # calc size and frames
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # new video
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    # Generate a black frame
    black_frame = np.zeros((height, width, 3), dtype=np.uint8)

    current_frame = 0
    shift = 0
    index_set = set(indices)

    while current_frame < total_frames:
        ret, frame = cap.read()
        if not ret:
            break

        # Check if we need to add a black frame before this frame
        if current_frame + shift in index_set:
            out.write(black_frame)
            shift += 1  # Increment shift because a black frame was added
        out.write(frame)
        current_frame += 1

    for remaining_index in indices:
        if remaining_index >= total_frames + shift:
            out.write(black_frame)

    cap.release()
    out.release()
    print(f"Video saved with black frames at {indices} to {output_path}")

# video_path = "data/20250116_021605/1/left/camera_video.mp4"
# output_path = "output/left_with_black_frames.mp4"
# indices = [10, 50, 100]
# add_black_frames(video_path, indices, output_path)
