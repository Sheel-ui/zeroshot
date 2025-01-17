import cv2

def extract_video_frames(input_video_path, start_frame, include_frames, output_video_path):
    # Open the video
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

    # Set the starting frame
    video_capture.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    frame_count = start_frame
    while True:
        ret, frame = video_capture.read()

        if not ret:
            print("End of video reached.")
            break  # End of video

        if frame_count > include_frames:
            break  # Stop once we've reached the end frame

        # Write the frame to the new video
        video_writer.write(frame)
        frame_count += 1

    # Release resources
    video_capture.release()
    video_writer.release()
    print(f"Video extracted from frame {start_frame} to frame {include_frames} and saved as {output_video_path}.")

# # Usage Example:
# input_video_path = 'data/20250116_021605/1/right/camera_video.mp4'
# output_video_path = 'output_video.mp4'
# start_frame = 100  # Example starting frame (inclusive)
# include_frames = 1172  # Number of frames to include

# extract_video_frames(input_video_path, start_frame, include_frames, output_video_path)
