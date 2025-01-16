import cv2

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
        
        # Optionally show the frame (for debugging)
        cv2.imshow('Frame', frame)
        
        # Wait a short time and check if 'q' is pressed to exit early (optional)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    
    # Release resources
    video_capture.release()
    video_writer.release()
    cv2.destroyAllWindows()
    print(f"New video saved as {output_video_path} with {frame_count - skip_count} frames.")

# Example usage
video_path = 'data/20250116_021605/1/right/camera_video.mp4'
output_path = 'output_video.mp4'
skip_frames_and_save_new_video(video_path, output_path, 100)
