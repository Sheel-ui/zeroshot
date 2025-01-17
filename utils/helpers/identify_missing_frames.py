def identify_skipped_frames(data, expected_interval=33.333, buffer=1.0):
    skipped_frames = []
    for i in range(1, len(data)):
        time_diff = (data[i]['timestamp'] - data[i-1]['timestamp']) * 1000
        if abs(time_diff - expected_interval) > buffer:
            skipped_frames.append(data[i]['frame_index'])

    return skipped_frames


# data = [
#     {"timestamp": 1737071404.7169514, "episode_index": 0, "sensor_type": "camera", "frame_index": 0},
#     {"timestamp": 1737071404.7503989, "episode_index": 0, "sensor_type": "camera", "frame_index": 1},
#     {"timestamp": 1737071404.7838383, "episode_index": 0, "sensor_type": "camera", "frame_index": 2},
#     {"timestamp": 1737071404.8172956, "episode_index": 0, "sensor_type": "camera", "frame_index": 3},
#     {"timestamp": 1737071404.8507373, "episode_index": 0, "sensor_type": "camera", "frame_index": 4}
# ]

# expected_interval = 33.333
# buffer = 10.0

# skipped_frames = identify_skipped_frames(data, expected_interval, buffer)
# print("Skipped frames:", skipped_frames)
