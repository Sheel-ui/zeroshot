from google.cloud import storage
from google.oauth2 import service_account

def upload_mp4_to_gcs(video_path, destination_blob_name):
    bucket_name = 'default00'
    key_path = 'secret.json'
    credentials = service_account.Credentials.from_service_account_file(key_path)
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    with open(video_path, "rb") as video_file:
        video_data = video_file.read()
        blob.upload_from_string(video_data, content_type='video/mp4')

    print(f"Video uploaded to {destination_blob_name} in bucket {bucket_name}.")

    print(f"File uploaded to {destination_blob_name} in bucket {bucket_name}.")


# destination_blob_name = 'test/video.mp4'

# video_path = 'output/combined_video_left_right.mp4'

# upload_mp4_to_gcs(video_path, destination_blob_name)
