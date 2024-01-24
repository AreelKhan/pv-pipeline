import boto3
import os
import json

print(json.load(open("secrets.json")))

# S3 bucket and path
BUCKET_NAME = 'oedi-data-lake'
PREFIX = 'pvdaq/inverters/'

# Local directory to save the downloaded files
local_directory = '/path/to/local/directory/'


"""
# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

# List objects in the specified S3 path
objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)['Contents']

# Download each object
for obj in objects:
    key = obj['Key']
    local_file_path = os.path.join(local_directory, os.path.basename(key))
    s3.download_file(bucket_name, key, local_file_path)
    print(f'Downloaded: {key} to {local_file_path}')
"""