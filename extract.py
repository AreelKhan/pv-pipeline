import boto3
import os
import json

aws_secrets = json.load(open("secrets.json"))

# S3 bucket and path
BUCKET_NAME = 'oedi-data-lake'
PREFIX = 'pvdaq/parquet/metrics/'

# Local directory to save the downloaded files
local_directory = 'data/'

# Create an S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_secrets["aws_access_key_id"],
    aws_secret_access_key=aws_secrets["aws_secret_key_id"],
    region_name=aws_secrets["region_name"]
    )


# List objects in the specified S3 path
objects = s3.list_objects(Bucket=BUCKET_NAME, Prefix=PREFIX, Delimiter="/")

print(type(objects))

"""
# Download each object
for obj in objects:
    key = obj['Key']
    local_file_path = os.path.join(local_directory, os.path.basename(key))
    s3.download_file(bucket_name, key, local_file_path)
    print(f'Downloaded: {key} to {local_file_path}')
"""