# s3_utils.py
import boto3


def get_new_files(s3_client, bucket, prefix, last_time):
    """List new files in S3 modified after last_time"""
    paginator = s3_client.get_paginator("list_objects_v2")
    new_files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"] > last_time:
                new_files.append((obj["Key"], obj["LastModified"]))
                if len(new_files) == LIMIT:
                    return new_files
    return new_files
