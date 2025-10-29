# S3Ops.py
# Not using yet, but skeleton for later

import boto3
import logging
import os
from config import Config

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
        region_name=Config.AWS_REGION
    )

def upload_to_s3(file_path, country, docket_id, doc_id):
    s3_client = get_s3_client()
    ext = os.path.splitext(file_path)[1].lower()
    s3_key = Config.S3_FOLDER_STRUCTURE.format(country=country, docket_id=docket_id, doc_id=doc_id, ext=ext[1:])
    
    try:
        s3_client.upload_file(file_path, Config.S3_BUCKET_NAME, s3_key)
        s3_link = f"https://{Config.S3_BUCKET_NAME}.s3.{Config.AWS_REGION}.amazonaws.com/{s3_key}"
        return s3_key, s3_link
    except Exception as e:
        logging.error(f"S3 upload failed: {e}")
        return None, None