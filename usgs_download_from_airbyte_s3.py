# python3 usgs_download_from_airbyte_s3.py
# This script downloads the USGS topo data from the Airbyte S3 bucket and uploads it to the USGS topo data bucket
# It also logs the keys of the files that have been processed to a file called processed_keys.txt
# It then uses the processed_keys.txt file to skip files that have already been processed
# It also uses the processed_keys.txt file to skip files that have already been uploaded to the USGS topo data bucket
# It also uses the processed_keys.txt file to skip files that have already been downloaded from the Airbyte S3 bucket
# It also uses the processed_keys.txt file to skip files that have already been uploaded to the USGS topo data bucket

import boto3
import json
import requests
from urllib.parse import urlparse
from io import BytesIO
import os
from dotenv import load_dotenv
import sys
import csv

csv.field_size_limit(sys.maxsize)

load_dotenv()
processed_log = "processed_keys.txt"


# Config
source_bucket = os.getenv("S3_BUCKET_NAME")
# bronze_prefix = "bronze/airbyte/miami-dade/" # change this to the correct prefix from which to grab the data
download_prefix = "bronze/airbyte/usgs-topos/"
region = os.getenv("AWS_DEFAULT_REGION")

s3 = boto3.client("s3", region_name=region)

def list_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def has_been_processed(key):
    if not os.path.exists(processed_log):
        return False
    with open(processed_log) as f:
        return key in f.read()

def mark_as_processed(key):
    with open(processed_log, "a") as f:
        f.write(key + "\n")


def already_uploaded(s3_key):
    try:
        s3.head_object(Bucket=source_bucket, Key=s3_key)
        return True
    except:
        return False

def process_json_file(obj_key):
    print(f"🔍 Reading: {obj_key}")
    response = s3.get_object(Bucket=source_bucket, Key=obj_key)
    lines = response["Body"].read().decode("utf-8").splitlines()

    for line in lines:
        try:
            record = json.loads(line)
            items = record.get("_airbyte_data", {}).get("items", [])
            print(f"Found {len(items)} items in record")

            for item in items:
                url = item.get("downloadURL")
                if not url:
                    continue

                filename = os.path.basename(urlparse(url).path)
                source_folder = obj_key.split('/')[2]  # e.g., 'miami-dade' or 'puerto-rico'
                s3_key = f"{download_prefix}{source_folder}/{filename}"

                if already_uploaded(s3_key):
                    print(f"✅ Already exists: {filename}")
                    continue

                print(f"⬇️ Downloading: {filename}")
                r = requests.get(url, stream=True)
                r.raise_for_status()

                s3.upload_fileobj(BytesIO(r.content), source_bucket, s3_key)
                print(f"🚀 Uploaded to s3://{source_bucket}/{s3_key}")

        except Exception as e:
            print(f"⚠️ Failed to process line: {e}")
            continue

def process_csv_file(obj_key):
    print(f"🔍 Reading CSV: {obj_key}")
    response = s3.get_object(Bucket=source_bucket, Key=obj_key)
    body = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(body.splitlines())
    for row in reader:
        url = row.get("downloadURL") or row.get("download_url")
        if not url:
            continue

        filename = os.path.basename(urlparse(url).path)
        source_folder = obj_key.split('/')[1]  # e.g., 'miami-dade' or 'puerto-rico'
        s3_key = f"{download_prefix}{source_folder}/{filename}"

        if already_uploaded(s3_key):
            print(f"✅ Already exists: {filename}")
            continue

        print(f"⬇️ Downloading: {filename}")
        r = requests.get(url, stream=True)
        r.raise_for_status()

        s3.upload_fileobj(BytesIO(r.content), source_bucket, s3_key)
        print(f"🚀 Uploaded to s3://{source_bucket}/{s3_key}")


def main():
    prefixes = ["bronze/airbyte/miami-dade/", "bronze/airbyte/puerto-rico/"]

    for prefix in prefixes:
        print(f"🔍 Listing objects in s3://{source_bucket}/{prefix}")
        for key in list_objects(source_bucket, prefix):
            if not (key.endswith(".jsonl") or key.endswith(".ndjson")):
                continue
            print(f"🔍 Processing: {key}")
            if has_been_processed(key):
                continue
            if key.endswith(".csv"):
                process_csv_file(key)
            else:
                process_json_file(key)
            mark_as_processed(key)


if __name__ == "__main__":
    main()
