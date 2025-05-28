import os
import requests
import boto3
from dotenv import load_dotenv
from datetime import datetime

# Load credentials from .env
load_dotenv()
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_prefix = os.getenv("S3_PREFIX")  # e.g., bronze/epa-tri/florida/
region = os.getenv("AWS_DEFAULT_REGION")

# AWS S3 client
s3 = boto3.client("s3", region_name=region)

current_year = datetime.now().year

# Years you want to pull
years = list(range(1987, current_year))  # Adjust as needed
states = ["PR", "FL"]

for state in states:
    for year in years:
        print(f"\n📥 Downloading TRI for {state}, {year}")
        url = f"https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/{year}_{state}/csv"

        try:
            response = requests.get(url)
            response.raise_for_status()

            # File paths
            filename = f"TRI_{year}_{state}.csv"
            local_folder = f"tri_{state.lower()}_data"
            os.makedirs(local_folder, exist_ok=True)

            local_path = os.path.join(local_folder, filename)
            s3_key = f"{s3_prefix}{state.lower()}/{year}/{filename}"


            # Save locally
            with open(local_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Saved to {local_path}")

            # Upload to S3
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"🚀 Uploaded to s3://{bucket_name}/{s3_key}")

        except Exception as e:
            print(f"❌ Failed for {state} {year}: {e}")
