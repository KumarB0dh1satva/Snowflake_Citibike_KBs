import requests
import xml.etree.ElementTree as ET
import pandas as pd
from urllib.parse import urlparse
import os

# Public Citi Bike S3 bucket
BUCKET_URL = "https://s3.amazonaws.com/tripdata/"

# Output CSV filename
OUTPUT_FILE = "citibike_s3_file_index.csv"

print("Fetching S3 bucket listing...")

response = requests.get(BUCKET_URL)

if response.status_code != 200:
    raise Exception(f"Failed to fetch bucket listing: {response.status_code}")

# Parse XML response
root = ET.fromstring(response.text)

# S3 XML namespace
namespace = {
    's3': 'http://s3.amazonaws.com/doc/2006-03-01/'
}

rows = []

print("Parsing file metadata...")

for content in root.findall('s3:Contents', namespace):

    key = content.find('s3:Key', namespace).text
    last_modified = content.find('s3:LastModified', namespace).text
    size = int(content.find('s3:Size', namespace).text)

    # Extract extension
    filename = os.path.basename(key)

    if "." in filename:
        file_format = filename.split(".")[-1].lower()
    else:
        file_format = "unknown"

    full_url = BUCKET_URL + key

    rows.append({
        "file_name": filename,
        "s3_key": key,
        "file_format": file_format,
        "file_size_bytes": size,
        "last_modified": last_modified,
        "file_url": full_url
    })

# Convert to DataFrame
df = pd.DataFrame(rows)

# Save locally as CSV
df.to_csv(OUTPUT_FILE, index=False)

print(f"\nCSV file created successfully: {OUTPUT_FILE}")
print(f"Total files indexed: {len(df)}")