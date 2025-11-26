# check_db.py
import os

import boto3
import json


# --- CONFIGURATION ---
# Force boto3 to use the specific profile from ~/.aws/credentials
os.environ['AWS_PROFILE'] = 'asense-iot'
# If necessary, force the region too (optional, depends on your ~/.aws/config)
# os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

def check_table(table_name):
    client = boto3.client('dynamodb')
    try:
        response = client.describe_table(TableName=table_name)
        table = response['Table']
        print(f"✅ Table '{table_name}' exists.")

        # Check GSIs
        gsis = table.get('GlobalSecondaryIndexes', [])
        gsi_names = [g['IndexName'] for g in gsis]

        if 'id-seq-index' in gsi_names:
            print(f"✅ GSI 'id-seq-index' found.")
        else:
            print(f"❌ GSI 'id-seq-index' NOT found. Found: {gsi_names}")

    except client.exceptions.ResourceNotFoundException:
        print(f"❌ Table '{table_name}' does not exist.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    check_table("asense_table_gyr")