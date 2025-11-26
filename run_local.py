import json
import os

# --- CRITICAL CONFIGURATION ---
# Set the profile BEFORE importing the lambda_function.
# This ensures db.access initializes boto3 with the correct credentials.
os.environ['AWS_PROFILE'] = 'asense-iot'

# Optional: Force region if your profile config doesn't specify it.
# os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

from lambda_function import lambda_handler

# --- CONFIGURATION ---
# Select which test to run by uncommenting lines below

# --- TEST 1: GYROSCOPE (Merged) ---
TEST_TOPIC = 'gyr'
TEST_ID = 'ASENSE00000005'
TEST_START = '1764086399000'
TEST_END = '1764089999000'  # Covers the 16 items you found
MERGE = 'true'
FORMAT_TUPLES = 'true'  # <--- Set to true to test

event = {
    'queryStringParameters': {
        'table_name': TEST_TOPIC,
        'id': TEST_ID,
        'start_time': TEST_START,
        'end_time': TEST_END,
        'minute': '0',
        'merge_by_hour': MERGE,
        'format_tuples': FORMAT_TUPLES # <--- Pass to lambda
    }
}

print(f"--- Request {TEST_TOPIC} (Merge: {MERGE}, Tuples: {FORMAT_TUPLES}) ---")
response = lambda_handler(event, None)

print(f"Status Code: {response['statusCode']}")

if response['statusCode'] == 200:
    body = json.loads(response['body'])
    print(f"Items found: {len(body)}")

    if len(body) > 0:
        print("First Item Preview:")
        preview = body[0].copy()

        # If checking gyr, preview gyr_x list
        if 'gyr_x' in preview:
            # Print first 3 tuples to verify structure [key, val]
            print(f"gyr_x (first 3): {preview['gyr_x'][:3]}")
            preview['gyr_x'] = "..."
        if 'gyr_y' in preview: preview['gyr_y'] = "..."
        if 'gyr_z' in preview: preview['gyr_z'] = "..."

        print(json.dumps(preview, indent=2))
else:
    print(response['body'])