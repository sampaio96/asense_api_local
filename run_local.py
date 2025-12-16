import json
import os

# --- CRITICAL CONFIGURATION ---
# Set the profile BEFORE importing the lambda_function.
# This ensures db.access initializes boto3 with the correct credentials.
os.environ['AWS_PROFILE'] = 'asense-iot'

# Optional: Force region if your profile config doesn't specify it.
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

from lambda_function import lambda_handler

# --- CONFIGURATION ---
TEST_TOPIC = 'acc'
TEST_ID = 'ASENSE00000022'
TEST_START = '1764201600000'
TEST_END = '1764468000000'
MERGE = 'true'

# Options: 'map', 'tuple_array', 'dict_array', 'combined_tuple', 'combined_dict'
FORMAT = 'tuple_array'

event = {
    'queryStringParameters': {
        'table_name': TEST_TOPIC,
        'id': TEST_ID,
        'start_time': TEST_START,
        'end_time': TEST_END,
        # 'minute': '0',
        'merge': MERGE,
        'output_format': FORMAT  # <--- NEW KEY
    }
}

print(f"--- Request {TEST_TOPIC} (Merge: {MERGE}, Format: {FORMAT}) ---")
response = lambda_handler(event, None)

print(f"Status Code: {response['statusCode']}")

if response['statusCode'] == 200:
    body = json.loads(response['body'])
    data = body.get('data', [])
    print(f"Items found: {len(data)}")
    if len(data) > 0:
        print("First Item Preview:")
        preview = data[0].copy()

        # Truncate large arrays for display
        for k, v in preview.items():
            if isinstance(v, list) and len(v) > 5:
                preview[k] = f"{v[:3]} ... ({len(v)} items)"

        print(json.dumps(preview, indent=2))
else:
    print(response['body'])