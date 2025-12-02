import sys
import os
import boto3
from boto3.dynamodb.conditions import Key

# AWS Config
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

# Path setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import access

# --- CONFIG ---
TABLE_NAME = "asense_table_data"
ID_VALUE = "ASENSE00000022"
START_TIME = 1764612000000
END_TIME = 1764633600000


def test_raw_query_no_limit():
    print(f"\n--- TEST 1: Raw Query (No Limit param) ---")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    kc = Key('id').eq(ID_VALUE) & Key('time').between(START_TIME, END_TIME)

    try:
        # Simple query, letting Boto3 handle default limits
        resp = table.query(KeyConditionExpression=kc)
        items = resp.get('Items', [])
        print(f"✅ Found {len(items)} items.")
        if len(items) > 0:
            print(f"   First Item Time: {items[0]['time']}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_access_paginated():
    print(f"\n--- TEST 2: access.query_paginated (New Logic) ---")
    try:
        # Force a reasonable limit
        items, next_ts = access.query_paginated(TABLE_NAME, ID_VALUE, START_TIME, END_TIME, limit=2048)
        print(f"✅ Found {len(items)} items.")
        if len(items) > 0:
            print(f"   First Item Time: {items[0]['time']}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_scan_fallback():
    print(f"\n--- TEST 3: Scan Fallback (Sanity Check) ---")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    # Scan for just that ID (Expensive, but proves existence)
    fe = Key('id').eq(ID_VALUE) & Key('time').between(START_TIME, END_TIME)

    try:
        resp = table.scan(FilterExpression=fe, Limit=100)
        items = resp.get('Items', [])
        print(f"✅ Found {len(items)} items via SCAN.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    test_raw_query_no_limit()
    test_access_paginated()
    test_scan_fallback()