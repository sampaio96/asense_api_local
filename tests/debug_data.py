import os
import sys
import json
from datetime import datetime

# AWS Config
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

sys.path.append(os.getcwd())
from db import access

# CONFIG (Data #16 Scenario)
TABLE_NAME = "asense_table_data"
ID_VALUE = "ASENSE00000016"
START_TIME = 1763856000000  # 2025-11-23 00:00:00
END_TIME = 1763942399000  # 2025-11-23 23:59:59

print(f"--- DEBUG: Querying {TABLE_NAME} ---")
print(f"ID: {ID_VALUE}")
print(f"Range: {START_TIME} -> {END_TIME}")

try:
    items = access.query_standard(TABLE_NAME, ID_VALUE, START_TIME, END_TIME)
    print(f"\nResult Count: {len(items)}")

    if len(items) > 0:
        print("\nFirst Item Keys & Time:")
        first = items[0]
        print(f"Time: {first.get('time')}")
        print(f"Keys: {list(first.keys())}")

        if len(items) > 1:
            print("\nLast Item Keys & Time:")
            last = items[-1]
            print(f"Time: {last.get('time')}")
    else:
        print("No items found.")

except Exception as e:
    print(f"ERROR: {e}")