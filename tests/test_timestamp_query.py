import sys
import os
import json

os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import access
from lambda_function import lambda_handler

# --- CONFIGURATION (Based on your successful run) ---
TEST_TABLE = "asense_table_acc"
TEST_ID = "ASENSE00000022"
# Use a window that we KNOW has data from previous logs
START_TIME = 1763725266495
# Make window large enough to cover at least 20 items
END_TIME = 1763728200000


def log(msg, status="INFO"):
    print(f"[{status}] {msg}")


def test_pagination_strictness():
    log("--- 1. Testing Pagination Strictness (+1 Logic) ---", "TEST")

    items_1, next_ts_1 = access.query_paginated(TEST_TABLE, TEST_ID, START_TIME, END_TIME, limit=10)

    if not items_1:
        log("Page 1 empty.", "SKIP")
        return

    t_last_1 = items_1[-1]['time']
    log(f"Page 1 Last Time: {t_last_1}")
    log(f"Next Token:       {next_ts_1}")

    if next_ts_1 == t_last_1 + 1:
        log("Token is exactly Last Time + 1.", "PASS")
    else:
        log(f"Token logic mismatch! {next_ts_1} != {t_last_1} + 1", "FAIL")

    # Fetch Page 2
    items_2, _ = access.query_paginated(TEST_TABLE, TEST_ID, next_ts_1, END_TIME, limit=10)
    if items_2:
        t_first_2 = items_2[0]['time']
        log(f"Page 2 First Time: {t_first_2}")
        if t_first_2 > t_last_1:
            log("Page 2 starts STRICTLY after Page 1.", "PASS")
        else:
            log("Page 2 overlaps Page 1!", "FAIL")


def test_merge_options_and_formats():
    log("\n--- 2. Testing Merge & Formats ---", "TEST")

    # We test True AND False
    merge_options = ['true', 'false']
    formats = ['map', 'tuple_array', 'dict_array']

    for m in merge_options:
        for fmt in formats:
            log(f"Testing Merge={m}, Format={fmt}...", "SUB")
            event = {
                'queryStringParameters': {
                    'table_name': 'acc',
                    'id': TEST_ID,
                    'start_time': str(START_TIME),
                    'end_time': str(END_TIME),  # Large enough window
                    'merge': m,
                    'output_format': fmt
                }
            }
            resp = lambda_handler(event, None)
            body = json.loads(resp['body'])
            data = body.get('data', [])

            if not data:
                log("No data returned.", "FAIL")
                continue

            item = data[0]

            # Check Sort Order (Monotonicity) if tuple_array and merge=true
            if m == 'true' and fmt == 'tuple_array' and 'acc_x' in item:
                acc_x = item['acc_x']
                timestamps = [p[0] for p in acc_x]
                if all(timestamps[i] <= timestamps[i + 1] for i in range(len(timestamps) - 1)):
                    log("  -> Vector Monotonicity: OK", "PASS")
                else:
                    log("  -> Vector Monotonicity: BROKEN", "FAIL")

            # Check Metadata Presence
            has_time = 'time' in item
            if m == 'true':
                if not has_time:
                    log("  -> Metadata 'time' removed (Correct).", "PASS")
                else:
                    log("  -> Metadata 'time' present (Should be removed!).", "FAIL")
            else:
                if has_time:
                    log("  -> Metadata 'time' present (Correct).", "PASS")
                else:
                    log("  -> Metadata 'time' missing (Should be present!).", "FAIL")


if __name__ == "__main__":
    test_pagination_strictness()
    test_merge_options_and_formats()