import sys
import os
import json
import time

# AWS Config
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_function import lambda_handler

# --- CONFIGURATION ---
TEST_TABLE_VALID = "data"  # Should work
TEST_TABLE_INVALID = "acc"  # Should fail
TEST_ID = "ASENSE00000022"  # Known good ID from previous tests
START_TIME = 1759276800000  # Window covering known data
END_TIME = 1767225600000


def log(msg, status="INFO"):
    print(f"[{status}] {msg}")


def test_timestamps_only_valid():
    log(f"--- 1. Testing Valid Request ({TEST_TABLE_VALID}) ---", "TEST")

    event = {
        'queryStringParameters': {
            'table_name': TEST_TABLE_VALID,
            'id': TEST_ID,
            'start_time': str(START_TIME),
            'end_time': str(END_TIME),
            'timestamps_only': 'true'
        }
    }

    resp = lambda_handler(event, None)

    if resp['statusCode'] != 200:
        log(f"Failed with status {resp['statusCode']}: {resp['body']}", "FAIL")
        return

    body = json.loads(resp['body'])
    ts_list = body.get('timestamps', [])
    count = body.get('count', 0)

    log(f"Returned {count} timestamps.")

    if len(ts_list) != count:
        log(f"Count mismatch! Body says {count}, list has {len(ts_list)}", "FAIL")
    elif count > 0:
        log(f"First timestamp: {ts_list[0]}", "INFO")
        log(f"Last timestamp:  {ts_list[-1]}", "INFO")

        # Check if they are actually timestamps (integers)
        if isinstance(ts_list[0], int):
            log("Items are Integers.", "PASS")
        else:
            log(f"Items are {type(ts_list[0])} (Expected int).", "FAIL")

        # Check if sorted? (DynamoDB usually returns sorted by SK)
        if all(ts_list[i] <= ts_list[i + 1] for i in range(len(ts_list) - 1)):
            log("Timestamps are sorted.", "PASS")
        else:
            log("Timestamps are NOT sorted.", "WARN")
    else:
        log("No timestamps returned (Window empty?)", "WARN")


def test_timestamps_only_invalid_table():
    log(f"\n--- 2. Testing Invalid Table ({TEST_TABLE_INVALID}) ---", "TEST")

    event = {
        'queryStringParameters': {
            'table_name': TEST_TABLE_INVALID,
            'id': TEST_ID,
            'start_time': str(START_TIME),
            'end_time': str(END_TIME),
            'timestamps_only': 'true'
        }
    }

    resp = lambda_handler(event, None)

    if resp['statusCode'] == 400:
        log("Got 400 Bad Request as expected.", "PASS")
        log(f"Error msg: {resp['body']}", "INFO")
    else:
        log(f"Expected 400, got {resp['statusCode']}.", "FAIL")


def test_timestamps_only_pagination_ignore():
    log("\n--- 3. Testing Pagination Ignored (No Limit) ---", "TEST")
    # To test this effectively without huge data, we just verify that we got ALL data
    # compared to a standard query which splits it (if data > 2048).
    # Since we can't easily force >2048 items locally without huge data,
    # we verify that 'next_timestamp' is NOT present in the response.

    event = {
        'queryStringParameters': {
            'table_name': TEST_TABLE_VALID,
            'id': TEST_ID,
            'start_time': str(START_TIME),
            'end_time': str(END_TIME),
            'timestamps_only': 'true'
        }
    }

    resp = lambda_handler(event, None)
    body = json.loads(resp['body'])

    if 'next_timestamp' not in body:
        log("'next_timestamp' absent (Correct, infinite limit).", "PASS")
    else:
        log("'next_timestamp' present (Should be infinite fetch!).", "FAIL")


if __name__ == "__main__":
    test_timestamps_only_valid()
    test_timestamps_only_invalid_table()
    test_timestamps_only_pagination_ignore()