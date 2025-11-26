import sys
import os
import json

# --- CRITICAL AWS CONFIGURATION ---
# This must run BEFORE importing lambda_function/db.access
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

# Fix path to allow importing lambda_function from parent dir
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_function import lambda_handler
import tests.config as config
import tests.time_utils as time_utils
import tests.remote_client as remote_client
import tests.comparator as comparator

# ================= TEST CASES =================
TEST_CASES = [
    {
        "name": "Gyr (Merged)",
        "topic": "gyr",
        "start": "2025-11-25 16:00:00",
        "end": "2025-11-25 17:00:00",
        "merge": "true"
    },
    {
        "name": "Gyr (Not Merged)",
        "topic": "gyr",
        "start": "2025-11-25 16:39:00",
        "end": "2025-11-25 16:40:00",
        "merge": "false"
    },
    {
        "name": "FFT (Merged)",
        "topic": "fft",
        "start": "2025-11-23 12:00:00",
        "end": "2025-11-23 13:00:00",
        "merge": "true"
    },
    {
        "name": "Data (Not Merged)",
        "topic": "data",
        # Note: Ensure these times match data available in your NEW tables
        "start": "2025-08-29 23:00:00",
        "end": "2025-08-29 23:05:00",
        "merge": "false"
    }
]


# ==============================================

def run_single_test(case):
    print(f"\n=== RUNNING TEST: {case['name']} ===")

    # 1. Prepare Times
    start_ms = time_utils.local_datetime_to_unix_milliseconds(case['start'])
    end_ms = time_utils.local_datetime_to_unix_milliseconds(case['end'])

    print(f"   Time Window: {case['start']} -> {case['end']}")
    print(f"   Unix MS:     {start_ms} -> {end_ms}")

    # 2. Fetch Remote (Ground Truth)
    remote_json = remote_client.fetch_ground_truth(
        case['topic'], start_ms, end_ms, config.ASENSE_ID, '0', case['merge']
    )

    if remote_json is None:
        print("   [SKIP] Could not fetch remote data. Check API Key or Network.")
        return

    # 3. Run Local Lambda
    event = {
        'queryStringParameters': {
            'table_name': case['topic'],
            'id': config.ASENSE_ID,
            'start_time': str(start_ms),
            'end_time': str(end_ms),
            'minute': '0',
            'merge_by_hour': case['merge'],
            'output_format': 'map'  # Force legacy default for comparison
        }
    }

    try:
        print(f"   [Local]  Invoking Lambda...")
        local_resp = lambda_handler(event, None)

        if local_resp['statusCode'] != 200:
            print(f"   [FAIL] Local Lambda Error: {local_resp['body']}")
            return

        local_json = json.loads(local_resp['body'])

    except Exception as e:
        print(f"   [FAIL] Local Execution Exception: {str(e)}")
        return

    # 4. Compare
    print(f"   [Compare] Comparing {len(local_json)} local items vs {len(remote_json)} remote items...")

    comparison = comparator.deep_compare(local_json, remote_json)

    if comparison.is_success():
        print("   ✅ SUCCESS: Local Output matches Remote API exactly.")
    else:
        print(f"   ❌ FAILED: Found {len(comparison.errors)} mismatches.")
        # Print first 10 errors to avoid console spam
        for err in comparison.errors[:10]:
            print(f"      - {err}")
        if len(comparison.errors) > 10:
            print(f"      ... and {len(comparison.errors) - 10} more.")


if __name__ == "__main__":
    if not config.API_KEY:
        print("⚠️  WARNING: API_KEY is missing in tests/config.py")

    for case in TEST_CASES:
        run_single_test(case)