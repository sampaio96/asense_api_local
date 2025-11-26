import sys
import os
import json

# --- AWS CONFIGURATION ---
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_function import lambda_handler
import tests.config as config
import tests.time_utils as time_utils
import tests.remote_client as remote_client
import tests.comparator as comparator

# ================= SCENARIO DEFINITIONS =================
# We define the scenarios here, and the runner generates
# both merge=true and merge=false tests for each.
SCENARIOS = [
    # --- ACCELEROMETER ---
    {
        "name": "Acc #16 (Nov 24) - Expect Success",
        "topic": "acc",
        "id": "ASENSE00000016",
        "start": "2025-11-24 00:00:00",
        "end": "2025-11-24 23:59:59",
        "expect_fail": False
    },
    {
        "name": "Acc #05 (Oct 22) - Expect Fail (Old Data)",
        "topic": "acc",
        "id": "ASENSE00000005",
        "start": "2025-10-22 00:00:00",
        "end": "2025-10-22 23:59:59",
        "expect_fail": True
    },

    # --- DATA (Scalar) ---
    {
        "name": "Data #16 (Nov 24) - Expect Success",
        "topic": "data",
        "id": "ASENSE00000016",
        "start": "2025-11-24 00:00:00",
        "end": "2025-11-24 23:59:59",
        "expect_fail": False
    },
    {
        "name": "Data #11 (Oct 27) - Expect Fail (Old Data)",
        "topic": "data",
        "id": "ASENSE00000011",
        "start": "2025-10-27 00:00:00",
        "end": "2025-10-27 23:59:59",
        "expect_fail": True
    },

    # --- FFT ---
    {
        "name": "FFT #17 (Nov 24-25) - Expect Success",
        "topic": "fft",
        "id": "ASENSE00000017",
        "start": "2025-11-24 00:00:00",
        "end": "2025-11-25 23:59:59",
        "expect_fail": False
    },
    {
        "name": "FFT #11 (Oct 22) - Expect Fail (Old Data)",
        "topic": "fft",
        "id": "ASENSE00000011",
        "start": "2025-10-22 00:00:00",
        "end": "2025-10-22 23:59:59",
        "expect_fail": True
    },

    # --- GYROSCOPE ---
    {
        "name": "Gyr #05 (Nov 23 10pm) - Expect Success",
        "topic": "gyr",
        "id": "ASENSE00000005",
        "start": "2025-11-23 22:00:00",
        "end": "2025-11-23 23:00:00",
        "expect_fail": False
    },
    {
        "name": "Gyr #05 (Oct 22) - Expect Fail (Old Data)",
        "topic": "gyr",
        "id": "ASENSE00000005",
        "start": "2025-10-22 00:00:00",
        "end": "2025-10-22 23:59:59",
        "expect_fail": True
    }
]


# ==============================================

def run_single_test(case, merge_flag):
    merge_str = "true" if merge_flag else "false"
    test_name_full = f"{case['name']} [Merge: {merge_str}]"

    print(f"\n{'=' * 80}")
    print(f"TEST: {test_name_full}")
    print(f"Topic: {case['topic'].upper()} | ID: {case['id']}")
    print(f"Window: {case['start']} -> {case['end']}")
    print(f"{'=' * 80}")

    # 1. Setup Times
    start_ms = time_utils.local_datetime_to_unix_milliseconds(case['start'])
    end_ms = time_utils.local_datetime_to_unix_milliseconds(case['end'])

    # 2. Fetch Remote
    remote_json = remote_client.fetch_ground_truth(
        case['topic'], start_ms, end_ms, case['id'], '0', merge_str
    )

    if remote_json is None:
        print("⛔ [SKIP] Remote fetch failed (Network/Auth).")
        return

    # 3. Fetch Local
    event = {
        'queryStringParameters': {
            'table_name': case['topic'],
            'id': case['id'],
            'start_time': str(start_ms),
            'end_time': str(end_ms),
            'minute': '0',
            'merge_by_hour': merge_str,
            'output_format': 'map'  # Enforce legacy format for comparison
        }
    }

    try:
        local_resp = lambda_handler(event, None)
        local_json = json.loads(local_resp['body'])
    except Exception as e:
        print(f"⛔ [FAIL] Local Exception: {str(e)}")
        return

    n_loc = len(local_json)
    n_rem = len(remote_json)
    print(f"\n--- COMPARISON ({n_loc} Local vs {n_rem} Remote) ---")

    # Check for the "Double Empty" condition immediately
    if n_loc == 0 and n_rem == 0:
        print("-" * 40)
        print("❌ TEST FAILED: Both Local and Remote returned 0 items.")
        print("   Reason: Data not found in either environment.")
        print("   (Test is invalid because we cannot verify logic without data)")
        print("-" * 40)
        return

    # 4. Deep Compare
    stats = comparator.deep_compare(local_json, remote_json)

    print("-" * 40)
    print(f"Fields Checked: {stats.checked_fields}")
    print(f"Matches Found:  {stats.matches}")
    print(f"Errors Found:   {len(stats.errors)}")
    print("-" * 40)

    # 5. Result Logic
    if stats.is_success():
        if case['expect_fail']:
            print("❌ TEST FAILED: Expected mismatch (New vs Old tables), but data matched perfectly.")
            print("   (Did you accidentally backfill the new table with old data?)")
        else:
            print("✅ TEST PASSED: Perfect Match.")
    else:
        if case['expect_fail']:
            print("✅ TEST PASSED (EXPECTED FAILURE): Environments are distinct.")
            # Validate that it failed for the right reason (e.g. Remote has data, Local doesn't)
            if n_rem > 0 and n_loc == 0:
                print(f"   > Validated: Remote has {n_rem} items, Local has 0 (Correct for Old Data).")
            else:
                print(f"   > Mismatch details: {stats.errors[0]}")
        else:
            print("❌ TEST FAILED: Mismatches detected in a 'Success' scenario.")
            for i, err in enumerate(stats.errors[:5]):
                print(f"   {i + 1}. {err}")
            if len(stats.errors) > 5:
                print(f"   ... and {len(stats.errors) - 5} more.")


if __name__ == "__main__":
    if not config.API_KEY:
        print("⚠️  WARNING: API_KEY is missing in tests/config.py")

    # Loop through scenarios, running True AND False merge for each
    for case in SCENARIOS:
        run_single_test(case, merge_flag=True)
        run_single_test(case, merge_flag=False)