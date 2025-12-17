import unittest
import json
import os
import sys
from itertools import product

# --- AWS CONFIGURATION ---
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_function import lambda_handler

# --- TEST CONFIGURATION ---
# Map tables to the devices that are known to have data for them
TABLE_DEVICE_MAP = {
    'acc': ['ASENSE00000022', 'ASENSE00000005'],
    'gyr': ['ASENSE00000005'],
    'ain': ['ASENSE00000005']
}
START_TIME = '1764547200000'
END_TIME = '1765324800000'


class TestFlagCombinations(unittest.TestCase):

    def _fetch_data(self, table_name, device_id, merge, correction, auto_odr):
        """Helper to call the lambda_handler with specific flags."""
        event = {
            'queryStringParameters': {
                'table_name': table_name,
                'id': device_id,
                'start_time': START_TIME,
                'end_time': END_TIME,
                'merge': str(merge).lower(),
                'enable_correction': str(correction).lower(),
                'auto_odr': str(auto_odr).lower(),
                'output_format': 'dict_array'
            }
        }

        try:
            response = lambda_handler(event, None)
            if response['statusCode'] != 200:
                print(f"  [API ERROR] Status {response['statusCode']}: {response.get('body', 'No body')}")
                return []
            return json.loads(response['body'])['data']
        except Exception as e:
            print(f"  [HANDLER CRASH] Exception: {e}")
            return []

    def _summarize_result(self, data, table_name, merge_flag):
        """Generates key metrics from a dataset."""
        if not data:
            return {"num_items": 0, "total_samples": 0, "is_monotonic": "N/A", "sample_period_ms": "N/A"}

        # Determine primary vector key based on table
        vec_key_map = {'acc': 'acc_x', 'gyr': 'gyr_x', 'ain': 'ain_a'}
        vec_key = vec_key_map[table_name]

        total_samples = sum(len(item.get(vec_key, [])) for item in data)

        # Monotonicity check (simplified for this test)
        is_monotonic = True
        if merge_flag and data and vec_key in data[0]:
            samples = data[0].get(vec_key, [])
            timestamps = [p['time'] for p in samples]
            if not all(timestamps[i] < timestamps[i + 1] for i in range(len(timestamps) - 1)):
                is_monotonic = False

        # --- CORRECTED PERIOD CHECK ---
        # We check the period of the *second* packet/segment, as the first is never calibrated.
        sample_period_ms = "N/A"
        if merge_flag:
            # For merged data, check the seam between packet 0 and 1 (sample index 64 & 65)
            if data and vec_key in data[0]:
                all_samples = data[0][vec_key]
                if len(all_samples) > 65:
                    delta = all_samples[65]['time'] - all_samples[64]['time']
                    sample_period_ms = f"{delta:.4f}"
        else:  # Unmerged
            # For unmerged data, check the second item in the list (packet at index 1)
            if len(data) > 1 and vec_key in data[1]:
                samples = data[1][vec_key]
                if len(samples) > 1:
                    delta = samples[1]['time'] - samples[0]['time']
                    sample_period_ms = f"{delta:.4f}"

        return {
            "num_items": len(data),
            "total_samples": total_samples,
            "is_monotonic": "✅ YES" if is_monotonic else "❌ NO",
            "sample_period_ms": sample_period_ms
        }

    def test_all_combinations(self):
        """Main test runner."""
        flags = [True, False]
        combinations = list(product(flags, repeat=3))

        for table_name, device_list in TABLE_DEVICE_MAP.items():
            for device_id in device_list:
                results = {}
                print(f"\n{'=' * 90}\n🔬 RUNNING 8 SCENARIOS FOR DEVICE: {device_id} (Table: {table_name})\n{'=' * 90}")

                for i, (merge, correction, auto_odr) in enumerate(combinations):
                    key = (merge, correction, auto_odr)
                    print(f"  ({i + 1}/8) Testing -> Merge: {merge}, Correction: {correction}, AutoODR: {auto_odr}")

                    data = self._fetch_data(table_name, device_id, merge, correction, auto_odr)
                    summary = self._summarize_result(data, table_name, merge_flag=merge)
                    results[key] = summary

                print(f"\n--- 📊 COMPARISON TABLE: {device_id} ({table_name}) ---")
                print(
                    f"| {'Merge':<7} | {'Correction':<12} | {'Auto ODR':<10} || {'Items':>7} | {'Samples':>9} | {'Monotonic':>11} | {'Period (ms)':>15} |")
                print(f"|{'-' * 9}|{'-' * 14}|{'-' * 12}||{'-' * 9}|{'-' * 11}|{'-' * 13}|{'-' * 17}|")

                for m, c, a in sorted(combinations, key=lambda x: (x[0], x[1], x[2]), reverse=True):
                    res = results.get((m, c, a), {})
                    print(
                        f"| {str(m):<7} | {str(c):<12} | {str(a):<10} || {res.get('num_items', 'N/A'):>7} | {res.get('total_samples', 'N/A'):>9} | {res.get('is_monotonic', 'N/A'):>11} | {res.get('sample_period_ms', 'N/A'):>15} |")

                print(f"{'-' * 90}\n")


if __name__ == '__main__':
    unittest.main()