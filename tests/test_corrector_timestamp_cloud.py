import unittest
import json
import os
import sys

# --- AWS CONFIGURATION ---
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lambda_function import lambda_handler


class TestRealCloudData(unittest.TestCase):
    # Parameters known to produce the 1236ms / 1331ms anomaly
    PARAMS = {
        'table_name': 'acc',
        'id': 'ASENSE00000024',
        'start_time': '1762041600000',
        'end_time': '1763910116642',
        'output_format': 'dict_array'
    }

    def _fetch(self, merge_val, correction_val):
        event = {
            'queryStringParameters': self.PARAMS.copy()
        }
        event['queryStringParameters']['merge'] = merge_val
        event['queryStringParameters']['enable_correction'] = correction_val

        print(f"\n[REQUEST] Merge={merge_val}, Correction={correction_val}")
        response = lambda_handler(event, None)

        if response['statusCode'] != 200:
            raise Exception(f"API Error: {response['body']}")

        return json.loads(response['body'])['data']

    def _check_monotonicity(self, points, verbose_fail=False):
        """Returns True if t[i] < t[i+1] for all points."""
        timestamps = [p['time'] for p in points]
        for i in range(len(timestamps) - 1):
            if timestamps[i] >= timestamps[i + 1]:
                if verbose_fail:
                    print(f"    ❌ ORDER FAIL at index {i}")
                    print(f"       t[{i}]   = {timestamps[i]}")
                    print(f"       t[{i + 1}] = {timestamps[i + 1]}")
                    print(f"       Diff     = {timestamps[i + 1] - timestamps[i]}")
                return False
        return True

    def test_1_merge_true_comparison(self):
        """
        Validates internal vector stitching.
        """
        print("\n=== TEST 1: MERGE=TRUE (Vector Stitching) ===")

        # 1. Correction OFF
        data_off = self._fetch(merge_val='true', correction_val='false')
        if not data_off: return
        acc_x_off = data_off[0]['acc_x']

        is_sorted_off = self._check_monotonicity(acc_x_off, verbose_fail=True)
        self.assertFalse(is_sorted_off, "Data should be BROKEN (unsorted) when correction is disabled.")
        print("    ✅ Confirmed: Data is broken without correction.")

        # 2. Correction ON
        data_on = self._fetch(merge_val='true', correction_val='true')
        acc_x_on = data_on[0]['acc_x']

        is_sorted_on = self._check_monotonicity(acc_x_on, verbose_fail=True)
        self.assertTrue(is_sorted_on, "Data should be PERFECT (strictly increasing) when correction is enabled.")
        print("    ✅ Confirmed: Data is fixed with correction.")

    def test_2_merge_false_comparison(self):
        """
        Validates Packet-to-Packet Deltas.
        Ensures NO delta is <= 1260ms.
        """
        print("\n=== TEST 2: MERGE=FALSE (Packet Boundaries) ===")

        data = self._fetch(merge_val='false', correction_val='true')
        print(f"Fetched {len(data)} packets.")

        # Iterate through packets to verify deltas are valid
        for i in range(1, len(data)):
            # In 'dict_array', 'time' is removed from root, but preserved in samples.
            # The timestamp of the packet is the timestamp of the LAST sample.
            t_curr = data[i]['acc_x'][-1]['time']
            t_prev = data[i - 1]['acc_x'][-1]['time']

            delta = t_curr - t_prev

            # --- CRITICAL CHECK ---
            # 1. Monotonicity (Start of B > End of A) - Approximation
            # Actually, let's check Packet Delta (End to End).
            # The firmware anomaly implies Delta <= 1260.
            # We assert that NO delta is <= 1260.

            if delta <= 1260:
                print(f"❌ INVALID DELTA at index {i}: {delta}ms (<= 1260)")
                print(f"   Prev Time: {t_prev}")
                print(f"   Curr Time: {t_curr}")
                self.fail(f"Found delta {delta}ms which is <= 1260ms. Correction failed.")

            # Also verify strict sample ordering at the seam
            t_prev_sample_end = t_prev
            t_curr_sample_start = data[i]['acc_x'][0]['time']

            if t_curr_sample_start <= t_prev_sample_end:
                self.fail(f"Samples overlapped at packet seam {i - 1}->{i}")

        print("    ✅ Packet Analysis:")
        print("       1. All Packet-to-Packet deltas are > 1260ms.")
        print("       2. No sample overlaps at packet seams.")


if __name__ == '__main__':
    unittest.main()