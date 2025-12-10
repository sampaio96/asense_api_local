import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.corrector import apply_correction


class TestSensorCorrector(unittest.TestCase):

    def _create_mock_row(self, time_val, vec_keys=None):
        """Creates a mock processed row with one sample in vectors."""
        row = {'time': time_val}
        if vec_keys:
            for k in vec_keys:
                # Add one sample at the exact packet timestamp
                row[k] = [{'time': time_val, 'val': 1.0}]
        return row

    def test_case_2_standard_fix(self):
        """
        Scenario: Normal -> Long -> Short.
        t0=0
        t1=1280 (d=1280)
        t2=2585 (d=1305) -> Long (Trigger Confirm) -> NEEDS FIX
        t3=3840 (d=1255) -> Short (Trigger)

        Fix:
        Short is 1255. Target 1280. Diff = 25.
        Row 2 (2585) should be decreased by 25 -> 2560.
        """
        print("\n--- Test: Standard Correction (Row & Vectors) ---")
        rows = [
            self._create_mock_row(0, ['acc_x']),
            self._create_mock_row(1280, ['acc_x']),
            self._create_mock_row(2585, ['acc_x']),
            self._create_mock_row(3840, ['acc_x'])
        ]

        corrected = apply_correction(rows)

        # Check Root Timestamps
        self.assertEqual(corrected[2]['time'], 2560, "Root timestamp should shift -25")

        # Check Delta Consistency
        self.assertEqual(corrected[2]['time'] - corrected[1]['time'], 1280)
        self.assertEqual(corrected[3]['time'] - corrected[2]['time'], 1280)

        # CRITICAL: Check Internal Vector Timestamps
        # The sample inside row[2]['acc_x'] was 2585, should now be 2560
        sample_time = corrected[2]['acc_x'][0]['time']
        self.assertEqual(sample_time, 2560, "Internal vector sample should shift -25")

    def test_case_5_massive_gap(self):
        """
        Scenario: Normal -> Massive Gap -> Short Catch-up.
        t0=0
        t1=1,000,000 (d=1,000,000)
        t2=1,001,260 (d=1260) -> Short Trigger

        Fix:
        Short is 1260. Target 1280. Diff = 20.
        Row 1 (1,000,000) should decrease by 20 -> 999,980.
        """
        print("\n--- Test: Massive Gap Fix ---")
        rows = [
            self._create_mock_row(0),
            self._create_mock_row(1000000),
            self._create_mock_row(1001260)
        ]

        corrected = apply_correction(rows)

        self.assertEqual(corrected[1]['time'], 999980)
        # Check Short Gap is fixed
        self.assertEqual(corrected[2]['time'] - corrected[1]['time'], 1280)

    def test_case_no_fix_needed(self):
        """
        Scenario: Normal -> Short (but previous wasn't Long).
        t0=0
        t1=1280
        t2=2540 (d=1260)
        Previous d=1280. No fix.
        """
        print("\n--- Test: No Fix (False Positive) ---")
        rows = [
            self._create_mock_row(0),
            self._create_mock_row(1280),
            self._create_mock_row(2540)
        ]

        corrected = apply_correction(rows)
        self.assertEqual(corrected[2]['time'], 2540)


if __name__ == '__main__':
    unittest.main()