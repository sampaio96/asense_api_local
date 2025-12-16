# tests/test_corrector.py
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
        Median should be 1280.
        t0=0
        t1=1280 (d=1280)
        t2=2585 (d=1305) -> Long (Trigger Confirm) -> NEEDS FIX
        t3=3840 (d=1255) -> Short (Trigger)

        Thresholds (1280 +/- 1.5%): [1260.8, 1299.2]
        Short: 1255 <= 1260.8 (True)
        Long:  1305 >= 1299.2 (True)
        Fix: Target 1280.
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
        # Target: t3 - median = 3840 - 1280 = 2560
        self.assertEqual(corrected[2]['time'], 2560, "Root timestamp should shift to match median spacing")

        # Check Delta Consistency
        self.assertEqual(corrected[2]['time'] - corrected[1]['time'], 1280)
        self.assertEqual(corrected[3]['time'] - corrected[2]['time'], 1280)

        # CRITICAL: Check Internal Vector Timestamps
        sample_time = corrected[2]['acc_x'][0]['time']
        self.assertEqual(sample_time, 2560, "Internal vector sample should shift")

    def test_case_5_massive_gap_with_context(self):
        """
        Scenario: Normal -> Massive Gap -> Short Catch-up.
        We add context packets so the Median is established at 1280.

        t0 = 0
        t1 = 1280
        t2 = 2560 (Median established at 1280)
        -- GAP --
        t3 = 1,000,000 (d ~ 1M, ignored by Median)
        t4 = 1,001,260 (d = 1260, Trigger)

        Logic:
        Median = 1280.
        Short Thresh = 1260.8.
        Long Thresh = 1299.2.

        Delta(t3->t4) = 1260. (1260 <= 1260.8) -> TRUE
        Delta(t2->t3) = 997440. (>= 1299.2) -> TRUE

        Fix:
        t3 should shift so t4-t3 = 1280.
        New t3 = 1,001,260 - 1280 = 999,980.
        """
        print("\n--- Test: Massive Gap Fix (With Context) ---")
        rows = [
            self._create_mock_row(0),
            self._create_mock_row(1280),
            self._create_mock_row(2560),
            self._create_mock_row(1000000),
            self._create_mock_row(1001260)
        ]

        corrected = apply_correction(rows)

        # Expect t3 to shift
        self.assertEqual(corrected[3]['time'], 999980)

        # Check Short Gap is fixed to Median
        self.assertEqual(corrected[4]['time'] - corrected[3]['time'], 1280)

    def test_case_no_fix_needed(self):
        """
        Scenario: Normal -> Short (but previous wasn't Long).
        """
        print("\n--- Test: No Fix (False Positive) ---")
        rows = [
            self._create_mock_row(0),
            self._create_mock_row(1280),
            self._create_mock_row(2540)  # d=1260
        ]
        # Median=1280. Thresh=1260.8.
        # 1260 <= 1260.8 (True).
        # Prev d=1280. 1280 >= 1299.2 (False).
        # NO FIX.

        corrected = apply_correction(rows)
        self.assertEqual(corrected[2]['time'], 2540)


if __name__ == '__main__':
    unittest.main()