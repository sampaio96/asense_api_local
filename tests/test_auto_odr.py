# tests/test_auto_odr.py
import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.corrector import apply_correction


class TestSensorCorrector(unittest.TestCase):

    def _create_mock_row(self, time_val, vec_keys=None, num_samples=64):
        """
        Creates a mock processed row.
        Default: 64 samples, ODR=50Hz (20ms period).
        """
        row = {'time': time_val}
        if vec_keys:
            for k in vec_keys:
                samples = []
                # Standard Processor logic: Last sample at time_val, spacing 20ms
                for i in range(num_samples):
                    steps_back = (num_samples - 1) - i
                    t = time_val - (steps_back * 20)
                    samples.append({'time': t, 'val': 1.0})
                row[k] = samples
        return row

    def test_auto_odr_off_standard_behavior(self):
        """
        Scenario: Auto ODR is FALSE.
        Packets arrive with 1250ms gap.
        Expect: Samples remain at 20ms spacing (Standard).
        """
        print("\n--- Test: Auto ODR OFF (Standard 50Hz) ---")
        rows = [
            self._create_mock_row(0, ['acc_x']),
            self._create_mock_row(1250, ['acc_x']),
            self._create_mock_row(2500, ['acc_x'])  # Added 3rd row
        ]

        corrected = apply_correction(rows, auto_odr=False)

        # Check P1 (Middle packet)
        samples = corrected[1]['acc_x']

        # Verify Spacing is exactly 20
        spacing = samples[1]['time'] - samples[0]['time']
        self.assertEqual(spacing, 20.0)
        print("    ✅ Validated: Spacing remains 20.0ms (Standard)")

    def test_auto_odr_on_compression(self):
        """
        Scenario: Auto ODR is TRUE.
        Packets arrive with 1250ms gap.
        Expect: Samples compress to fit 1250ms.
        Period = 1250 / 64 = 19.53125 ms.
        """
        print("\n--- Test: Auto ODR ON (Compression / Fast Crystal) ---")
        rows = [
            self._create_mock_row(0, ['acc_x']),
            self._create_mock_row(1250, ['acc_x']),
            self._create_mock_row(2500, ['acc_x'])  # Added 3rd row
        ]

        corrected = apply_correction(rows, auto_odr=True)

        # Check P1 (gap 0 -> 1250)
        samples = corrected[1]['acc_x']

        # Last sample should still be anchor
        self.assertEqual(samples[-1]['time'], 1250)

        # First sample calculation
        # t_first = 1250 - (63 * (1250/64))
        # t_first = 1250 - 1230.46875 = 19.53125
        first_time = samples[0]['time']

        # Verify it is positive (no overlap with previous packet at 0)
        self.assertGreater(first_time, 0)

        # Verify Spacing
        spacing = samples[1]['time'] - samples[0]['time']
        expected_spacing = 1250.0 / 64.0
        self.assertAlmostEqual(spacing, expected_spacing, places=5)
        print(f"    ✅ Validated: Spacing compressed to {spacing:.4f}ms")

    def test_auto_odr_guardrail(self):
        """
        Scenario: Massive Gap (2000ms).
        Expect: Auto ODR should NOT stretch samples. Should keep 20ms.
        """
        print("\n--- Test: Auto ODR Guardrail (Packet Loss) ---")
        rows = [
            self._create_mock_row(0, ['acc_x']),
            self._create_mock_row(2000, ['acc_x']),
            self._create_mock_row(4000, ['acc_x'])  # Added 3rd row
        ]

        corrected = apply_correction(rows, auto_odr=True)
        samples = corrected[1]['acc_x']

        spacing = samples[1]['time'] - samples[0]['time']
        self.assertEqual(spacing, 20.0)
        print("    ✅ Validated: Spacing NOT stretched (remains 20.0ms)")

    def test_integration_glitch_and_drift(self):
        """
        Scenario: Fast Crystal (1250ms) + Late Timestamp Glitch.
        """
        print("\n--- Test: Integration (Glitch Fix + Auto ODR) ---")
        rows = [
            self._create_mock_row(0, ['acc_x']),
            self._create_mock_row(1250, ['acc_x']),
            self._create_mock_row(2520, ['acc_x']),  # The Glitch
            self._create_mock_row(3750, ['acc_x'])
        ]

        corrected = apply_correction(rows, auto_odr=True)

        self.assertEqual(corrected[2]['time'], 2500, "Timestamp should be corrected to 2500")

        samples_t2 = corrected[2]['acc_x']
        spacing_t2 = samples_t2[1]['time'] - samples_t2[0]['time']
        expected_spacing = 1250.0 / 64.0

        self.assertAlmostEqual(spacing_t2, expected_spacing, places=5)
        print("    ✅ Validated: Glitch fixed AND samples recalibrated.")


if __name__ == '__main__':
    unittest.main()