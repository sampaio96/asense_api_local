import sys
import os

# Ensure we can import from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processors import acc

# --- CONFIGURATION ---
MOCK_END_TIME = 1700000000000  # Example Epoch MS
ODR = 50.0                     # 50 Hz = 20ms per sample
SAMPLES_PER_AXIS = 64
TOTAL_POINTS = SAMPLES_PER_AXIS * 3
MOCK_AXYZ = [1.0] * TOTAL_POINTS

mock_event = {
    'id': 'TEST_DEVICE',
    'time': MOCK_END_TIME,  # The anchor
    'odr': ODR,
    'scale': 1.0,
    'axyz': MOCK_AXYZ,
    'seq': 99,
    'tamb': 2500, # 25.0 C
    'w_s': 1000,  # 10.0 m/s
    'w_d': 8      # 180 deg
}

print("--- Testing ACC Processor Timestamp Logic ---")
print(f"Input Time (End): {MOCK_END_TIME}")
print(f"ODR: {ODR} Hz (Period: {1000/ODR} ms)")

# Run Process
result = acc.process([mock_event], fmt='tuple_array')
item = result[0]

# Extract Data
acc_x = item['acc_x'] # List of [time, val]
tamb = item['tamb']

print(f"\n[Scalars] Tamb Time: {tamb[0][0]} (Expected: {MOCK_END_TIME})")

print("\n[Vectors] Checking X-Axis Timestamps:")
first_point = acc_x[0]
last_point = acc_x[-1]

print(f"First Point (Index 0):  Time={first_point[0]}")
print(f"Last Point  (Index 63): Time={last_point[0]}")

# Verification
expected_last = float(MOCK_END_TIME)
expected_first = MOCK_END_TIME - ((SAMPLES_PER_AXIS - 1) * (1000.0/ODR))

print("\n--- Verification ---")
if abs(last_point[0] - expected_last) < 0.001:
    print("✅ Last point matches Input Time exactly.")
else:
    print(f"❌ Last point mismatch! Got {last_point[0]}, expected {expected_last}")

if abs(first_point[0] - expected_first) < 0.001:
    print("✅ First point matches calculated start time.")
else:
    print(f"❌ First point mismatch! Got {first_point[0]}, expected {expected_first}")
    print(f"   Diff: {first_point[0] - expected_first}")

if item['time'] == MOCK_END_TIME:
    print("✅ Item root 'time' matches Input Time.")
else:
    print("❌ Item root 'time' mismatch.")