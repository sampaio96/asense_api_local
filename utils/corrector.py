# utils/corrector.py
import logging

# Keys that contain time-series data (lists of dicts with 'time')
# scalar keys like 'tamb' are also lists in the internal format
VECTOR_KEYS = [
    'acc_x', 'acc_y', 'acc_z',
    'gyr_x', 'gyr_y', 'gyr_z',
    'ain_a', 'ain_b',
    'tamb', 'w_s', 'w_d'
]

logger = logging.getLogger("Corrector")
logger.setLevel(logging.INFO)


def _log_table(rows, title):
    """Debug helper to visualize deltas."""
    logger.info(f"--- {title} ---")
    if not rows:
        return
    logger.info(f"{'IDX':<4} | {'TIME':<15} | {'DELTA':<8} | {'STATUS'}")
    for i, row in enumerate(rows):
        t = row.get('time', 0)
        d = t - rows[i - 1]['time'] if i > 0 else 0
        status = "OK"
        if i > 0:
            if d <= 1260:
                status = "<< SHORT"
            elif d >= 1300:
                status = ">> LONG"
        logger.info(f"{i:<4} | {t:<15} | {d:<8} | {status}")


def apply_correction(items):
    """
    Corrects timestamp anomalies in a sorted list of sensor packets.

    Algorithm (Case 2 - Look Back):
    1. Trigger: Current Delta (i - i-1) <= 1260ms.
    2. Confirm: Previous Delta (i-1 - i-2) >= 1300ms.
    3. Fix: Shift row[i-1] BACKWARDS so Current Delta becomes exactly 1280ms.
       This simultaneously fixes the short gap and reduces the previous long gap.

    Args:
        items: List of dicts (processed rows). Each row has 'time' and vector keys.
    """
    if len(items) < 3:
        return items

    # Optional: Log before state
    # _log_table(items, "Before Correction")

    count_fixed = 0

    # Start at 2 to ensure we have a history of (i-1) and (i-2)
    for i in range(2, len(items)):
        t_curr = items[i]['time']
        t_prev = items[i - 1]['time']
        t_pprev = items[i - 2]['time']

        delta_curr = t_curr - t_prev
        delta_prev = t_prev - t_pprev

        # The Logic:
        if delta_curr <= 1260 and delta_prev >= 1300:
            # Calculate correction
            # We need delta_curr to be 1280.
            # Current gap is too small. We must move t_prev earlier (smaller).
            diff_needed = 1280 - delta_curr

            print(f"  >>> ANOMALY FIX (Idx {i}):")
            print(f"      Row[{i}] vs Row[{i-1}] Delta: {delta_curr} (Too Short)")
            print(f"      Row[{i-1}] vs Row[{i-2}] Delta: {delta_prev} (Too Long)")
            print(f"      ACTION: Shifting Row[{i-1}] backwards by {diff_needed}ms")

            # 1. Fix Root Timestamp
            items[i - 1]['time'] -= diff_needed

            # 2. Fix All Internal Samples
            # Since processors calculated sample times based on the root time,
            # we must shift them all by the same amount to maintain consistency.
            _shift_vector_timestamps(items[i - 1], -diff_needed)

            count_fixed += 1
            # logger.info(f"Fixed row {i-1}: Shifted by -{diff_needed}ms")

    if count_fixed > 0:
        logger.info(f"Timestamp Correction: Fixed {count_fixed} anomalies.")
        # Optional: Log after state
        # _log_table(items, "After Correction")

    return items


def _shift_vector_timestamps(item, offset):
    """
    Iterates through known vector keys in an item and shifts
    their internal 'time' values by the offset.
    """
    for key in VECTOR_KEYS:
        if key in item and isinstance(item[key], list):
            for sample in item[key]:
                if 'time' in sample:
                    sample['time'] += offset