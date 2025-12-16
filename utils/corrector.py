# utils/corrector.py
import logging
import statistics

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
    Corrects timestamp anomalies in a sorted list of sensor packets using a Median Filter.

    Algorithm:
    1. Calculate Local ODR (Median) from valid deltas (<= 1408ms).
    2. Determine Dynamic Thresholds (Median +/- 1.5%).
    3. Iterate to find 'Late Timestamp' signature:
       - Current Delta <= Thresh_Short
       - Previous Delta >= Thresh_Long
    4. Fix: Shift row[i-1] so Current Delta matches Median exactly.

    Args:
        items: List of dicts (processed rows). Each row has 'time' and vector keys.
    """
    if len(items) < 3:
        return items

    # --- Step 1: Establish Local Pulse (Median) ---
    raw_deltas = []
    # Calculate all raw deltas first
    for i in range(1, len(items)):
        d = items[i]['time'] - items[i - 1]['time']
        # Filter: Exclude discontinuities (> 1280 + 10%)
        if d <= 1408:
            raw_deltas.append(d)

    if raw_deltas:
        median_delta = statistics.median(raw_deltas)
    else:
        median_delta = 1280.0  # Fallback to standard if all are discontinuities

    # --- Step 2: Calculate Dynamic Thresholds (1.5% Rule) ---
    # 1.5% of 1280 is 19.2ms (approx 1 sample duration)
    # Using strict inequalities in logic implies:
    # Short: <= Median - 1.5%
    # Long:  >= Median + 1.5%
    thresh_short = median_delta * (1 - 0.015)
    thresh_long = median_delta * (1 + 0.015)

    # logger.info(f"Corrector Config: Median={median_delta:.2f}, Short<={thresh_short:.2f}, Long>={thresh_long:.2f}")

    count_fixed = 0

    # Start at 2 to ensure we have a history of (i-1) and (i-2)
    for i in range(2, len(items)):
        t_curr = items[i]['time']
        t_prev = items[i - 1]['time']
        t_pprev = items[i - 2]['time']

        delta_curr = t_curr - t_prev
        delta_prev = t_prev - t_pprev

        # --- Step 3: Trigger Logic ---
        if delta_curr <= thresh_short and delta_prev >= thresh_long:
            # We fix relative to the CURRENT packet (i), using the MEDIAN.
            # Target previous time: t_curr - median
            # Diff needed: how much to shift t_prev BACK to match target

            # Current: t_curr - t_prev = delta_curr
            # Desired: t_curr - t_new  = median
            # => t_new = t_curr - median
            # shift = t_prev - t_new
            # shift = t_prev - (t_curr - median)
            # shift = median - (t_curr - t_prev)
            # shift = median - delta_curr

            diff_needed = median_delta - delta_curr

            # Round to int for timestamps
            shift_int = int(round(diff_needed))

            print(f"  >>> ANOMALY FIX (Idx {i}):")
            print(f"      Median: {median_delta:.1f}")
            print(f"      Row[{i}] Delta: {delta_curr} (<= {thresh_short:.1f})")
            print(f"      Row[{i-1}] Delta: {delta_prev} (>= {thresh_long:.1f})")
            print(f"      ACTION: Shifting Row[{i-1}] backwards by {shift_int}ms")

            # 1. Fix Root Timestamp
            items[i - 1]['time'] -= shift_int

            # 2. Fix All Internal Samples
            _shift_vector_timestamps(items[i - 1], -shift_int)

            count_fixed += 1

    if count_fixed > 0:
        logger.info(f"Timestamp Correction: Fixed {count_fixed} anomalies using Median={median_delta:.2f}.")

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