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


def apply_correction(items, auto_odr=False):
    """
    Corrects timestamp anomalies in a sorted list of sensor packets using a Median Filter,
    and optionally recalibrates internal sample spacing (Auto ODR).

    Algorithm:
    1. Calculate Local ODR (Median) from valid deltas (<= 1408ms).
    2. Determine Dynamic Thresholds (Median +/- 1.5%).
    3. Iterate to find 'Late Timestamp' signature:
       - Current Delta <= Thresh_Short
       - Previous Delta >= Thresh_Long
    4. Fix: Shift row[i-1] so Current Delta matches Median exactly.
    5. If auto_odr=True, iterate again to recalibrate sample spacing.

    Args:
        items: List of dicts (processed rows). Each row has 'time' and vector keys.
        auto_odr: If True, uses actual Median and recalibrates samples.
    """
    if len(items) < 3:
        if auto_odr:
            calibrated_count = _recalibrate_samples(items)
            if calibrated_count > 0:
                logger.info(f"Auto ODR: Recalibrated sample spacing for {calibrated_count} packets.")
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
        median_delta = 1280.0  # Fallback or Forced Standard

    # --- Step 2: Calculate Dynamic Thresholds (1.5% Rule) ---
    # 1.5% of 1280 is 19.2ms (approx 1 sample duration)
    # Using strict inequalities in logic implies:
    # Short: <= Median - 1.5%
    # Long:  >= Median + 1.5%
    thresh_short = median_delta * (1 - 0.015)
    thresh_long = median_delta * (1 + 0.015)

    logger.info(f"Corrector Config: Median={median_delta:.1f}ms, Thresholds=[{thresh_short:.1f}, {thresh_long:.1f}]")

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

            # Step 4: Apply Correction
            # 1. Fix Root Timestamp
            items[i - 1]['time'] -= shift_int

            # 2. Fix All Internal Samples
            _shift_vector_timestamps(items[i - 1], -shift_int)

            count_fixed += 1

    if count_fixed > 0:
        logger.info(f"Timestamp Correction: Fixed {count_fixed} anomalies using Median={median_delta:.2f}.")

    # --- Step 5: Auto ODR Recalibration ---
    if auto_odr:
        calibrated_count = _recalibrate_samples(items)
        if calibrated_count > 0:
            logger.info(f"Auto ODR: Recalibrated sample spacing for {calibrated_count} packets.")

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


def _recalibrate_samples(items):
    """
    Recalculates sample timestamps for each packet (except the last)
    based on the time delta to its successor.
    """
    # We iterate from 1 because we need the delta (t_i - t_i-1)
    # The first packet (idx 0) cannot be auto-calibrated as it has no predecessor reference.
    # It remains with the Standard ODR (50Hz) from the Processor.

    calibrated_count = 0
    # Iterate through each packet that has a successor.
    for i in range(len(items) - 1):
        delta = items[i + 1]['time'] - items[i]['time']

        # Guardrail: Only stretch/compress if delta is within a reasonable window
        if 1152 <= delta <= 1408:
            calibrated_count += 1
            # Recalibrate all vectors in the CURRENT item (items[i])
            for key in VECTOR_KEYS:
                if key in items[i] and isinstance(items[i][key], list):
                    samples = items[i][key]
                    count = len(samples)
                    if count > 1:
                        # N samples span N-1 intervals. The total duration is delta.
                        period = delta / (count - 1)

                        # The first sample should be at t_current - delta
                        # We recalculate all from the anchor to avoid float drift.
                        start_time = items[i]['time'] - delta

                        for k, sample in enumerate(samples):
                            sample['time'] = start_time + (k * period)
    return calibrated_count
