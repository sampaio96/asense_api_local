import logging
import statistics

VECTOR_KEYS = ['acc_x', 'acc_y', 'acc_z', 'gyr_x', 'gyr_y', 'gyr_z', 'ain_a', 'ain_b', 'tamb', 'w_s', 'w_d']

logger = logging.getLogger("Corrector")
logger.setLevel(logging.INFO)


def apply_correction(items, enable_glitch_fix=False, enable_auto_odr=False):
    """
    Corrects timestamp anomalies and optionally recalibrates sample spacing.
    """
    if len(items) < 2:
        return items

    # --- Stage 1: Glitch Correction (Conditional) ---
    if enable_glitch_fix and len(items) >= 3:
        raw_deltas = [items[i]['time'] - items[i - 1]['time'] for i in range(1, len(items))]
        valid_deltas = [d for d in raw_deltas if 1000 <= d <= 1500]

        median_delta = statistics.median(valid_deltas) if enable_auto_odr and valid_deltas else 1280.0

        thresh_short = median_delta * (1 - 0.015)
        thresh_long = median_delta * (1 + 0.015)

        logger.info(
            f"Corrector Config: Median={median_delta:.1f}ms, Thresholds=[{thresh_short:.1f}, {thresh_long:.1f}]")
        count_fixed = 0

        for i in range(2, len(items)):
            delta_curr = items[i]['time'] - items[i - 1]['time']
            delta_prev = items[i - 1]['time'] - items[i - 2]['time']

            if delta_curr <= thresh_short and delta_prev >= thresh_long:
                diff_needed = median_delta - delta_curr
                shift_int = int(round(diff_needed))
                logger.info(f"  > Glitch Fix @ Idx {i - 1}: Correcting packet time by {-shift_int}ms.")

                items[i - 1]['time'] -= shift_int
                _shift_vector_timestamps(items[i - 1], -shift_int)
                count_fixed += 1

        if count_fixed > 0:
            logger.info(f"Timestamp Correction: Fixed {count_fixed} anomalies.")

    # --- Stage 2: Auto ODR Recalibration (Conditional) ---
    if enable_auto_odr:
        calibrated_count = _recalibrate_samples_backwards(items)
        if calibrated_count > 0:
            logger.info(f"Auto ODR: Recalibrated sample spacing for {calibrated_count} packets.")

    return items


def _shift_vector_timestamps(item, offset):
    """Shifts all internal 'time' values in an item's vectors."""
    for key in VECTOR_KEYS:
        if key in item and isinstance(item[key], list):
            for sample in item[key]:
                if 'time' in sample:
                    sample['time'] += offset


def _recalibrate_samples_backwards(items):
    """
    Recalibrates sample timestamps for each packet (from the second onwards)
    based on the time delta from its predecessor. This is numerically stable.
    """
    calibrated_count = 0
    for i in range(1, len(items)):
        end_time_anchor = items[i]['time']
        prev_end_time = items[i - 1]['time']
        delta = end_time_anchor - prev_end_time

        if 1000 <= delta <= 1500:
            calibrated_count += 1
            for key in VECTOR_KEYS:
                if key in items[i] and isinstance(items[i][key], list):
                    samples = items[i][key]
                    count = len(samples)
                    if count > 0:
                        period = delta / count
                        for k, sample in enumerate(samples):
                            steps_back = (count - 1) - k
                            sample['time'] = end_time_anchor - (steps_back * period)
    return calibrated_count