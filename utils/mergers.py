# utils/mergers.py
import datetime


def get_hour_from_timestamp(timestamp_ms):
    try:
        ts_seconds = float(timestamp_ms) / 1000
    except (ValueError, TypeError):
        return 0
    dt = datetime.datetime.fromtimestamp(ts_seconds, tz=datetime.timezone.utc)
    dt_rounded = dt.replace(minute=0, second=0, microsecond=0)
    return int(dt_rounded.timestamp() * 1000)


def merge_items_in_group(group):
    if not group:
        return {}

    # 1. Base is the first item (sorted by time in Lambda)
    merged_item = group[0].copy()

    # 2. Iterate through the rest
    for item in group[1:]:
        for key, value in item.items():
            if key == 'time': continue  # Skip time, we keep the earliest

            current_val = merged_item.get(key)

            # Internal Standard guarantees that vectors are LISTS.
            # We simply concatenate them.
            if isinstance(value, list) and isinstance(current_val, list):
                merged_item[key] = current_val + value

            # If it's a scalar (metadata like scale/odr) or a new key,
            # we generally keep the existing one (first wins) or set if missing.
            elif key not in merged_item:
                merged_item[key] = value

    return merged_item


# FFT Merger needs to handle the standard list format and rename keys
def merge_fft_axes_in_group(group):
    merged_item = {}
    for item in group:
        temp_item = item.copy()
        axis = str(temp_item.get('axis', ''))

        # Rename 'fft' -> 'fft_x' etc. based on axis
        if 'fft' in temp_item:
            val = temp_item.pop('fft')  # This is a list of dicts now
            if axis == '0':
                temp_item['fft_x'] = val
            elif axis == '1':
                temp_item['fft_y'] = val
            elif axis == '2':
                temp_item['fft_z'] = val

        temp_item.pop('axis', None)

        for key, value in temp_item.items():
            current_val = merged_item.get(key)
            if isinstance(value, list) and isinstance(current_val, list):
                merged_item[key] = current_val + value
            elif key not in merged_item:
                merged_item[key] = value

    return merged_item


def merge_fft_axes_by_hour(data):
    merged_data = []
    current_hour = None
    current_group = []

    for item in data:
        item_hour = get_hour_from_timestamp(item['time'])
        if current_hour != item_hour:
            if current_group:
                merged_data.append(merge_fft_axes_in_group(current_group))
            current_hour = item_hour
            current_group = [item]
        else:
            current_group.append(item)

    if current_group:
        merged_data.append(merge_fft_axes_in_group(current_group))
    return merged_data