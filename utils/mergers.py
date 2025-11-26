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

    earliest_time = min(group, key=lambda x: x.get('time', float('inf')))['time']
    # Sort by seq
    group.sort(key=lambda x: (x.get('seq') is None, x.get('seq', 0)))

    merged_item = group[0].copy()
    merged_item['time'] = earliest_time

    for item in group[1:]:
        for key, value in item.items():
            if key == 'time': continue

            current_val = merged_item.get(key)

            # Case 1: Both are Dictionaries (Map format) -> Update
            if isinstance(value, dict) and isinstance(current_val, dict):
                merged_item[key] = {**current_val, **value}

            # Case 2: Both are Lists (Tuple/Dict Array format) -> Concatenate
            elif isinstance(value, list) and isinstance(current_val, list):
                merged_item[key] = current_val + value

            # Case 3: Key doesn't exist in merged yet -> Set it
            elif key not in merged_item:
                merged_item[key] = value

    # Sort keys in top level
    merged_item = dict(sorted(merged_item.items()))

    # Sort contents
    for key, value in merged_item.items():
        if isinstance(value, dict):
            merged_item[key] = dict(sorted(value.items()))
        # No need to sort lists here; concatenation of time-ordered sequences
        # preserves order naturally.

    return merged_item


def merge_items_by_hour(data):
    if not data: return []
    merged_data = []
    current_hour_ts = None
    current_group = []

    for item in data:
        item_time = item.get('time')
        if item_time is None: continue
        item_hour_ts = get_hour_from_timestamp(item_time)

        if current_hour_ts is None or current_hour_ts != item_hour_ts:
            if current_group:
                merged_data.append(merge_items_in_group(current_group))
            current_hour_ts = item_hour_ts
            current_group = [item]
        else:
            current_group.append(item)

    if current_group:
        merged_data.append(merge_items_in_group(current_group))

    return merged_data


def merge_fft_axes_in_group(group):
    merged_item = {}
    for item in group:
        temp_item = item.copy()
        axis = str(temp_item.get('axis', ''))

        # Handle axis renaming (works for Lists and Dicts)
        if 'fft' in temp_item:
            val = temp_item.pop('fft')
            if axis == '0':
                temp_item['fft_x'] = val
            elif axis == '1':
                temp_item['fft_y'] = val
            elif axis == '2':
                temp_item['fft_z'] = val

        temp_item.pop('axis', None)

        for key, value in temp_item.items():
            current_val = merged_item.get(key)

            if isinstance(value, dict) and isinstance(current_val, dict):
                merged_item[key] = {**current_val, **value}
            elif isinstance(value, list) and isinstance(current_val, list):
                merged_item[key] = current_val + value
            elif key not in merged_item:
                merged_item[key] = value

    merged_item = dict(sorted(merged_item.items()))
    # Sort dictionary contents (Lists don't need sorting here for FFT usually)
    for key, value in merged_item.items():
        if isinstance(value, dict):
            merged_item[key] = dict(sorted(value.items()))

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