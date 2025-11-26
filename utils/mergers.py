import datetime


def get_hour_from_timestamp(timestamp_ms):
    """Rounds a timestamp down to the last hour (UTC)."""
    try:
        ts_seconds = float(timestamp_ms) / 1000
    except (ValueError, TypeError) as e:
        return 0  # Fail safe

    dt = datetime.datetime.fromtimestamp(ts_seconds, tz=datetime.timezone.utc)
    dt_rounded = dt.replace(minute=0, second=0, microsecond=0)
    return int(dt_rounded.timestamp() * 1000)


def merge_items_in_group(group):
    """Merges a group of time-series dictionaries."""
    if not group:
        return {}

    earliest_time = min(group, key=lambda x: x.get('time', float('inf')))['time']
    # Sort by seq, putting None last
    group.sort(key=lambda x: (x.get('seq') is None, x.get('seq', 0)))

    merged_item = group[0].copy()
    merged_item['time'] = earliest_time

    for item in group[1:]:
        for key, value in item.items():
            if key == 'time': continue

            current_val = merged_item.get(key)

            if isinstance(value, dict) and isinstance(current_val, dict):
                merged_item[key] = {**current_val, **value}
            elif key not in merged_item:
                merged_item[key] = value

    # Sort keys
    merged_item = dict(sorted(merged_item.items()))
    for key, value in merged_item.items():
        if isinstance(value, dict):
            merged_item[key] = dict(sorted(value.items()))

    return merged_item


def merge_items_by_hour(data):
    """Standard merge for ACC, GYR, AIN, DATA."""
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
    """Specific merge logic for FFT (renaming axis to fft_x/y/z)."""
    merged_item = {}

    # We don't have a 'seq' to sort by in FFT typically, relying on time/axis
    for item in group:
        # Clone to avoid modifying original
        temp_item = item.copy()

        # Handle axis renaming
        axis = str(temp_item.get('axis', ''))
        if 'fft' in temp_item:
            if axis == '0':
                temp_item['fft_x'] = temp_item.pop('fft')
            elif axis == '1':
                temp_item['fft_y'] = temp_item.pop('fft')
            elif axis == '2':
                temp_item['fft_z'] = temp_item.pop('fft')

        temp_item.pop('axis', None)

        for key, value in temp_item.items():
            if isinstance(value, dict):
                merged_item[key] = {**merged_item.get(key, {}), **value}
            else:
                # Keep first value encountered
                merged_item[key] = merged_item.get(key, value)

    merged_item = dict(sorted(merged_item.items()))
    for key, value in merged_item.items():
        if isinstance(value, dict):
            merged_item[key] = dict(sorted(value.items()))

    return merged_item


def merge_fft_axes_by_hour(data):
    """FFT specific merge wrapper."""
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