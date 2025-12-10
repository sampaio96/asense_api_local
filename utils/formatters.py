# utils/formatters.py

def float_to_padded_string(val, n_left_zeros):
    """Formats a float to a string with left-padded zeros."""
    s = str(val)
    if 'e' in s.lower():
        s = f"{val:.20f}".rstrip('0')
    if '.' not in s:
        s += '.0'
    if s.endswith('.'):
        s += '0'
    sign = ""
    if s.startswith('-'):
        sign = "-"
        s = s[1:]
    left, right = s.split('.')
    left_padded = left.zfill(n_left_zeros)
    return f"{sign}{left_padded}.{right}"


class DataBuilder:
    def __init__(self, output_format, index_key='time', value_key='val', pad_zeros=5, prefix=None):
        """
        :param output_format: 'map', 'tuple_array', or 'dict_array'
        :param index_key: Key for the domain (e.g., 'time', 'freq')
        :param value_key: Key for the value (e.g., 'val')
        """
        self.fmt = output_format
        self.index_key = index_key
        self.value_key = value_key
        self.pad = pad_zeros
        self.prefix = prefix

        if self.fmt == 'map':
            self.data = {}
        else:
            self.data = []

    def add(self, index_val, value_val):
        if self.fmt == 'map':
            if self.prefix:
                # SPECIAL CASE: Prefix mode (w_s_avg)
                # We assume index_val is the integer index (0,1,2...) here
                key = f"{self.prefix}{int(index_val)}"
            else:
                # STANDARD CASE: Timestamp/Freq formatting
                key = float_to_padded_string(index_val, self.pad)

            self.data[key] = value_val

        elif self.fmt == 'tuple_array':
            self.data.append([index_val, value_val])
        elif self.fmt == 'dict_array':
            self.data.append({self.index_key: index_val, self.value_key: value_val})

    def get_result(self):
        return self.data


# Group keys by common configuration
TIME_KEYS = ['acc_x', 'acc_y', 'acc_z', 'gyr_x', 'gyr_y', 'gyr_z',
             'ain_a', 'ain_b', 'tamb', 'w_s', 'w_d']
FREQ_KEYS = ['fft', 'fft_x', 'fft_y', 'fft_z']

# 'idx' is the domain (time or freq), 'val' is the magnitude
FIELD_SCHEMA = {k: {'idx': 'time', 'val': 'val', 'pad': 5} for k in TIME_KEYS}
FIELD_SCHEMA.update({k: {'idx': 'freq', 'val': 'val', 'pad': 3} for k in FREQ_KEYS})
FIELD_SCHEMA['w_s_avg'] = {'idx': 'time', 'val': 'val', 'pad': 0, 'prefix': 'w_s_avg_'}

GROUPS = {
    'acc': {'keys': ['acc_x', 'acc_y', 'acc_z'], 'labels': ['x', 'y', 'z']},
    'gyr': {'keys': ['gyr_x', 'gyr_y', 'gyr_z'], 'labels': ['x', 'y', 'z']},
    'ain': {'keys': ['ain_a', 'ain_b'], 'labels': ['a', 'b']},
    'fft': {'keys': ['fft_x', 'fft_y', 'fft_z'], 'labels': ['x', 'y', 'z']},
}


def convert_item_format(item, target_format):
    """
    Converts a single item's fields from 'dict_array' to target_format.
    Supports: 'map', 'tuple_array', 'dict_array', 'combined_tuple', 'combined_dict'
    """
    if target_format == 'dict_array':
        return item

    converted = item.copy()
    processed_keys = set()

    # 1. Handle Combined Groups (acc, gyr, ain, fft)
    if target_format in ['combined_tuple', 'combined_dict']:
        for group_name, config in GROUPS.items():
            keys = config['keys']
            labels = config['labels']

            # Check if all keys for this group exist in the item
            if all(k in item for k in keys):
                # We assume all lists in the group are length-aligned
                first_key = keys[0]
                reference_list = item[first_key]

                # Dynamic Lookup: Is this group 'time' based or 'freq' based?
                domain_key = FIELD_SCHEMA[first_key]['idx']

                group_data = []

                for i in range(len(reference_list)):
                    # Extract domain value (time or freq)
                    domain_val = reference_list[i].get(domain_key)

                    # Extract values from all members
                    vals = [item[k][i].get('val') for k in keys]

                    if target_format == 'combined_tuple':
                        # Format: [time/freq, val_1, val_2, val_3]
                        row = [domain_val] + vals
                        group_data.append(row)
                    else:
                        # Format: {'time': t, 'x': val_1...} OR {'freq': f, 'x': val_1...}
                        row = {domain_key: domain_val}
                        for label, val in zip(labels, vals):
                            row[label] = val
                        group_data.append(row)

                converted[group_name] = group_data

                # Cleanup
                for k in keys:
                    converted.pop(k, None)
                    processed_keys.update(keys)

    # 2. Determine Sub-Format for remaining fields
    sub_format = target_format
    if target_format == 'combined_tuple':
        sub_format = 'tuple_array'
    elif target_format == 'combined_dict':
        sub_format = 'dict_array'

    # 3. Process remaining keys
    for key, val in item.items():
        if key in processed_keys or not isinstance(val, list) or key not in FIELD_SCHEMA:
            continue

        cfg = FIELD_SCHEMA[key]
        builder = DataBuilder(
            sub_format,
            index_key=cfg['idx'],
            value_key=cfg['val'],
            pad_zeros=cfg['pad'],
            prefix=cfg.get('prefix')
        )

        # FIX: If we are mapping with a prefix (like w_s_avg_), we want indices (0,1,2),
        # not the stored timestamp values.
        use_list_index = (sub_format == 'map' and cfg.get('prefix') is not None)

        for i, point in enumerate(val):
            val_v = point.get(cfg['val'])

            if use_list_index:
                idx_v = i  # w_s_avg_0, w_s_avg_1 ...
            else:
                idx_v = point.get(cfg['idx'])  # standard timestamp/freq

            if idx_v is not None and val_v is not None:
                builder.add(idx_v, val_v)

        converted[key] = builder.get_result()

    return converted