# utils/formatters.py

def float_to_padded_string(x, n_left_zeros):
    """
    Formats a float to a string with left-padded zeros on the integer part.
    """
    s = str(x)

    if 'e' in s.lower():
        s = f"{x:.20f}".rstrip('0')

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


def convert_to_tuples(item):
    """
    Iterates through an item's fields. If a field is a dictionary and its keys
    are parseable as floats (e.g. "0000.1"), converts it to a list of [key, value] lists.
    """
    new_item = item.copy()

    for key, val in new_item.items():
        if isinstance(val, dict) and val:
            # Check the first key to see if it's a number (e.g. "0000.02")
            # This avoids trying to convert structural dicts or things like "w_s_avg_0"
            first_k = next(iter(val))
            try:
                float(first_k)

                # It is a numeric map. Convert to list of lists.
                # [[1.2, 0.146...], [1.22, 0.153...]]
                tuple_list = [[float(k), v] for k, v in val.items()]

                # Ensure they are sorted numerically by the X-axis (time/freq)
                tuple_list.sort(key=lambda x: x[0])

                new_item[key] = tuple_list
            except ValueError:
                # Keys are not numbers (e.g. "w_s_avg_0"), keep as dict
                pass

    return new_item