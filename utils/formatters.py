# utils/formatters.py

def float_to_padded_string(x, n_left_zeros):
    """Formats a float to a string with left-padded zeros."""
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


class DataBuilder:
    def __init__(self, output_format, x_label='time', y_label='val', pad_zeros=5):
        self.fmt = output_format
        self.x_label = x_label
        self.y_label = y_label
        self.pad = pad_zeros

        # Initialize container based on format
        if self.fmt == 'map':
            self.data = {}
        else:
            self.data = []

    def add(self, x, y):
        """Adds a point (x, y) to the container respecting the format."""
        if self.fmt == 'map':
            # Only use string padding here!
            key = float_to_padded_string(x, self.pad)
            self.data[key] = y
        elif self.fmt == 'tuple_array':
            self.data.append([x, y])
        elif self.fmt == 'dict_array':
            self.data.append({self.x_label: x, self.y_label: y})

    def get_result(self):
        return self.data