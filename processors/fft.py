import math
from utils.formatters import float_to_padded_string


def process(raw_items):
    processed_list = []

    for event in raw_items:
        fft_vals = [float(x) for x in event['fft']]
        scale = float(event['scale'])
        odr = float(event['odr'])
        axis = str(event['axis'])
        time = int(event['time'])
        client_id = event['id']

        message_length = math.floor(len(fft_vals))

        item = {
            'id': client_id,
            'time': time,
            'scale': scale,
            'odr': odr,
            'axis': axis
        }

        fft_data = {}
        factor = scale * math.pow(2, -15)

        for i in range(message_length):
            fft_frequency = i / 1024 * 50
            key = float_to_padded_string(fft_frequency, 3)

            # CHANGED: Direct values
            amplitude = fft_vals[i] * factor
            fft_data[key] = amplitude

        item['fft'] = fft_data
        processed_list.append(item)

    return processed_list