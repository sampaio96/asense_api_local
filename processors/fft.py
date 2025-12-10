import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='dict_array'):
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

        # Using 'freq' as label for dict_array
        b_fft = DataBuilder(fmt, 'freq', 'val', 3)
        factor = scale * math.pow(2, -15)

        for i in range(message_length):
            fft_frequency = i / 1024 * 50
            amplitude = fft_vals[i] * factor
            b_fft.add(fft_frequency, amplitude)

        item['fft'] = b_fft.get_result()
        processed_list.append(item)

    return processed_list