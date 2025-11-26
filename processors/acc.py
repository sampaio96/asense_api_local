import math
from utils.formatters import float_to_padded_string


def process(raw_items):
    processed_list = []

    for event in raw_items:
        axyz = [float(x) for x in event['axyz']]
        scale = float(event['scale'])
        odr = float(event['odr'])
        time = int(event['time'])
        client_id = event['id']
        seq = int(event.get('seq', 1))

        # Optional fields
        tamb = float(event.get('tamb', 0))
        w_s = float(event.get('w_s', 0)) * 0.01
        w_d = float(event.get('w_d', 0)) * 360 / 16

        message_i = seq - 1
        message_length = math.floor(len(axyz) / 3)

        item = {
            'id': client_id,
            'time': time,
            'scale': scale,
            'odr': odr,
            'seq': seq
        }

        # Temperature & Wind (Option 2)
        t_base = message_i * message_length / odr
        key_base = float_to_padded_string(t_base, 5)

        # CHANGED: Direct values
        item['tamb'] = {key_base: tamb}
        item['w_s'] = {key_base: w_s}
        item['w_d'] = {key_base: w_d}

        # Accelerometer Data
        acc_x_data = {}
        acc_y_data = {}
        acc_z_data = {}

        factor = scale * math.pow(2, -15)

        for i in range(message_length):
            ind = i + message_length * message_i
            t = ind / odr
            key = float_to_padded_string(t, 5)

            # CHANGED: Direct values
            acc_x_data[key] = axyz[i * 3] * factor
            acc_y_data[key] = axyz[i * 3 + 1] * factor
            acc_z_data[key] = axyz[i * 3 + 2] * factor

        item['acc_x'] = acc_x_data
        item['acc_y'] = acc_y_data
        item['acc_z'] = acc_z_data

        processed_list.append(item)

    return processed_list