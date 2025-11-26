import math
from utils.formatters import float_to_padded_string


def process(raw_items):
    processed_list = []

    for event in raw_items:
        # Assuming 'ain' is the key in the raw table (or check if it's 'axyz' reused)
        ain = [float(x) for x in event.get('ain', [])]
        scale = float(event.get('scale', 1))
        odr = float(event.get('odr', 1))
        time = int(event['time'])
        client_id = event['id']
        seq = int(event.get('seq', 1))

        message_i = seq - 1
        message_length = math.floor(len(ain) / 2)

        item = {
            'id': client_id,
            'time': time,
            'scale': scale,
            'odr': odr,
            'seq': seq
        }

        ain_a_data = {}
        ain_b_data = {}

        for i in range(message_length):
            ind = i + message_length * message_i
            t = ind / odr
            key = float_to_padded_string(t, 5)

            # CHANGED: Direct values
            ain_a_data[key] = ain[i * 2] * scale
            ain_b_data[key] = ain[i * 2 + 1] * scale

        item['ain_a'] = ain_a_data
        item['ain_b'] = ain_b_data

        processed_list.append(item)

    return processed_list