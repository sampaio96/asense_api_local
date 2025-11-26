import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='map'):
    processed_list = []

    for event in raw_items:
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

        b_a = DataBuilder(fmt, 'time', 'val', 5)
        b_b = DataBuilder(fmt, 'time', 'val', 5)

        for i in range(message_length):
            ind = i + message_length * message_i
            t = ind / odr

            b_a.add(t, ain[i * 2] * scale)
            b_b.add(t, ain[i * 2 + 1] * scale)

        item['ain_a'] = b_a.get_result()
        item['ain_b'] = b_b.get_result()

        processed_list.append(item)

    return processed_list