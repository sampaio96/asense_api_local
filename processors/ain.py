import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='dict_array'):
    processed_list = []

    for event in raw_items:
        ain = [float(x) for x in event.get('ain', [])]
        scale = float(event.get('scale', 1))
        odr = float(event.get('odr', 1))
        # Current assumption: event['time'] is the timestamp of the LAST sample in this packet
        end_time_ms = int(event['time'])
        client_id = event['id']
        seq = int(event.get('seq', 1))

        message_length = math.floor(len(ain) / 2)

        item = {
            'id': client_id,
            'time': end_time_ms,
            'scale': scale,
            'odr': odr,
            'seq': seq
        }

        b_a = DataBuilder(fmt, 'time', 'val', 5)
        b_b = DataBuilder(fmt, 'time', 'val', 5)

        if odr > 0:
            period_ms = 1000.0 / odr
        else:
            period_ms = 0

        for i in range(message_length):
            steps_back = (message_length - 1) - i
            t = end_time_ms - (steps_back * period_ms)

            b_a.add(t, ain[i * 2] * scale)
            b_b.add(t, ain[i * 2 + 1] * scale)

        item['ain_a'] = b_a.get_result()
        item['ain_b'] = b_b.get_result()

        processed_list.append(item)

    return processed_list