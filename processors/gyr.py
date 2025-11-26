import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='map'):
    processed_list = []

    for event in raw_items:
        gxyz = [float(x) for x in event['gxyz']]
        scale_raw = int(event['scale'])
        odr = float(event['odr'])
        time = int(event['time'])
        client_id = event['id']
        seq = int(event.get('seq', 1))

        scale = float(scale_raw)
        if scale_raw < 14:
            scale = 2000 / math.pow(2, scale_raw)
        elif scale_raw == 15:
            scale = 2000 / math.pow(2, 7)
        elif scale_raw == 31:
            scale = 2000 / math.pow(2, 6)

        message_i = seq - 1
        message_length = math.floor(len(gxyz) / 3)

        item = {
            'id': client_id,
            'time': time,
            'scale': scale,
            'odr': odr,
            'seq': seq
        }

        b_x = DataBuilder(fmt, 'time', 'val', 5)
        b_y = DataBuilder(fmt, 'time', 'val', 5)
        b_z = DataBuilder(fmt, 'time', 'val', 5)

        factor = scale * math.pow(2, -15)

        for i in range(message_length):
            ind = i + message_length * message_i
            t = ind / odr

            b_x.add(t, gxyz[i * 3] * factor)
            b_y.add(t, gxyz[i * 3 + 1] * factor)
            b_z.add(t, gxyz[i * 3 + 2] * factor)

        item['gyr_x'] = b_x.get_result()
        item['gyr_y'] = b_y.get_result()
        item['gyr_z'] = b_z.get_result()

        processed_list.append(item)

    return processed_list