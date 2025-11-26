import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='map'):
    processed_list = []

    for event in raw_items:
        axyz = [float(x) for x in event['axyz']]
        scale = float(event['scale'])
        odr = float(event['odr'])
        time = int(event['time'])
        client_id = event['id']
        seq = int(event.get('seq', 1))

        # Optionals
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

        # 1. Build Temp/Wind (Scalar over time)
        t_base = message_i * message_length / odr

        # These are single points, but formatted as a collection of 1
        b_tamb = DataBuilder(fmt, 'time', 'val', 5)
        b_ws = DataBuilder(fmt, 'time', 'val', 5)
        b_wd = DataBuilder(fmt, 'time', 'val', 5)

        b_tamb.add(t_base, tamb)
        b_ws.add(t_base, w_s)
        b_wd.add(t_base, w_d)

        item['tamb'] = b_tamb.get_result()
        item['w_s'] = b_ws.get_result()
        item['w_d'] = b_wd.get_result()

        # 2. Build Accelerometer Arrays
        b_x = DataBuilder(fmt, 'time', 'val', 5)
        b_y = DataBuilder(fmt, 'time', 'val', 5)
        b_z = DataBuilder(fmt, 'time', 'val', 5)

        factor = scale * math.pow(2, -15)

        for i in range(message_length):
            ind = i + message_length * message_i
            t = ind / odr

            b_x.add(t, axyz[i * 3] * factor)
            b_y.add(t, axyz[i * 3 + 1] * factor)
            b_z.add(t, axyz[i * 3 + 2] * factor)

        item['acc_x'] = b_x.get_result()
        item['acc_y'] = b_y.get_result()
        item['acc_z'] = b_z.get_result()

        processed_list.append(item)

    return processed_list