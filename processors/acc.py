import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='dict_array'):
    processed_list = []

    for event in raw_items:
        axyz = [float(x) for x in event['axyz']]
        scale = float(event['scale'])
        odr = float(event['odr'])
        # Current assumption: event['time'] is the timestamp of the LAST sample in this packet
        end_time_ms = int(event['time'])
        client_id = event['id']

        # We still keep seq for debugging or sorting if needed, but NOT for time calc
        seq = int(event.get('seq', 1))

        # Optionals
        tamb = float(event.get('tamb', 0))
        w_s = float(event.get('w_s', 0)) * 0.01
        w_d = float(event.get('w_d', 0)) * 360 / 16

        # 3 axes, so length is total / 3
        message_length = math.floor(len(axyz) / 3)

        item = {
            'id': client_id,
            'time': end_time_ms,
            'scale': scale,
            'odr': odr,
            'seq': seq
        }

        # 1. Build Temp/Wind (Scalar)
        # These are recorded at the time of the packet (end_time)
        b_tamb = DataBuilder(fmt, 'time', 'val', 5)
        b_ws = DataBuilder(fmt, 'time', 'val', 5)
        b_wd = DataBuilder(fmt, 'time', 'val', 5)

        b_tamb.add(end_time_ms, tamb)
        b_ws.add(end_time_ms, w_s)
        b_wd.add(end_time_ms, w_d)

        item['tamb'] = b_tamb.get_result()
        item['w_s'] = b_ws.get_result()
        item['w_d'] = b_wd.get_result()

        # 2. Build Accelerometer Arrays
        b_x = DataBuilder(fmt, 'time', 'val', 5)
        b_y = DataBuilder(fmt, 'time', 'val', 5)
        b_z = DataBuilder(fmt, 'time', 'val', 5)

        factor = scale * math.pow(2, -15)

        # Calculate sample period in milliseconds
        # ODR is in Hz (samples per second). 1000 / ODR = ms per sample.
        if odr > 0:
            period_ms = 1000.0 / odr
        else:
            period_ms = 0

        for i in range(message_length):
            # i=0 is the oldest sample in this packet
            # i=(message_length-1) is the newest sample (at end_time_ms)

            # Number of steps back from the end
            steps_back = (message_length - 1) - i

            t = end_time_ms - (steps_back * period_ms)

            b_x.add(t, axyz[i * 3] * factor)
            b_y.add(t, axyz[i * 3 + 1] * factor)
            b_z.add(t, axyz[i * 3 + 2] * factor)

        item['acc_x'] = b_x.get_result()
        item['acc_y'] = b_y.get_result()
        item['acc_z'] = b_z.get_result()

        processed_list.append(item)

    return processed_list