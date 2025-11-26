import math
from utils.formatters import float_to_padded_string


def process(raw_items):
    processed_list = []

    for event in raw_items:
        # 1. Parse Inputs
        # DynamoDB usually returns Decimals for numbers, cast to float
        gxyz = [float(x) for x in event['gxyz']]
        scale_raw = int(event['scale'])
        odr = float(event['odr'])
        time = int(event['time'])
        client_id = event['id']
        seq = int(event.get('seq', 1))

        # 2. Fix Scale (Logic from gyr_preprocessing)
        scale = float(scale_raw)
        if scale_raw < 14:
            scale = 2000 / math.pow(2, scale_raw)
        elif scale_raw == 15:
            scale = 2000 / math.pow(2, 7)
        elif scale_raw == 31:
            scale = 2000 / math.pow(2, 6)

        message_i = seq - 1
        message_length = math.floor(len(gxyz) / 3)

        # 3. Base Item
        item = {
            'id': client_id,
            'time': time,
            'scale': scale,
            'odr': odr,
            'seq': seq
        }

        # 4. Process Axis Data
        gyr_x_data = {}
        gyr_y_data = {}
        gyr_z_data = {}

        factor = scale * math.pow(2, -15)

        for i in range(message_length):
            ind = i + message_length * message_i
            t = ind / odr
            key = float_to_padded_string(t, 5)

            # CHANGED: Assign float directly, removed {'N': str(...)}
            gyr_x_data[key] = gxyz[i * 3] * factor
            gyr_y_data[key] = gxyz[i * 3 + 1] * factor
            gyr_z_data[key] = gxyz[i * 3 + 2] * factor

        item['gyr_x'] = gyr_x_data
        item['gyr_y'] = gyr_y_data
        item['gyr_z'] = gyr_z_data

        processed_list.append(item)

    return processed_list