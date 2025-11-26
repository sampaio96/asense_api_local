import math
from utils.formatters import DataBuilder


def process(raw_items, fmt='map'):
    processed_list = []

    for event in raw_items:
        time = int(event['time'])
        client_id = event['id']
        scale = float(event['scale'])
        odr = float(event['odr'])

        def get_f(key, default=0):
            return float(event.get(key, default))

        f_acc = scale * math.pow(2, -24)
        f_inc = math.pow(2, -24)
        f_spec = odr / 1024
        f_mag = scale * math.pow(2, -15)

        item = {
            'id': client_id,
            'time': time,
            'scale': scale,
            'odr': odr,
            'aavgx': get_f("aavgx") * f_acc,
            'aavgy': get_f("aavgy") * f_acc,
            'aavgz': get_f("aavgz") * f_acc,
            'amaxx': get_f("amaxx") * f_acc,
            'amaxy': get_f("amaxy") * f_acc,
            'amaxz': get_f("amaxz") * f_acc,
            'aminx': get_f("aminx") * f_acc,
            'aminy': get_f("aminy") * f_acc,
            'aminz': get_f("aminz") * f_acc,
            'theta': get_f("theta") * f_inc,
            'phi': get_f("phi") * f_inc,
            'nx1': get_f("nx1") * f_spec,
            'nx2': get_f("nx2") * f_spec,
            'ny1': get_f("ny1") * f_spec,
            'ny2': get_f("ny2") * f_spec,
            'nz1': get_f("nz1") * f_spec,
            'nz2': get_f("nz2") * f_spec,
            'mx1': get_f("mx1") * f_mag,
            'mx2': get_f("mx2") * f_mag,
            'my1': get_f("my1") * f_mag,
            'my2': get_f("my2") * f_mag,
            'mz1': get_f("mz1") * f_mag,
            'mz2': get_f("mz2") * f_mag,
            'in_a': get_f('in_a'),
            'in_b': get_f('in_b'),
            'lat': get_f('lat'),
            'long': get_f('long'),
            'tamb': get_f('tamb'),
            'w_s': get_f('w_s') * 0.01,
            'w_d': get_f('w_d') * 360 / 16,
        }

        # Flatten w_s_avg
        # Note: For 'map', we use strict "w_s_avg_0" keys.
        # For arrays, we use 0, 1, 2 as keys/x-values.
        w_s_avg = event.get("w_s_avg", [0] * 6)
        if not isinstance(w_s_avg, list): w_s_avg = [0] * 6

        if fmt == 'map':
            w_s_avg_dict = {}
            for i, val in enumerate(w_s_avg):
                w_s_avg_dict[f"w_s_avg_{i}"] = float(val) * 0.01
            item['w_s_avg'] = w_s_avg_dict
        else:
            # Using 'index' as label for dict_array
            b_ws = DataBuilder(fmt, 'index', 'val', 0)
            for i, val in enumerate(w_s_avg):
                b_ws.add(i, float(val) * 0.01)
            item['w_s_avg'] = b_ws.get_result()

        processed_list.append(item)

    return processed_list