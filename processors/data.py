# processors/data.py
import math
from datetime import datetime, timezone
from utils.formatters import DataBuilder


def process(raw_items, fmt='dict_array'):
    processed_list = []

    for event in raw_items:
        time_ms = int(event['time'])
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
            'time': time_ms,
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

        # Handle w_s_avg
        w_s_avg = event.get("w_s_avg", [0] * 6)
        if not isinstance(w_s_avg, list): w_s_avg = [0] * 6

        if fmt == 'map':
            # MAP format: keep explicit string keys (w_s_avg_0...5)
            w_s_avg_dict = {}
            for i, val in enumerate(w_s_avg):
                w_s_avg_dict[f"w_s_avg_{i}"] = float(val) * 0.01
            item['w_s_avg'] = w_s_avg_dict
        else:
            # ARRAY format: Calculate UTC timestamps relative to whole hour

            # 1. Convert timestamp to datetime (UTC)
            dt = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)

            # 2. Floor to nearest hour (18:45 -> 18:00)
            dt_floor = dt.replace(minute=0, second=0, microsecond=0)

            # 3. Get base timestamp for the last element (whole hour) in ms
            base_hour_ms = int(dt_floor.timestamp() * 1000)

            ws_len = len(w_s_avg)
            step_ms = 10 * 60 * 1000  # 10 minutes in ms

            b_ws = DataBuilder(fmt, index_key='time', value_key='val', pad_zeros=0)

            for i, val in enumerate(w_s_avg):
                # The last element (index = len-1) is the base_hour.
                # Previous elements are subtracted by 10 mins each step back.
                steps_back = (ws_len - 1) - i

                # Calculate specific UTC timestamp for this data point
                point_time = base_hour_ms - (steps_back * step_ms)

                b_ws.add(point_time, float(val) * 0.01)

            item['w_s_avg'] = b_ws.get_result()

        processed_list.append(item)

    return processed_list