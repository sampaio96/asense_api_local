# tests/time_utils.py
from datetime import datetime
from pytz import timezone
import tests.config as config

def convert_datetime_between_timezones(dt, input_timezone, output_timezone):
    input_tz = timezone(input_timezone)
    output_tz = timezone(output_timezone)
    if dt.tzinfo is None:
        input_dt = input_tz.localize(dt)
    else:
        input_dt = dt
    return input_dt.astimezone(output_tz)

def local_datetime_to_unix_milliseconds(local_datetime_str):
    """
    Converts "YYYY-MM-DD HH:MM:SS" (Local Configured TZ) to Unix Epoch MS.
    """
    dt = datetime.strptime(local_datetime_str, "%Y-%m-%d %H:%M:%S")
    utc_dt = convert_datetime_between_timezones(dt, config.LOCAL_TIMEZONE, 'UTC')
    return int(utc_dt.timestamp() * 1000)