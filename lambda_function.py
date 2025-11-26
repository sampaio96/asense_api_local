import json
import datetime
import traceback
from db import access
from processors import factory
from utils import mergers


def lambda_handler(event, context):
    query_params = event.get('queryStringParameters') or {}

    # 1. Parse Parameters
    topic = query_params.get('table_name')
    id_value = query_params.get('id')
    start_time_str = query_params.get('start_time')
    end_time_str = query_params.get('end_time')
    minute_str = query_params.get('minute', '0')

    merge_by_hour = str(query_params.get('merge_by_hour', 'false')).lower() == 'true'

    # Output Format: 'map' (default), 'tuple_array', 'dict_array'
    output_format = query_params.get('output_format', 'map')
    valid_formats = ['map', 'tuple_array', 'dict_array']
    if output_format not in valid_formats:
        # Fallback or error? Let's fallback for safety
        output_format = 'map'

    if not all([topic, id_value]):
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required parameters'})}

    # 2. Route Health Check
    if topic == 'health':
        start = int(start_time_str) if start_time_str else None
        end = int(end_time_str) if end_time_str else None
        try:
            result = access.query_health_status('asense_table_req_resp', id_value, start, end)
            timestamps = [int(item['time']) for item in result]
            dt_strings = [datetime.datetime.utcfromtimestamp(ts / 1000).isoformat() + 'Z' for ts in timestamps]
            return {'statusCode': 200, 'body': json.dumps({'timestamps': timestamps, 'datetime_strings': dt_strings})}
        except Exception as e:
            return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

    # 3. Validate Time
    try:
        start_time = int(start_time_str)
        end_time = int(end_time_str)
        minute = int(minute_str)
    except (ValueError, TypeError):
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid time parameters'})}

    # 4. Map Topic
    table_name = f"asense_table_{topic}"

    try:
        # 5. Fetch
        raw_items = []
        is_seq_table = topic in ['acc', 'gyr', 'ain']

        if is_seq_table:
            raw_items = access.query_with_seq_filter(table_name, id_value, start_time, end_time, minute)
        else:
            raw_items = access.query_standard(table_name, id_value, start_time, end_time)

        if not raw_items:
            return {'statusCode': 200, 'body': json.dumps([])}

        # 6. Process Data (Pass output_format)
        processor = factory.get_processor(topic)
        if not processor:
            return {'statusCode': 400, 'body': json.dumps({'error': f'Unknown topic: {topic}'})}

        # Inject format here!
        processed_items = processor.process(raw_items, fmt=output_format)

        # 7. Sort
        processed_items.sort(key=lambda x: (x.get('time', 0), x.get('seq', 0)))

        # 8. Merge
        if merge_by_hour:
            if topic == 'fft':
                processed_items = mergers.merge_fft_axes_by_hour(processed_items)
            else:
                processed_items = mergers.merge_items_by_hour(processed_items)

        # 9. Final Formatting (Dates)
        final_list = []
        for item in processed_items:
            if 'time' in item:
                item['datetime'] = datetime.datetime.utcfromtimestamp(item['time'] / 1000).isoformat() + 'Z'
            final_list.append(dict(sorted(item.items())))

        return {
            'statusCode': 200,
            'body': json.dumps(final_list)
        }

    except Exception as e:
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"})
        }