import json
import datetime
import traceback
from db import access
from processors import factory
from utils import mergers, formatters, corrector


def lambda_handler(event, context):
    print(f"Received Event: {json.dumps(event)}")  # DEBUG LOG

    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key,Authorization',
        'Access-Control-Allow-Methods': 'OPTIONS,GET'
    }

    query_params = event.get('queryStringParameters') or {}

    # 1. Parse Parameters
    topic = query_params.get('table_name')
    id_value = query_params.get('id')
    start_time_str = query_params.get('start_time')
    end_time_str = query_params.get('end_time')

    # Feature Flags
    timestamps_only = str(query_params.get('timestamps_only', 'false')).lower() == 'true'
    merge_param = str(query_params.get('merge', 'true')).lower()
    merge = merge_param != 'false'

    # Feature Flag for Correction
    enable_correction_param = str(query_params.get('enable_correction', 'true')).lower()
    enable_correction = enable_correction_param != 'false'

    # Output Format: 'map' (default), 'tuple_array', 'dict_array', 'combined_tuple', 'combined_dict'
    output_format = query_params.get('output_format', 'map')
    valid_formats = ['map', 'tuple_array', 'dict_array', 'combined_tuple', 'combined_dict']
    if output_format not in valid_formats:
        output_format = 'map'

    if not all([topic, id_value]):
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Missing required parameters'})
        }

    # 2. Route Health Check
    if topic == 'health':
        start = int(start_time_str) if start_time_str else None
        end = int(end_time_str) if end_time_str else None
        try:
            result = access.query_health_status('asense_table_req_resp', id_value, start, end)
            timestamps = [int(item['time']) for item in result]
            dt_strings = [datetime.datetime.utcfromtimestamp(ts / 1000).isoformat() + 'Z' for ts in timestamps]
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'timestamps': timestamps, 'datetime_strings': dt_strings})
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': str(e)})
            }

    # 3. Validate Time
    try:
        start_time = int(start_time_str)
        end_time = int(end_time_str)
    except (ValueError, TypeError):
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Invalid time parameters'})
        }

    # 4. Map Topic
    table_name = f"asense_table_{topic}"

    try:
        # --- TIMESTAMPS ONLY ROUTE ---
        if timestamps_only:
            if topic != 'data':
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': "'timestamps_only' is currently supported for 'data' table only."})
                }

            # Fetch using the GSI, no limit
            ts_list = access.query_timestamps_only(table_name, id_value, start_time, end_time)
            print(f"Timestamps found: {len(ts_list)}")  # DEBUG LOG

            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'timestamps': ts_list, 'count': len(ts_list)})
            }

        # 5. Standard Fetch with Pagination
        raw_items, next_timestamp = access.query_paginated(table_name, id_value, start_time, end_time)

        print(f"Items fetched: {len(raw_items)}")  # DEBUG LOG

        if not raw_items:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'data': []})
            }

        # 6. Process Data
        processor = factory.get_processor(topic)
        if not processor:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Unknown topic: {topic}'})
            }

        processed_items = processor.process(raw_items)  # use the internal format ('dict_array')

        # 7. Sort (Critical for delta calculation)
        processed_items.sort(key=lambda x: x.get('time', 0))

        # 8. Correct Timestamps
        # We apply correction ONLY to high-freq sensor data where this 1280ms packet logic applies.
        if enable_correction and topic in ['acc', 'gyr', 'ain']:
             print(f"--- Applying Timestamp Correction (Batch: {len(processed_items)}) ---")
             processed_items = corrector.apply_correction(processed_items)
        else:
             print(f"--- Timestamp Correction SKIPPED (Enabled: {enable_correction}, Topic: {topic}) ---")

        # 9. Merge
        if merge:
            if topic == 'data':
                # do not merge
                pass
            elif topic == 'fft':
                processed_items = mergers.merge_fft_axes_by_hour(processed_items)
            else:
                if processed_items:
                    processed_items = [mergers.merge_items_in_group(processed_items)]
                else:
                    processed_items = []
                # Old function: merge by hour. No longer needed since we don't rely on seq number to calculate timestamps.
                # The previous method required the seq number to be unique within the hour, and possibly to reset each hour.
                # processed_items = mergers.merge_items_by_hour(processed_items)

        # 10. Final Formatting (Enrich & Cleanup)
        final_list = []
        for item in processed_items:
            # A. Generate Datetime String (The "Anchor")
            # We do this BEFORE popping 'time'.
            if 'time' in item:
                item['datetime'] = datetime.datetime.utcfromtimestamp(item['time'] / 1000).isoformat() + 'Z'

            # B. Cleanup Metadata
            # We remove variable fields to clean up the root object, but keep 'datetime' as the human-readable anchor.
            if topic in ['acc', 'gyr', 'ain']:
                item.pop('time', None)

            if merge:
                # Remove fields not needed after merging
                for field in ['seq', 'odr', 'scale']:
                    item.pop(field, None)

            # C. CONVERT FORMAT (The new step)
            # This transforms the dict_arrays into map/tuple_array if requested
            formatted_item = formatters.convert_item_format(item, output_format)

            final_list.append(dict(sorted(formatted_item.items())))

        # 11. Construct Response
        response_body = {
            'data': final_list
        }

        if next_timestamp:
            response_body['next_timestamp'] = next_timestamp

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(response_body)
        }

    except Exception as e:
        traceback.print_exc()
        print(f"ERROR: {str(e)}")  # DEBUG LOG
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"})
        }