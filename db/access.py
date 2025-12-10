import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal

# Initialize globally for reuse
dynamodb = boto3.resource('dynamodb')


def query_health_status(table_name, id_value, start_time=None, end_time=None):
    table = dynamodb.Table(table_name)
    key_expr = Key('id').eq(id_value)

    if start_time and end_time:
        key_expr &= Key('time').between(start_time, end_time)
    elif start_time:
        key_expr &= Key('time').ge(start_time)
    elif end_time:
        key_expr &= Key('time').le(end_time)

    response = table.query(
        KeyConditionExpression=key_expr,
        FilterExpression=Attr('isReq').eq(False),
        ScanIndexForward=False,
        Limit=20
    )
    return response.get('Items', [])


def query_paginated(table_name, id_value, start_time, end_time, limit=32):
    """
    Queries DynamoDB using ID + Time index.
    Accumulates items until 'limit' is reached or data is exhausted.
    Returns (items_list, next_timestamp_or_none).
    """
    table = dynamodb.Table(table_name)

    key_condition = Key('id').eq(id_value) & Key('time').between(start_time, end_time)

    items = []
    last_key = None
    params = {'KeyConditionExpression': key_condition, 'Limit': limit}

    while True:
        # Update limit to fetch only what we need to reach the target
        needed = limit - len(items)
        if needed <= 0:
            break

        params['Limit'] = needed

        response = table.query(**params)
        batch = response.get('Items', [])
        items.extend(batch)

        last_key = response.get('LastEvaluatedKey')

        # 1. No more data in DB matching query -> Break
        if not last_key:
            break

        # 2. Update start key for next iteration
        params['ExclusiveStartKey'] = last_key

    # Extract next_timestamp only if we have a continuation key
    # AND we actually have items (safety check)
    next_timestamp = None
    if last_key and 'time' in last_key:
        # ADD +1 TO AVOID OVERLAP
        next_timestamp = int(last_key['time']) + 1

    return items, next_timestamp


def query_timestamps_only(table_name, id_value, start_time, end_time):
    """
    Queries the 'id-time-index-only-keys' GSI.
    Returns a list of integer timestamps.
    Does NOT enforce a 2048 limit (fetches all keys in range).
    """
    table = dynamodb.Table(table_name)

    # GSI Name provided by you
    INDEX_NAME = 'id-time-index-only-keys'

    key_condition = Key('id').eq(id_value) & Key('time').between(start_time, end_time)

    timestamps = []
    params = {
        'IndexName': INDEX_NAME,
        'KeyConditionExpression': key_condition,
        # We only project 'time' to keep network payload from DB small
        'ProjectionExpression': '#t',
        'ExpressionAttributeNames': {'#t': 'time'}
    }

    while True:
        response = table.query(**params)

        # Extract just the time integer
        batch = [int(item['time']) for item in response.get('Items', [])]
        timestamps.extend(batch)

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
        params['ExclusiveStartKey'] = last_key

    return timestamps