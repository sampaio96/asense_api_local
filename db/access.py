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


def query_paginated(table_name, id_value, start_time, end_time, limit=2048):
    """
    Queries DynamoDB using ID + Time index.
    Accumulates items until 'limit' is reached or data is exhausted.
    Returns (items_list, next_timestamp_or_none).
    """
    table = dynamodb.Table(table_name)

    key_condition = Key('id').eq(id_value) & Key('time').between(start_time, end_time)

    items = []
    last_key = None

    params = {
        'KeyConditionExpression': key_condition,
        # We set an initial limit, but we must update it in the loop
        # to avoid over-fetching if we only need a few more items.
        'Limit': limit
    }

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
        next_timestamp = int(last_key['time'])

    return items, next_timestamp