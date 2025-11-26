import boto3
from boto3.dynamodb.conditions import Key, Attr

# Initialize globally for reuse
dynamodb = boto3.resource('dynamodb')


def recurrent_query(table_name, key_condition, filter_exp=None, projection=None, names=None, index_name=None):
    table = dynamodb.Table(table_name)
    items = []

    params = {"KeyConditionExpression": key_condition}
    if filter_exp: params["FilterExpression"] = filter_exp
    if projection: params["ProjectionExpression"] = projection
    if names: params["ExpressionAttributeNames"] = names
    if index_name: params["IndexName"] = index_name

    while True:
        response = table.query(**params)
        items.extend(response.get('Items', []))
        if 'LastEvaluatedKey' not in response:
            break
        params["ExclusiveStartKey"] = response['LastEvaluatedKey']

    return items


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
        KeyConditionExpression=key_expression,
        FilterExpression=Attr('isReq').eq(False),
        ScanIndexForward=False,
        Limit=20
    )
    return response.get('Items', [])


def query_with_seq_filter(table_name, id_value, start_time, end_time, minute):
    """
    Uses the id-seq-index GSI to efficiently fetch blocks of data.
    """
    GSI_NAME = 'id-seq-index'
    NUM_SEQ_PER_BLOCK = 48
    start_seq = (minute * NUM_SEQ_PER_BLOCK) + 1
    end_seq = ((minute + 1) * NUM_SEQ_PER_BLOCK)

    # Query GSI for ID and Seq range
    key_condition = Key('id').eq(id_value) & Key('seq').between(start_seq, end_seq)

    # Filter results by time (since time isn't in the GSI key)
    filter_expr = Attr('time').between(start_time, end_time)

    # Fetch full items directly using the Index
    return recurrent_query(
        table_name,
        key_condition,
        filter_exp=filter_expr,
        index_name=GSI_NAME
    )


def query_standard(table_name, id_value, start_time, end_time):
    """Standard query for tables without the seq logic (fft, data)."""
    key_condition = Key('id').eq(id_value) & Key('time').between(start_time, end_time)
    return recurrent_query(table_name, key_condition)