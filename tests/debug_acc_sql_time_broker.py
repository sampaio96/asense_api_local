import boto3
import csv
from boto3.dynamodb.conditions import Key

# --- CONFIGURATION ---
PROFILE_NAME = 'asense-iot'
REGION = 'eu-central-1'
TABLE_NAME = 'asense_table_acc'

PARTITION_KEY_VAL = 'ASENSE00000005'
TIME_START = 1764943200000

INTERVAL_3_MIN = 5 * 60 * 1000
INTERVAL_1_HR = 60 * 60 * 1000
INTERVAL_24_HR = 24 * 60 * 60 * 1000
TIME_END = TIME_START + INTERVAL_1_HR

# False = Get only id, time, time_broker (Fastest, minimal data)
# True  = Get ALL attributes for every row (Slower, larger file)
FETCH_ALL_ATTRIBUTES = True

# Construct filename based on mode
suffix = '_FULL_DATA.csv' if FETCH_ALL_ATTRIBUTES else '_time_broker.csv'
OUTPUT_FILE = f"csvs/{PARTITION_KEY_VAL}_{TABLE_NAME}_{TIME_START}_{TIME_END}{suffix}"


def export_dynamo_to_csv():
    # 1. Setup Session
    session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION)
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    print(f"Connected to table: {TABLE_NAME} in {REGION}")
    print(f"Mode: {'FETCHING ALL ATTRIBUTES' if FETCH_ALL_ATTRIBUTES else 'FETCHING SPECIFIC COLUMNS'}")
    print(f"Starting export for ID: {PARTITION_KEY_VAL}...")

    # 2. Prepare CSV file
    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        writer = None  # We will initialize this after we get the first batch of data

        last_evaluated_key = None
        total_items = 0

        while True:
            # 3. Construct the Query
            query_kwargs = {
                'KeyConditionExpression': Key('id').eq(PARTITION_KEY_VAL) & Key('time').between(TIME_START, TIME_END),
            }

            # IF WE ONLY WANT SPECIFIC COLUMNS:
            if not FETCH_ALL_ATTRIBUTES:
                query_kwargs['ProjectionExpression'] = '#id, #t, time_broker'
                query_kwargs['ExpressionAttributeNames'] = {'#id': 'id', '#t': 'time'}

            # (If fetching all, we simply do not add ProjectionExpression)

            # Handle Pagination
            if last_evaluated_key:
                query_kwargs['ExclusiveStartKey'] = last_evaluated_key

            # 4. Execute Query
            try:
                response = table.query(**query_kwargs)
            except Exception as e:
                print(f"Error querying DynamoDB: {e}")
                break

            items = response.get('Items', [])

            if not items:
                print("No items found in this batch.")
                if total_items == 0:
                    print("Query returned 0 results total.")
                    break

            # 5. Initialize CSV Writer (Lazy Initialization)
            # We do this inside the loop because if FETCH_ALL is True, we don't know
            # the column names until we see the first item.
            if writer is None:
                if FETCH_ALL_ATTRIBUTES:
                    # Dynamically get headers from the keys of the first item
                    # (Assumes all rows have similar schema)
                    fieldnames = list(items[0].keys())
                    # Ensure id and time are first for readability
                    if 'id' in fieldnames:
                        fieldnames.insert(0, fieldnames.pop(fieldnames.index('id')))
                    if 'time' in fieldnames:
                        fieldnames.insert(1, fieldnames.pop(fieldnames.index('time')))
                else:
                    # Hardcoded headers for specific mode
                    fieldnames = ['id', 'time', 'time_broker']

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

            # 6. Write batch to CSV
            for item in items:
                if FETCH_ALL_ATTRIBUTES:
                    # Write row as-is (Boto3 Decimals convert to string automatically in CSV)
                    writer.writerow(item)
                else:
                    # Clean data manually for the specific 3 columns
                    writer.writerow({
                        'id': item.get('id'),
                        'time': int(item.get('time')),
                        'time_broker': int(item.get('time_broker')) if item.get('time_broker') is not None else ''
                    })

            total_items += len(items)
            print(f"Fetched {len(items)} items (Total: {total_items})...")

            # Check if there is more data
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break

    print(f"Done! Exported {total_items} rows to {OUTPUT_FILE}")


if __name__ == '__main__':
    export_dynamo_to_csv()