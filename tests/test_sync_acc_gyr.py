import boto3
from boto3.dynamodb.conditions import Key
import os
import pandas as pd
from datetime import datetime, timezone
import calendar
import sys

# --- CONFIGURATION ---
# Set AWS Profile and Region
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

# Set to False to run for ALL IDs and ALL Months
TRIAL = False

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'acc_gyr_alignment_results.csv')

# IDs to process
DEVICE_IDS = [
    "ASENSE00000011",
    "ASENSE00000012",
    "ASENSE00000017",
    "ASENSE00000018",
    "ASENSE00000021",
    "ASENSE00000023",
    "ASENSE00000026",
    "ASENSE00000028"
]

# Table Name Templates
TABLE_NEW_FMT = "asense_table_{}"
TABLE_OLD_FMT = "asense_prototype_table_{}_preprocessed"

# Scenarios: (Year, Month, [List of Table Types to check])
SCENARIOS = [
    # (2025, 11, ['old']),  # Nov 25 -> Old only
    (2025, 12, ['new']),  # Dec 25 -> Both
    # (2026, 1, ['new'])  # Jan 26 -> New only
]

LIMIT = 20
MAX_DELTA_MS = 2000  # 2 seconds tolerance


def get_month_window(year, month):
    """Returns start and end timestamps (ms) for a given month."""
    dt_start = datetime(year, month, 1, tzinfo=timezone.utc)
    ts_start = int(dt_start.timestamp() * 1000)

    last_day = calendar.monthrange(year, month)[1]
    dt_end = datetime(year, month, last_day, 23, 59, 59, 999999, tzinfo=timezone.utc)
    ts_end = int(dt_end.timestamp() * 1000)

    return ts_start, ts_end


def query_last_items(table_name, device_id, start_ms, end_ms, limit):
    """Queries DynamoDB for the LAST 'limit' items in the time range."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key('id').eq(device_id) & Key('time').between(start_ms, end_ms),
            ScanIndexForward=False,  # Descending order (latest first)
            Limit=limit
        )
        items = response.get('Items', [])
        extracted = []
        for item in items:
            extracted.append({
                'seq': int(item.get('seq', -1)),
                'time': int(item.get('time', 0)),
                'time_broker': int(item.get('time_broker', 0)) if item.get('time_broker') is not None else None
            })

        extracted.sort(key=lambda x: x['seq'])
        return extracted
    except Exception as e:
        print(f"    [Error Querying {table_name}]: {e}")
        return []


def align_sequences(acc_data, gyr_data):
    """Aligns rows where seq matches AND abs(time_diff) < 2000ms."""
    aligned_rows = []

    # Create lookup for Gyr
    gyr_map = {row['seq']: row for row in gyr_data}

    for acc_row in acc_data:
        seq = acc_row['seq']
        if seq in gyr_map:
            gyr_row = gyr_map[seq]
            delta = abs(acc_row['time'] - gyr_row['time'])

            if delta < MAX_DELTA_MS:
                aligned_rows.append({
                    'seq': seq,
                    'acc_time': acc_row['time'],
                    'gyr_time': gyr_row['time'],
                    'delta_time': acc_row['time'] - gyr_row['time'],
                    'acc_broker': acc_row['time_broker'],
                    'gyr_broker': gyr_row['time_broker'],
                    'delta_broker': (acc_row['time_broker'] - gyr_row['time_broker']) if (
                                acc_row['time_broker'] and gyr_row['time_broker']) else None
                })
    return aligned_rows


def main():
    print(f"--- STARTING COMPARISON (Trial Mode: {TRIAL}) ---")
    print(f"--- OUTPUT FILE: {OUTPUT_FILE} ---")

    # Initialize CSV file with headers if it doesn't exist or we want to overwrite
    # Using 'w' to overwrite every time we start the script fresh
    cols = ['device_id', 'period', 'table_type', 'seq', 'acc_time', 'gyr_time', 'delta_time', 'acc_broker',
            'gyr_broker', 'delta_broker']
    pd.DataFrame(columns=cols).to_csv(OUTPUT_FILE, index=False)

    ids_to_run = DEVICE_IDS[:1] if TRIAL else DEVICE_IDS

    for device_id in ids_to_run:
        print(f"\n{'=' * 60}")
        print(f"DEVICE: {device_id}")
        print(f"{'=' * 60}")

        scenarios_to_run = SCENARIOS[:1] if TRIAL else SCENARIOS

        for year, month, table_types in scenarios_to_run:
            start_ms, end_ms = get_month_window(year, month)
            period_str = f"{year}-{month:02d}"
            print(f"\n📅 Period: {period_str}")

            for t_type in table_types:
                # Determine table names
                if t_type == 'new':
                    table_acc = TABLE_NEW_FMT.format('acc')
                    table_gyr = TABLE_NEW_FMT.format('gyr')
                else:
                    table_acc = TABLE_OLD_FMT.format('acc')
                    table_gyr = TABLE_OLD_FMT.format('gyr')

                print(f"   🔎 Checking [{t_type.upper()}] Tables...")

                # 1. Fetch Data
                acc_items = query_last_items(table_acc, device_id, start_ms, end_ms, LIMIT)
                gyr_items = query_last_items(table_gyr, device_id, start_ms, end_ms, LIMIT)

                if not acc_items or not gyr_items:
                    print(f"      ⚠️  Insufficient data (ACC={len(acc_items)}, GYR={len(gyr_items)})")
                    continue

                # 2. Align Data
                aligned = align_sequences(acc_items, gyr_items)

                if aligned:
                    print(f"      ✅ Found {len(aligned)} matches. Saving...")

                    df = pd.DataFrame(aligned)

                    # Add context columns
                    df['device_id'] = device_id
                    df['period'] = period_str
                    df['table_type'] = t_type

                    # Reorder cols
                    df = df[cols]

                    # Append to CSV
                    df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)

                    # Print preview
                    print(df[['seq', 'delta_time', 'delta_broker']].head(3).to_string(index=False))
                else:
                    print("      ❌ No matches found within tolerance.")

    print(f"\n\nDone! Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()