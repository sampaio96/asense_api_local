import os
import boto3
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config

# --- CONFIGURATION ---
RUN_RANGES = [
    # {"start": "2025-12-01T00:00:00", "end": "2025-12-31T23:59:59"},
    {"start": "2026-01-01T00:00:00", "end": "2026-01-31T23:59:59"}
]

GRANULARITY_MIN = 60
TOPIC = 'data'
TABLE_NAME = "asense_table_data"
GSI_NAME = "id-time-index-only-keys"

TRIAL_RUN = False
TRIAL_ID = 3
MAX_WORKERS = 20

# Absolute path resolution to prevent tests/tests/ folders
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COVERAGE_DIR = os.path.join(BASE_DIR, "coverage")

BOTO_CONFIG = Config(
    region_name='eu-central-1',
    retries={'max_attempts': 10, 'mode': 'standard'}
)

os.environ['AWS_PROFILE'] = 'asense-iot'


# --- UTILS ---
def sanitize_iso(iso_str):
    return iso_str.replace("-", "").replace(":", "").replace("T", "_")


def iso_to_ms(iso_str):
    return int(datetime.fromisoformat(iso_str).replace(tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_block_start(ms, gran_min):
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    dt = dt.replace(minute=0, second=0, microsecond=0)
    return int(dt.timestamp() * 1000)


# --- WORKER FUNCTION ---
def scan_data_topic(device_id, start_ms, end_ms, gran_min):
    session = boto3.Session(profile_name='asense-iot')
    dynamodb = session.resource('dynamodb', config=BOTO_CONFIG)
    table = dynamodb.Table(TABLE_NAME)

    block_size_ms = gran_min * 60 * 1000
    coverage_map = {}

    curr_init = ms_to_block_start(start_ms, gran_min)
    while curr_init <= end_ms:
        coverage_map[curr_init] = 0
        curr_init += block_size_ms

    query_count = 0
    try:
        params = {
            'IndexName': GSI_NAME,
            'KeyConditionExpression': Key('id').eq(device_id) & Key('time').between(start_ms, end_ms),
            'ProjectionExpression': '#t',
            'ExpressionAttributeNames': {'#t': 'time'}
        }

        while True:
            query_count += 1
            response = table.query(**params)
            for item in response.get('Items', []):
                hit_time = int(item['time'])
                block_start = ms_to_block_start(hit_time, gran_min)
                if block_start in coverage_map:
                    coverage_map[block_start] = 1

            last_key = response.get('LastEvaluatedKey')
            if not last_key: break
            params['ExclusiveStartKey'] = last_key

    except Exception as e:
        print(f"  🛑 Error for {device_id}: {e}")

    rows = []
    for ts, status in coverage_map.items():
        rows.append({
            'device_id': device_id,
            'topic': TOPIC,
            'table_name': TABLE_NAME,
            'timestamp_ms': ts,
            'datetime': datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            'has_data': status
        })
    return rows, query_count, device_id


# --- MAIN RUNNER ---
def run_all():
    os.makedirs(COVERAGE_DIR, exist_ok=True)

    id_list = [f"ASENSE{str(TRIAL_ID).zfill(8)}"] if TRIAL_RUN else [f"ASENSE{str(i).zfill(8)}" for i in range(1, 37)]

    for time_range in RUN_RANGES:
        start_ms = iso_to_ms(time_range['start'])
        end_ms = iso_to_ms(time_range['end'])

        # Identifier for naming
        run_id_slug = f"FINAL_{sanitize_iso(time_range['start'])}_to_{sanitize_iso(time_range['end'])}_{GRANULARITY_MIN}min_{'TRIAL' if TRIAL_RUN else 'FLEET'}"

        all_results = []

        print(f"\n🚀 DATA TABLE SCAN | {time_range['start']} -> {time_range['end']}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scan_data_topic, dev, start_ms, end_ms, GRANULARITY_MIN) for dev in id_list]

            for i, future in enumerate(as_completed(futures)):
                rows, q_count, dev = future.result()
                all_results.extend(rows)
                print(f"  [{i + 1}/{len(id_list)}] ✅ {dev} | Found {sum(r['has_data'] for r in rows)} hours")

        df = pd.DataFrame(all_results)
        if df.empty: continue

        # Renamed CSV to include topic (DATA) so it doesn't overwrite report_...csv
        csv_path = os.path.join(COVERAGE_DIR, f"report_{TOPIC.upper()}_{run_id_slug}.csv")
        df.to_csv(csv_path, index=False)

        # Heatmap
        pivot_df = df.pivot(index="device_id", columns="datetime", values="has_data")
        fig = px.imshow(
            pivot_df,
            title=f"Coverage: DATA | New Table | {GRANULARITY_MIN}m",
            color_continuous_scale=['#1a1a1a', '#00CC96'],
            aspect="auto"
        )
        fig.update_coloraxes(showscale=False)
        fig.update_layout(template="plotly_dark")

        html_path = os.path.join(COVERAGE_DIR, f"heatmap_{TOPIC.upper()}_{run_id_slug}.html")
        fig.write_html(html_path)
        print(f"✅ CSV and Heatmap saved: {html_path}")


if __name__ == "__main__":
    run_all()