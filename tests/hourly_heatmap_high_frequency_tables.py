import os
import boto3
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config

# --- CONFIGURATION ---
START_ISO = "2026-01-01T00:00:00"
END_ISO = "2026-01-31T23:59:59"
GRANULARITY_MIN = 60

USE_PROTOTYPE = False
TABLE_PREFIX = "asense_prototype_table_" if USE_PROTOTYPE else "asense_table_"
TABLE_SUFFIX = "_preprocessed" if USE_PROTOTYPE else ""
TOPICS = ['acc', 'gyr', 'ain']

TRIAL_RUN = False
TRIAL_ID = 3

# --- NETWORK & RETRY CONFIG ---
# This prevents the "frozen" state by forcing a timeout and retrying 10 times.
BOTO_CONFIG = Config(
    region_name='eu-central-1',
    connect_timeout=5,  # 5 seconds to connect
    read_timeout=10,  # 10 seconds to receive data
    retries={
        'max_attempts': 10,  # Retry 10 times on network failure
        'mode': 'standard'
    }
)

os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'


# --- UTILS ---
def sanitize_iso(iso_str):
    return iso_str.replace("-", "").replace(":", "").replace("T", "_")


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def iso_to_ms(iso_str):
    return int(datetime.fromisoformat(iso_str).replace(tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_block_start(ms, gran_min):
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    if gran_min >= 60:
        hours_delta = dt.hour % (gran_min // 60) if gran_min > 60 else 0
        dt = dt.replace(hour=dt.hour - hours_delta, minute=0, second=0, microsecond=0)
    else:
        dt = dt.replace(minute=(dt.minute // gran_min) * gran_min, second=0, microsecond=0)
    return int(dt.timestamp() * 1000)


# --- WORKER FUNCTION ---
def scan_device_topic(device_id, topic, start_ms, end_ms, gran_min, table_params):
    """Worker task that handles the jumping logic for one (Device, Topic) pair."""
    # Create session and resource inside the thread for thread-safety
    session = boto3.Session(profile_name='asense-iot')
    dynamodb = session.resource('dynamodb', config=BOTO_CONFIG)

    table_name = f"{table_params['prefix']}{topic}{table_params['suffix']}"
    table = dynamodb.Table(table_name)

    block_size_ms = gran_min * 60 * 1000

    # Pre-build local map
    local_rows = []
    coverage_map = {}
    curr_init = ms_to_block_start(start_ms, gran_min)
    while curr_init <= end_ms:
        coverage_map[curr_init] = 0
        curr_init += block_size_ms

    search_pointer = start_ms
    query_count = 0

    while search_pointer <= end_ms:
        query_count += 1
        try:
            response = table.query(
                KeyConditionExpression=Key('id').eq(device_id) & Key('time').between(search_pointer, end_ms),
                Limit=1,
                ProjectionExpression="#t",
                ExpressionAttributeNames={"#t": "time"}
            )
            items = response.get('Items', [])
            if not items:
                break

            hit_time = int(items[0]['time'])
            block_start = ms_to_block_start(hit_time, gran_min)
            coverage_map[block_start] = 1
            search_pointer = block_start + block_size_ms

        except Exception as e:
            # If after 10 retries it still fails, we log it and exit this specific worker
            print(f"\n  🛑 PERMANENT ERROR for {device_id} {topic}: {e}")
            break

    # Format result rows
    for ts, status in coverage_map.items():
        local_rows.append({
            'device_id': device_id,
            'topic': topic,
            'table_name': table_name,
            'timestamp_ms': ts,
            'datetime': datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            'has_data': status
        })
    return local_rows, query_count, topic, device_id


# --- MAIN RUNNER ---
def check_coverage_parallel():
    os.makedirs("coverage", exist_ok=True)

    start_ms = iso_to_ms(START_ISO)
    end_ms = iso_to_ms(END_ISO)

    table_mode_slug = "PROTO" if USE_PROTOTYPE else "FINAL"
    id_list = [f"ASENSE{str(TRIAL_ID).zfill(8)}"] if TRIAL_RUN else [f"ASENSE{str(i).zfill(8)}" for i in range(1, 37)]

    run_id_slug = f"{table_mode_slug}_{sanitize_iso(START_ISO)}_to_{sanitize_iso(END_ISO)}_{GRANULARITY_MIN}min_{'TRIAL' if TRIAL_RUN else 'FLEET'}"
    table_params = {'prefix': TABLE_PREFIX, 'suffix': TABLE_SUFFIX}

    all_results = []
    total_queries = 0

    # We set max_workers to the length of tasks so every ID/Topic pair starts immediately.
    tasks_count = len(id_list) * len(TOPICS)

    print(f"\n🚀 STARTING MASSIVELY PARALLEL SCAN")
    print(f"Tasks: {tasks_count} | Mode: {table_mode_slug} | Target: {run_id_slug}")
    print(f"Network: Retry up to 10x with 5s TCP timeout per request.")

    with ThreadPoolExecutor(max_workers=tasks_count) as executor:
        futures = []
        for device_id in id_list:
            for topic in TOPICS:
                futures.append(executor.submit(
                    scan_device_topic, device_id, topic, start_ms, end_ms, GRANULARITY_MIN, table_params
                ))

        finished = 0
        for future in as_completed(futures):
            finished += 1
            rows, q_count, topic, dev = future.result()
            all_results.extend(rows)
            total_queries += q_count

            # Progress tracker
            print(
                f"  [{finished}/{tasks_count}] ✅ Done: {dev} | {topic.upper():<3} | Queries: {q_count:03} | Found: {sum(r['has_data'] for r in rows)}")

    # Finalize Data
    df = pd.DataFrame(all_results)
    if df.empty: return

    csv_path = f"coverage/report_{run_id_slug}.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n✅ Total Queries: {total_queries}")
    print(f"✅ CSV Saved: {csv_path}")

    # Heatmaps
    print("Generating Heatmaps...")
    for topic in TOPICS:
        sub_df = df[df['topic'] == topic]
        if sub_df.empty: continue
        pivot_df = sub_df.pivot(index="device_id", columns="datetime", values="has_data")
        fig = px.imshow(
            pivot_df,
            title=f"Coverage: {topic.upper()} | {table_mode_slug} | {GRANULARITY_MIN}m",
            color_continuous_scale=['#1a1a1a', '#00CC96'],
            aspect="auto"
        )
        fig.update_coloraxes(showscale=False)
        fig.update_layout(template="plotly_dark")
        html_path = f"coverage/heatmap_{topic.upper()}_{run_id_slug}.html"
        fig.write_html(html_path)
        print(f"✅ {topic.upper()} Heatmap: {html_path}")


if __name__ == "__main__":
    check_coverage_parallel()