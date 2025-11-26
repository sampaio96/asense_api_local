# tests/remote_client.py
import requests
import tests.config as config


def fetch_ground_truth(table_name, start_ms, end_ms, device_id, minute, merge_str):
    params = {
        "table_name": table_name,
        "start_time": start_ms,
        "end_time": end_ms,
        "id": device_id,
        "merge_by_hour": merge_str,
        "minute": minute
        # output_format defaults to legacy 'map' implicitly on server
    }

    headers = {"x-api-key": config.API_KEY}

    try:
        print(f"   [Remote] GET {table_name}...")
        resp = requests.get(config.URL, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"   [Remote] Error: {e}")
        return None