import json
import os
import sys

# Ensure we can import from tests/
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import tests.config as config
import tests.time_utils as time_utils

# Directory to save JSON files
OUTPUT_DIR = "aws_test_events"

# Define specific high-value test scenarios
SCENARIOS = [
    {
        "filename": "1_gyr_merged.json",
        "params": {
            "table_name": "gyr",
            "id": "ASENSE00000005",
            "start": "2025-11-25 16:00:00",
            "end": "2025-11-25 17:00:00",
            "merge_by_hour": "true",
            "output_format": "map"
        }
    },
    {
        "filename": "2_acc_merged.json",
        "params": {
            "table_name": "acc",
            "id": "ASENSE00000005",
            "start": "2025-11-25 16:00:00",
            "end": "2025-11-25 17:00:00",
            "merge_by_hour": "true",
            "output_format": "map"
        }
    },
    {
        "filename": "3_fft_merged.json",
        "params": {
            "table_name": "fft",
            "id": "ASENSE00000017",
            "start": "2025-11-24 00:00:00",
            "end": "2025-11-25 23:59:59",
            "merge_by_hour": "true",
            "output_format": "map"
        }
    },
    {
        "filename": "4_data_scalar.json",
        "params": {
            "table_name": "data",
            "id": "ASENSE00000003",
            "start": "2025-11-25 12:00:00",
            "end": "2025-11-25 13:00:00",
            "merge_by_hour": "false",
            "output_format": "map"
        }
    },
    {
        "filename": "5_gyr_tuples_format.json",
        "params": {
            "table_name": "gyr",
            "id": "ASENSE00000005",
            "start": "2025-11-25 16:00:00",
            "end": "2025-11-25 17:00:00",
            "merge_by_hour": "true",
            "output_format": "tuple_array"  # <--- New feature test
        }
    }
]


def generate():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"Generatng AWS Console Test Events in '{OUTPUT_DIR}/'...\n")

    for scenario in SCENARIOS:
        p = scenario['params']

        # Calculate Unix Timestamps
        start_ms = time_utils.local_datetime_to_unix_milliseconds(p['start'])
        end_ms = time_utils.local_datetime_to_unix_milliseconds(p['end'])

        # Construct AWS Lambda Proxy Event Structure
        aws_event = {
            "queryStringParameters": {
                "table_name": p['table_name'],
                "id": p['id'],
                "start_time": str(start_ms),
                "end_time": str(end_ms),
                "minute": "0",
                "merge_by_hour": p['merge_by_hour'],
                "output_format": p['output_format']
            }
        }

        filepath = os.path.join(OUTPUT_DIR, scenario['filename'])
        with open(filepath, 'w') as f:
            json.dump(aws_event, f, indent=4)

        print(f"✅ Created {scenario['filename']}")
        print(f"   Window: {p['start']} -> {p['end']}")
        print(f"   Unix:   {start_ms} -> {end_ms}\n")


if __name__ == "__main__":
    generate()