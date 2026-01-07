import os
import boto3
from boto3.dynamodb.conditions import Key
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'

TABLE_NAMES = ['acc', 'data', 'ain', 'gyr', 'fft']
ID_START = 1
ID_END = 33
MAX_TIMESTAMP = 1867367451000  # User provided upper limit

# --- PATH SETUP ---
# Determine the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MD_FILENAME = os.path.join(SCRIPT_DIR, "db_ranges_report.md")
IMG_FILENAME = os.path.join(SCRIPT_DIR, "db_ranges_report.png")


def get_dynamo_resource():
    return boto3.resource('dynamodb')


def ms_to_utc_str(ts):
    if ts is None:
        return "N/A"
    try:
        # Check if decimal or int, convert to float/int
        val = int(ts)
        # Modern UTC conversion
        dt = datetime.datetime.fromtimestamp(val / 1000.0, datetime.timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z'
    except Exception:
        return str(ts)


def fetch_edge_item(table, device_id, ascending=True):
    """
    Fetches exactly 1 item.
    ascending=True  -> Earliest item (time >= 0)
    ascending=False -> Oldest/Latest item (time <= MAX)
    """
    try:
        key_condition = Key('id').eq(device_id)

        if ascending:
            # Earliest: time >= 0, ScanIndexForward=True (Ascending)
            key_condition &= Key('time').gte(0)
        else:
            # Latest: time <= MAX, ScanIndexForward=False (Descending)
            key_condition &= Key('time').lte(MAX_TIMESTAMP)

        response = table.query(
            KeyConditionExpression=key_condition,
            Limit=1,
            ScanIndexForward=ascending,
            ProjectionExpression="#id, #t, time_broker, seq",
            ExpressionAttributeNames={'#id': 'id', '#t': 'time'}
        )

        items = response.get('Items', [])
        return items[0] if items else None
    except Exception as e:
        print(f"Error querying {table.name} for {device_id}: {e}")
        return None


def format_cell_data(item):
    if not item:
        return "No Data"

    t_dev = ms_to_utc_str(item.get('time'))
    t_brk = ms_to_utc_str(item.get('time_broker')) if item.get('time_broker') else "No Brk"
    seq = item.get('seq', 'No Seq')

    # Format for Markdown (HTML line breaks)
    return f"T: {t_dev}<br>B: {t_brk}<br>S: {seq}"


def format_cell_data_plain(item):
    """Plain text version for Matplotlib table"""
    if not item:
        return "-\n-\n-"

    t_dev = ms_to_utc_str(item.get('time'))
    t_brk = ms_to_utc_str(item.get('time_broker')) if item.get('time_broker') else "-"
    seq = item.get('seq', '-')

    return f"T: {t_dev}\nB: {t_brk}\nS: {seq}"


def main():
    dynamodb = get_dynamo_resource()

    # Store data for Markdown and DataFrame
    md_rows = []
    # For DataFrame
    df_data = []

    print(f"--- Scanning IDs ASENSE{ID_START:08d} to ASENSE{ID_END:08d} ---")

    for i in range(ID_START, ID_END + 1):
        device_id = f"ASENSE{i:08d}"
        print(f"Processing {device_id}...")

        row_md = {'ID': device_id}
        row_plain = {'ID': device_id}

        has_any_data = False

        for topic in TABLE_NAMES:
            table_name = f"asense_table_{topic}"
            table = dynamodb.Table(table_name)

            # 1. Earliest (Start)
            start_item = fetch_edge_item(table, device_id, ascending=True)

            # 2. Oldest/Latest (End)
            # Only query end if start exists to save 1 query on empty tables
            end_item = None
            if start_item:
                has_any_data = True
                end_item = fetch_edge_item(table, device_id, ascending=False)

            # Prepare MD strings
            col_start = f"{topic.upper()} Start"
            col_end = f"{topic.upper()} End"

            row_md[col_start] = format_cell_data(start_item)
            row_md[col_end] = format_cell_data(end_item)

            # Prepare Plain strings for Image
            row_plain[col_start] = format_cell_data_plain(start_item)
            row_plain[col_end] = format_cell_data_plain(end_item)

        md_rows.append(row_md)
        df_data.append(row_plain)

    # --- 1. GENERATE MARKDOWN ---
    # We define specific column order
    cols_order = ['ID']
    for topic in TABLE_NAMES:
        cols_order.append(f"{topic.upper()} Start")
        cols_order.append(f"{topic.upper()} End")

    df = pd.DataFrame(df_data)
    # Reorder columns
    df = df[cols_order]

    print(f"Generating Markdown at {MD_FILENAME}...")
    try:
        with open(MD_FILENAME, 'w') as f:
            f.write("# Database Range Report\n\n")
            f.write(f"Generated: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n\n")

            # Write Header
            header = "| " + " | ".join(cols_order) + " |"
            separator = "| " + " | ".join(["---"] * len(cols_order)) + " |"
            f.write(header + "\n")
            f.write(separator + "\n")

            # Write Rows using the MD-formatted dictionary list
            for r in md_rows:
                row_str = "| " + " | ".join([str(r.get(c, "")) for c in cols_order]) + " |"
                f.write(row_str + "\n")
    except Exception as e:
        print(f"Error writing markdown: {e}")

    # --- 2. GENERATE IMAGE ---
    print(f"Generating Image at {IMG_FILENAME}...")

    # Matplotlib Table Setup
    # Calculate figure height based on number of rows
    row_height = 1  # inches
    fig_height = max(4, len(df) * row_height + 2)
    fig_width = 4 + (len(TABLE_NAMES) * 2 * 3)  # Dynamic width

    try:
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        ax.axis('off')

        # Create the table
        mpl_table = ax.table(
            cellText=df.values,
            colLabels=df.columns,
            cellLoc='left',
            loc='center',
            colLoc='center'
        )

        # Styling
        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(9)
        mpl_table.scale(1, 4)  # Scale height to accommodate 3 lines of text per cell

        # Add zebra striping for readability
        for i, key in enumerate(mpl_table.get_celld().keys()):
            cell = mpl_table.get_celld()[key]
            row, col = key
            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#40466e')
            else:
                if row % 2 == 0:
                    cell.set_facecolor('#f1f1f2')
                else:
                    cell.set_facecolor('white')
                # Adjust padding
                cell.set_text_props(va='center')

        plt.title(f"Asense DB Ranges (ID {ID_START}-{ID_END})", pad=20, fontsize=16)
        plt.savefig(IMG_FILENAME, bbox_inches='tight', dpi=150)
        plt.close()
        print("Done.")
    except Exception as e:
        print(f"Error generating image: {e}")


if __name__ == "__main__":
    main()