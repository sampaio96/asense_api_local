import os
import boto3
from boto3.dynamodb.conditions import Key

# AWS Config
os.environ['AWS_PROFILE'] = 'asense-iot'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'


def check_data_table():
    table_name = "asense_table_data"
    print(f"--- Inspecting {table_name} ---")

    client = boto3.client('dynamodb')
    try:
        resp = client.describe_table(TableName=table_name)
        schema = resp['Table']['KeySchema']
        print("Key Schema:")
        for key in schema:
            print(f"  - {key['AttributeName']} ({key['KeyType']})")

        # Check Attribute Definitions (Types)
        attrs = resp['Table']['AttributeDefinitions']
        print("Attribute Definitions:")
        for att in attrs:
            print(f"  - {att['AttributeName']}: {att['AttributeType']} (S=String, N=Number)")

    except Exception as e:
        print(f"Error describing table: {e}")
        return

    # Check a real item
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    print("\n--- Sampling 1 Item ---")
    # Scan just 1 item to see the types
    resp = table.scan(Limit=1)
    if resp['Items']:
        item = resp['Items'][0]
        print(f"ID: {item.get('id')} (Type: {type(item.get('id'))})")
        print(f"Time: {item.get('time')} (Type: {type(item.get('time'))})")
    else:
        print("Table is empty or scan returned nothing.")


if __name__ == "__main__":
    check_data_table()