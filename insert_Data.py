import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Define the table name
    table_name = 'lambda_Table'

    try:
        # Check if the table already exists
        existing_tables = dynamodb.list_tables()['TableNames']
        if table_name not in existing_tables:
            # Create DynamoDB table with sort key
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'},      # Partition Key
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}  # Sort Key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'},       # String
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'} # Number
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

            # Wait for the table to be created
            dynamodb.get_waiter('table_exists').wait(TableName=table_name)

        # Insert data into the table
        item = {
            'id': {'S': '123'},  # Replace '123' with your unique partition key value
            'timestamp': {'N': str(int(datetime.now().timestamp()))},  # Current timestamp
            'name': {'S': 'John Doe'},  # Example attribute
            'age': {'N': '30'}  # Example attribute
        }

        response = dynamodb.put_item(TableName=table_name, Item=item)

        return {
            'statusCode': 200,
            'body': json.dumps(f"Data inserted successfully into '{table_name}'! {response}")
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
