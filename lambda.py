import boto3
import json

def lambda_handler(event, context):
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Define the table name 
    table_name = 'lambda_Table'

    try:
        # Check if the table already exists
        existing_tables = dynamodb.list_tables()['TableNames']
        if table_name in existing_tables:
            return {
                'statusCode': 200,
                'body': json.dumps(f"Table '{table_name}' already exists!")
            }

        # Create DynamoDB table with sort key
        response = dynamodb.create_table(
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

        return {
            'statusCode': 200,
            'body': json.dumps(f"Table '{table_name}' created successfully with sort key!")
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
