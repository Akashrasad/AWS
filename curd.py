import boto3
import json
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# Define the table name
TABLE_NAME = 'lambda_Table'

def lambda_handler(event, context):
    try:
        # Determine operation from the event
        operation = event.get('operation', '').lower()

        if operation == 'create':
            return create_item(event.get('data', {}))
        elif operation == 'read':
            return read_item(event.get('key', {}))
        elif operation == 'update':
            return update_item(event.get('key', {}), event.get('update_data', {}))
        elif operation == 'delete':
            return delete_item(event.get('key', {}))
        else:
            return {
                'statusCode': 400,
                'body': json.dumps("Invalid operation! Use 'create', 'read', 'update', or 'delete'.")
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

# Function to create an item
def create_item(data):
    item = {
        'id': {'S': data['id']},  # Partition key
        'timestamp': {'N': str(data['timestamp'])},  # Sort key
        'name': {'S': data['name']},
        'age': {'N': str(data['age'])}
    }
    dynamodb.put_item(TableName=TABLE_NAME, Item=item)
    return {
        'statusCode': 200,
        'body': json.dumps(f"Item created successfully: {data}")
    }

# Function to read an item
def read_item(key):
    response = dynamodb.get_item(
        TableName=TABLE_NAME,
        Key={
            'id': {'S': key['id']},
            'timestamp': {'N': str(key['timestamp'])}
        }
    )
    item = response.get('Item')
    if not item:
        return {
            'statusCode': 404,
            'body': json.dumps("Item not found.")
        }
    return {
        'statusCode': 200,
        'body': json.dumps(item)
    }

# Function to update an item
def update_item(key, update_data):
    update_expression = "SET " + ", ".join(f"#{k} = :{k}" for k in update_data.keys())
    expression_attribute_names = {f"#{k}": k for k in update_data.keys()}
    expression_attribute_values = {f":{k}": {'S': v} if isinstance(v, str) else {'N': str(v)} for k, v in update_data.items()}

    dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={
            'id': {'S': key['id']},
            'timestamp': {'N': str(key['timestamp'])}
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )
    return {
        'statusCode': 200,
        'body': json.dumps(f"Item updated successfully: {key}")
    }

# Function to delete an item
def delete_item(key):
    dynamodb.delete_item(
        TableName=TABLE_NAME,
        Key={
            'id': {'S': key['id']},
            'timestamp': {'N': str(key['timestamp'])}
        }
    )
    return {
        'statusCode': 200,
        'body': json.dumps(f"Item deleted successfully: {key}")
    }
