import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb')
    # create table
    table = dynamodb.create_table(
        TableName='timeseries',
        KeySchema=[
            {   'AttributeName': 'id', 'KeyType': 'HASH'    },
            {   'AttributeName': 'time', 'KeyType': 'RANGE'    }
        ],
        AttributeDefinitions=[
            {   'AttributeName': 'id', 'AttributeType': 'N'    },
            {   'AttributeName': 'time', 'AttributeType': 'N'    }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    
    # wait until table exists
    table.meta.client.get_waiter('table_exists')
    print(table.item_count)
    
    table.out_item(
        Item={
            'id': 0,
            'time': 1234,
            'age': 23,
            'weight': 76
        })
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

