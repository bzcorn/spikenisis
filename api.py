import json
from botocore.exceptions import ClientError
import boto3
from datetime import datetime
import datetime
from spikenisis.common.common import getDynamoTable


def createSubscriber(event, context):
    """
    This function will create a subscriber lambda and add it to ddb
    """
    # get event and subscriber names
    # create ddb entry
    # create lambda
    # set ddb to "activate" subscription
    print(event)
    namespace = event['pathParameters']['namespace']
    event_name = event['pathParameters']['event']
    subscriber_name = event['pathParameters']['subscriber']
    function_name = f'{namespace}_event_{event_name}_subscriber_{subscriber_name}'
    # Create DDB entry
    table = getDynamoTable()
    event_name = f"{event_name}_subscriber_{subscriber_name}"
    table.put_item(
        Item={
            'namespace': namespace,
            'event_name': event_name,
            'subscriber_name': subscriber_name,
            'queue_state': False,
            'write_state': False,
            'confirmed_sequence_number': ' ',
            'confirmed_sequence_number_timestamp': ' ',
            'attempted_sequence_number': ' ',
            'attempted_sequence_number_timestamp': ' ',
            'endpoint': ' ',
            'created_date': str(datetime.datetime.utcnow()),
            'approximate_arrival_timestamp': str(datetime.datetime.utcnow().timestamp()),
        }
    )
    # Create Lambda
    client = boto3.client('lambda')
    with open('handler.zip', 'rb') as zipfile:
        response = client.create_function(
            FunctionName=function_name,
            Runtime='python3.7',
            Role='arn:aws:iam::352343957463:role/aws-python3-dev-us-east-1-lambdaRole',
            Handler='handler.forwardEvent',
            Code={
                'ZipFile': zipfile.read(),
            },
            Description='This lambda will send events to an endpoint',
            Timeout=300,
            MemorySize=256,
            Publish=False,
            Tags={
                'event': event_name,
                'namespace': namespace,
                'subscriber': subscriber_name
            }
        )
    client.put_function_concurrency(
        FunctionName=function_name,
        ReservedConcurrentExecutions=1
    )
    response = {
        "statusCode": 200,
        "body": json.dumps(response)
    }
    return response


def deleteSubscriber(event, context):
    """
    Deletes subscriber and its entry in DDB
    """
    namespace = event['pathParameters']['namespace']
    event_name = event['pathParameters']['event']
    subscriber_name = event['pathParameters']['subscriber']
    function_name = f'{namespace}_event_{event_name}_subscriber_{subscriber_name}'
    table = getDynamoTable()
    event_name = f"{event_name}_subscriber_{subscriber_name}"
    try:
        response = table.delete_item(
            Key={
                'namespace': 'infosec',
                'event_name': event_name
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        print("DeleteItem succeeded:")

    client = boto3.client('lambda')
    response = client.delete_function(
        FunctionName=function_name,
    )
    response = {
        "statusCode": 200,
        "body": json.dumps(response)
    }
    return response