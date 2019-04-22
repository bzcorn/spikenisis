import boto3
import json
import datetime

# For use with spike
def getDynamoTable():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    return dynamodb.Table('twitterTable')

# For use after spike
# def getDynamoTable(table_name, region_name='us-east-1'):
#     dynamodb = boto3.resource('dynamodb', region_name=region_name)
#     return dynamodb.Table(table_name)


def sendToSubscriber(batched_records, subscriber, code_name):
    """
    Given batched records send to subscriber lambda
    """
    client = boto3.client('lambda')
    print(f'subscriber is {subscriber}')
    event = {}
    event['Records'] = batched_records
    event['function_name'] = f'infosec_event_twitter_subscriber_{subscriber}'  # Needed for subscriber lambda
    event['sent_time'] = str(datetime.datetime.utcnow())
    event['code_name'] = code_name
    print(event['function_name'])
    print(event['Records'])
    print(f"Amount of records sent: {len(event['Records'])}")
    client.invoke(
        FunctionName=f'infosec_event_twitter_subscriber_{subscriber}',
        InvocationType='Event',
        Payload=json.dumps(event).encode('UTF-8'),
    )