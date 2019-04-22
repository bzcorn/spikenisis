import json
import botocore.vendored.requests as requests
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import time
import random
from datetime import datetime
import datetime
from spikenisis.common.common import sendToSubscriber, getDynamoTable
from spikenisis.established.established import timestampToSubscribers, getRecords, clean_data, create_timestamp_bookmarks, timestampToSequence

code_name = "quebec" # code_name to make it easier to find in sumologic

def eventHandler(event, context):
    """
    Responds to Kinesis events and pushes to subscriber lambdas
    """
    print(f"Event length: {len(event['Records'])}")
    print(f"Event: {event}")
    # logic for kinesis trigger or cron trigger?

    namespace = "infosec" # this should come from the kinesis event source arn, not hardset
    # query ddb and get all the current subscribers of the event
    subscriber_status = getSubscriberStatus()
    # if sub is new, use event
    newSubscriber(subscriber_status['new'], event)
    # if sub is old, compare with ddb
    establishedSubscriber(subscriber_status['established'])
    response = {
        "statusCode": 200,
        "body": json.dumps(event)
    }
    return response

def forwardEvent(event, context):
    """
    Sends batched events one at a time to an API endpoint in sumologic
    """
    sumologic_endpoint = "https://collectors.us2.sumologic.com/receiver/v1/http/ZaVnC4dhaV0dj7176zJQBPx_E-nz3JETvVPKcaOSKZiTB83S-36r6zdVRCblV7mlTuPFS7zlu2uIKlnnIbKPANEM8AV2kBPo0tTQyCMvnlKUsCAUjGfr1A=="
    print(f'event is: {event}')
    print(f'event["Records"] length is {len(event["Records"])}')
    name = event["function_name"].split("_")
    table = getDynamoTable()
    event_name = f'{name[2]}_{name[3]}_{name[4]}'
    # set write state to True
    if lockSubscriberWrite(table, event_name) == "ConditionalCheckFailedException":
        return
    else:
        print("Lock set to True")
    # Get latest write
    table = getDynamoTable()
    response = table.query(
        KeyConditionExpression=Key('namespace').eq('infosec') & Key(
            'event_name').begins_with(event_name),
        ConsistentRead=True
    )
    response['Items'][0]['approximate_arrival_timestamp']
    # response['Items'][0]['arrival_timestamp']

    # if last_sequence_number_confirmed implies its a new sub and to start
    # ingesting immediately
    if response['Items'][0]['confirmed_sequence_number'] == " ":
        new_sub = True
    else:
        new_sub = False
    print(f'new_sub is {new_sub}')
    i = 0
    bookmark = 0
    print (f"str(event['Records'][0]['ApproximateArrivalTimestamp']) is {event['Records'][0]['ApproximateArrivalTimestamp']}")
    print (f"str(response['Items'][0]['approximate_arrival_timestamp']) is {str(response['Items'][0]['approximate_arrival_timestamp'])}")
    if str(event['Records'][0]['ApproximateArrivalTimestamp']) == str(response['Items'][0]['approximate_arrival_timestamp']):
        overlap = True
    else:
        overlap = False
    for record in event['Records']:
        print(f"event: {str(record['ApproximateArrivalTimestamp'])}, ddb: {response['Items'][0]['approximate_arrival_timestamp']}")
        if str(record['ApproximateArrivalTimestamp']) == str(response['Items'][0]['approximate_arrival_timestamp']):
            bookmark = i
        i += 1
    i -= 1 # final iteration shouldn't count
    print(f'bookmark {bookmark}, i {i}')
    # If there isn't any overlap on records, end
    print (f'overlap evals to: {overlap}')
    print (f'not new_sub evals to: {not new_sub}')
    print (f'i - bookmark <= 0 evals to: {i - bookmark <= 0}')
    # bookmark < 1 implies that there is overlap and to process from overlap point
    if ((not overlap and (bookmark < 1)) and not new_sub) or (i - bookmark <= 0):
        print('No overlap between records and not new')
        lockSubscriberWrite(table, event_name, False)
        return
    print(f"records to write: {i - bookmark + 1}")
    if len(event['Records']) - 1 > 50:
        max_value = 50
    else:
        max_value = len(event['Records']) - 1
    for record in event['Records'][bookmark + 1:max_value]:
        print(f"Record is: {record}")
        sleep_timer = random.randint(1, 1500) / 1000
        data = {
            'record': record,
            'code_name': event['code_name'],
            'function_name': event['function_name'],
            'sleep': sleep_timer
        }
        # Save to DynamoDB the attempted seq number, utc datestamp
        attemptedSequenceWrite(table, event_name, record['SequenceNumber'])
        sending = True
        while sending:
            tries = 0
            response = requests.post(sumologic_endpoint, data=json.dumps(data))
            # Save to DynamoDB the confirmed seq number, utc datestamp
            print(f'Will write this timestamp: {record["ApproximateArrivalTimestamp"]}')
            if response.status_code == 200:
                confirmedSequenceWrite(
                    table,
                    event_name,
                    record['SequenceNumber'],
                    record['ApproximateArrivalTimestamp']
                )
                time.sleep(sleep_timer)
                sending = False
            elif tries > 5:
                sending = False
                lockSubscriberWrite(table, event_name, False)
            else:
                print ("Some kind of error with the request to Sumo")
                tries += 1
                time.sleep(1)
    lockSubscriberWrite(table, event_name, False)


def lockSubscriberWrite(table, event_name, lock=True):
    """
    Sets the write_status in ddb for the subscriber to the lock boolean. If
    lock=True it will only write if event_state on DDB is False and vice versa
    """
    try:
        response = table.update_item(
            Key={
                'namespace': 'infosec',
                'event_name': event_name
            },
            UpdateExpression='SET write_state = :val1',
            ExpressionAttributeValues={
                ':val1': lock
            },
            ConditionExpression=Attr('write_state').eq(not lock),
        )
        if lock:
            print("Locking subscriber")
        else:
            print("Unlocking subscriber")
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(f"Conditional Check Fail for write_state: {e.response['Error']['Message']}")
            return e.response['Error']['Code']
        else:
            print(f'Unexpected error: {e}')
            raise


def attemptedSequenceWrite(table, event_name, sequence_number):
    """
    Sets the attempted sequence write number
    """
    try:
        response = table.update_item(
            Key={
                'namespace': 'infosec',
                'event_name': event_name
            },
            UpdateExpression=(
                'SET '
                'attempted_sequence_number = :val1,'
                'attempted_sequence_number_timestamp = :val2'),
            ExpressionAttributeValues={
                ':val1': sequence_number,
                ':val2': str(datetime.datetime.utcnow())
            }
        )
        return response
    except ClientError as e:
        print(f'Unexpected error: {e}')
        # will need logic here to stop this subscription
        raise

def confirmedSequenceWrite(table, event_name, sequence_number, arrival_timestamp):
    """
    Sets the attempted sequence write number
    """
    try:
        print(f'setting arrival_timestamp: {arrival_timestamp}')
        response = table.update_item(
            Key={
                'namespace': 'infosec',
                'event_name': event_name
            },
            UpdateExpression=(
                'SET '
                'confirmed_sequence_number = :val1,'
                'confirmed_sequence_number_timestamp = :val2,'
                'approximate_arrival_timestamp = :val3'),
            ExpressionAttributeValues={
                ':val1': sequence_number,
                ':val2': str(datetime.datetime.utcnow()),
                ':val3': str(arrival_timestamp)
            },
            ConditionExpression=Attr('attempted_sequence_number').
                eq(sequence_number),
        )
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(f"Conditional Check Fail for confirmed_sequence_number: {e.response['Error']['Message']}")
            return e.response['Error']['Message']
        else:
            print(f'Unexpected error: {e}')
            # will need logic here to stop this subscription
            raise


def getSubscriberStatus():
    """
    Return a dictionary with subscibers that are new and established. Query
    DDB and get all the current subscribers to an event.
    """
    table = getDynamoTable()
    response = table.query(
        KeyConditionExpression=Key('namespace').eq('infosec') & Key(
            'event_name').begins_with('twitter_subscriber'),
        ConsistentRead=True
    )
    d = {
        'new': [],
        'established': [],
    }
    for subscriber in response['Items']:
        if subscriber['confirmed_sequence_number'] == " ":
            d['new'].append(subscriber)
        else:
            d['established'].append(subscriber)
    return d


def newSubscriber(subscribers, event):
    """
    Send events to new subscriber to establish the subscriber
    """
    def cleanEventRecordData(record):
        """
        Given a record turn it into a record that appears to have been
        pulled from kinesis and not an event
        """
        d = {
            'ApproximateArrivalTimestamp': record['approximateArrivalTimestamp'],
            'Data': record['data'],
            'PartitionKey': record['partitionKey'],
            'SequenceNumber': record['sequenceNumber'],
        }
        return d
    print(f'processing any New Subscribers')
    records = []
    for event_record in event['Records']:
        clean_record = cleanEventRecordData(event_record['kinesis'])
        records.append(clean_record)
    for subscriber in subscribers:
        subscriber_name = subscriber['event_name'].split('_')[-1]
        sendToSubscriber(records, subscriber_name, code_name)


def establishedSubscriber(response):
    """
    For subscribers that have already had events sent to them query DDB and
    pull the oldest confirmed delivery timestamp and grab records from that
    point, batch them up, and send to the subscribers
    """
    print(f'processing any Established Subscribers')
    if len(response) < 1:
        return
    timestamp_to_sequence_number = timestampToSequence(response)
    timestamp_to_subscribers = timestampToSubscribers(response) # matches subscribers to their current place in kinesis stream
    print(f'timestamp_to_subscribers: {timestamp_to_subscribers}')
    subscriber_timestamps = sorted(timestamp_to_subscribers.keys()) # list of unique timestamps
    print(f'subscriber_timestamps: {subscriber_timestamps}')
    sequence_number = timestamp_to_sequence_number[subscriber_timestamps[0]]
    records = getRecords(sequence_number)['Records'] # records since the oldest timestamp using sequence number
    timestamp_bookmark = create_timestamp_bookmarks(records, subscriber_timestamps)  # timestamp dictionary/record bookmark
    records = clean_data(records)
    print (f'records: {records}')
    print (f'timestamp_bookmark: {timestamp_bookmark}')
    for timestamp in subscriber_timestamps:
        for subscriber in timestamp_to_subscribers[timestamp]:
            batched_records = records[timestamp_bookmark[timestamp]:] # Batch up the current records that apply to subscribers
            sendToSubscriber(batched_records, subscriber, code_name)
    return


