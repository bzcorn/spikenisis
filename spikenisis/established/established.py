import boto3
import base64

def timestampToSubscribers(response):
    """
    Create a dictionary that links a timestamp to subscribers. Makes it easier
    to batch up messages to send to subscribers.
    {123456789.123: [subA, ..., subN]}
    :return:
    """
    timestamp_to_subscribers = {}  # matches subscribers to their current place in kinesis stream
    for established_subscriber in response:
        stream_event, subscriber = established_subscriber['event_name'].split(
            '_subscriber_'
        )
        approximate_arrival_timestamp = established_subscriber[
            'approximate_arrival_timestamp'
        ]
        if approximate_arrival_timestamp not in timestamp_to_subscribers:
            timestamp_to_subscribers[approximate_arrival_timestamp] = []
            timestamp_to_subscribers[approximate_arrival_timestamp].append(
                subscriber)
        else:
            timestamp_to_subscribers[approximate_arrival_timestamp].append(
                subscriber)
    return timestamp_to_subscribers


def getRecords(StartingSequenceNumber):
    """
    Given a timestamp return the records from that point
    """
    print(f"Grabbing records from: {StartingSequenceNumber}")
    client = boto3.client('kinesis')
    if StartingSequenceNumber == " ":
        response = client.get_shard_iterator(
            StreamName='twitterStream',
            ShardId='shardId-000000000000',
            ShardIteratorType='LATEST'
        )
    else:
        response = client.get_shard_iterator(
            StreamName='twitterStream',
            ShardId='shardId-000000000000',
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=StartingSequenceNumber
        )
    records = client.get_records(
        ShardIterator=response['ShardIterator']
    )
    print (f'found {len(records)} records since Sequence Number {StartingSequenceNumber}')
    return records


def clean_data(records):
    """
    Clean up records to be json.dump-able and sent to subscriber
    """
    for record in records:
        timestamp = record['ApproximateArrivalTimestamp'].timestamp()
        decoded_data = base64.b64encode(record['Data']).decode(
            'utf-8')  # Needs to be base64'd and decoded to match regular kinesis
        record['ApproximateArrivalTimestamp'] = timestamp
        record['Data'] = decoded_data
    return records


def create_timestamp_bookmarks(records, subscriber_timestamps):
    """
    If the first time stamp in sorted_timestamp is older than the
    current records timestamp
    then place an bookmark in timestamp_bookmark
    this is to make it easy to send the correct batch of records later
    """
    timestamp_bookmark = {}  # timestamp dictionary/record bookmark
    i = 0
    record_iteration = 0
    for record in records:
        timestamp = record['ApproximateArrivalTimestamp'].timestamp()
        if i < len(subscriber_timestamps):
            if float(subscriber_timestamps[i]) <= timestamp:
                timestamp_bookmark[
                    subscriber_timestamps[i]] = record_iteration
                print (f'set {timestamp_bookmark[subscriber_timestamps[i]]} to {record_iteration + 1}')
                i += 1
                print (f'i is {i}')
    return timestamp_bookmark


def timestampToSequence(response_list):
    """
    Takes a DDB query and returns a dictionary of timestamp: sequence_number
    """
    d = {}
    for item in response_list:
        d[item['approximate_arrival_timestamp']] = item[
            'confirmed_sequence_number']
    return d
