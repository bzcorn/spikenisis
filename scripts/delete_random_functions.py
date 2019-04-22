import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('twitterTable')

client = boto3.client('dynamodb')
response = table.query(
    KeyConditionExpression=Key('namespace').eq('infosec') & Key('event_name').begins_with('twitter_subscriber'),
    ConsistentRead=True
)

for item in response['Items']:
    table.update_item(
        Key={
            'namespace': 'infosec',
            'event_name': item['event_name']
        },
        UpdateExpression='SET write_state = :val1',
        ExpressionAttributeValues={
            ':val1': False
        }
    )


for item in response['Items']:
  i = item['event_name'].split('_')
  print (i)
  url = f'https://xpcnqg9f37.execute-api.us-east-1.amazonaws.com/dev/create/namespace/infosec/event/{i[0]}/subscriber/{i[-1]}'
  response = requests.delete(url)
  print (response.status_code)
