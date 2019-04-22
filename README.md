What does this do?

It creates a kinesis stream, an eventHandler python, and two APIs (createSubscriber and deleteSubscriber). It simulates a datasteram feeding a kinesis stream, which then triggers the eventHandler lambda, which then pushes batched messages to subscriber lambas.  The subscriber lambas push the messages to Sumologic 1 message at at time and wait a random time between 1-3 seconds to 'simulate' slow subscribers.

TODO
4) Create tests to ensure that messages are being delivered in order
5) Find maximum scale of 1 stream (there's a max invocation size to subscriber lambdas, protect against that)
6) Log processing times to create dashboards

How to deploy

1) Get AWS access and secret keys. See this doc for examples (https://docs.google.com/document/d/1xvCb-bddPmK6GHFRGSsywSZz_kwXFftJdviZTZhYpdU/edit). If you need an AWS account go here (https://sites.google.com/thoughtworks.com/infosec-hub/services/aws-accounts)
2) Install serverless framework.  See instructions here (https://serverless.com/framework/docs/providers/aws/guide/installation/)
3) Zip up handler.zip.  This will be used to create new subscriber lambas later. `zip handler.zip handler.py`
4) Deploy serverless.  `serverless deploy`
5) Check the eventHandler lambda in AWS. You may need to configure the event stream to kinesis manually
6) Add 2 policies in IAM to the IAM role for lamba.  Full Lambda access and Full kinesis access.
7) Create a DynamoDB table called twitterTable.  Keys are namespace and event_name (Cloudformation code coming later)
8) Run `python random_functions.py` to quickly spin up additional 'subscribers'
9) Run `python tweeps.py` to start streaming data to kinesis


