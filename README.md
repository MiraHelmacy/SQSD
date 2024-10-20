# SQSD

A Java application that consumes messages from an Amazon SQS queue. 

This example application receives a message from the SQS queue, prints the messages to the console, and then deletes the messages from the same SQS queue. 

This project contains a maven application with [AWS Java SDK 2.x](https://github.com/aws/aws-sdk-java-v2) dependencies.

#### Building the project
```
mvn clean package
```
#### Usage
```
java -jar target/sqsd-1.0.0.jar -h
usage: sqsd [-aid <arg>] [-h] [-m <arg>] [-ma <arg>] [-q <arg>] [-qu
       <arg>] [-region <arg>] [-sa <arg>] [-t <arg>] [-w <arg>]
Consume messages from an SQS queue
 -aid,--account-id <arg>          AWS Account id of SQS Queue. Required if
                                  the Queue is in a different account.
 -h,--help                        Prints this help message
 -m,--max-messages <arg>          Max Number of Messages to receive per
                                  request
 -ma,--message-attributes <arg>   Message Attributes
 -q,--queue-name <arg>            Queue Name of SQS queue to consume
                                  messages from. Required if Queue URL is
                                  not present
 -qu,--queue-url <arg>            Queue URL to consume messages from.
                                  Required if Queue name is not present
 -region,--region <arg>           AWS Region
 -sa,--system-attribute <arg>     System Parameters
 -t,--threads <arg>               Number of threads. Max of 16
 -w,--wait-time <arg>             How long to wait to receive messages
```

### Demo

Included in this repository is a demo CloudFormation template that creates a CloudFormation stack with SQS queue prepared for testing SQSD.

The stack template is in the ```demo``` folder and is called ```demo.yaml```

>[!Note]
>An AWS Account with Admin permissions is expected to complete this demo.
>The demo also assumes you have a basic understanding of the AWS CLI and the AWS CLI is installed.

#### Create the stack

>[!Warning]
>Deploying a CloudFormation Stack or an SQS Queue may have accompanying costs.

To create the stack, run the following command from the root folder of this respository:

```
aws cloudformation create-stack --stack-name SQSD-Demo-Stack --template-body file://demo/demo.yaml --region us-east-1
```

#### Check the stack's outputs
When the stack is deployed successfully, this stack will contain two different outputs

1. The Queue URL: ```https://sqs.us-east-1.amazonaws.com/<account id>/SQSD-Demo-Queue```
2. Queue Name: SQSD-Demo-Queue

SQSD can use either the queue name or queue URL in order to request messages. To check what the queue name and queue url are for the created stack. Use the following command.

```
aws cloudformation describe-stacks --stack-name SQSD-Demo-Stack --region us-east-1
```

>[!Note]
>Creating a CloudFormation stack can take time. Waiting a few minutes after creating the stack to see the queue name or queue url may be required.

#### Starting SQSD

Once you know the queue name or queue url, you are ready to run SQSD. 

First, the application needs to be compiled.

```
mvn clean package
```

Then SQSD can be run using the queue name. 

```
java -jar target/sqsd-1.0.0.jar --queue-name SQSD-Demo-Queue --region us-east-1
```

Or the queue URL:

```
java -jar target/sqsd-1.0.0.jar --queue-url <Your-Queue-URL-Here> --regio
n us-east-1
```

#### Clean Up

>[!Caution]
>Deleting the demo stack will also delete the demo queue. Deleting the queue will also delete any messages in the queue.

To clean up the demo. Delete the stack by running the following command.

```
aws cloudformation delete-stack --stack-name SQSD-Demo-Stack --region us-east-1
```