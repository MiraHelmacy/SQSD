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

