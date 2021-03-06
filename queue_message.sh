#!/bin/bash

base='
{
    "foo": "retry"
}
'

message_attributes='
{
    "MessageType": { "DataType": "String", "StringValue": "test" },
    "Testy": { "DataType": "String", "StringValue": "test" }
}
'

push(){
    docker exec -i go-sqs-consumer-localstack awslocal sqs send-message --queue-url http://localhost:4566/000000000000/local-queue --message-body "$base" --message-attributes "$message_attributes"
}

while [ : ]
do
    push
    sleep 1
done
