#!/bin/sh

set -e

mkdir dynamodb
cd dynamodb
wget https://s3.eu-central-1.amazonaws.com/dynamodb-local-frankfurt/dynamodb_local_latest.tar.gz
tar xvzf dynamodb_local_latest.tar.gz

java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar &
