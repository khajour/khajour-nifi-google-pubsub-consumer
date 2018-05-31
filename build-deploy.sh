#!/bin/bash

echo "building..."
mvn clean package
rm -f /usr/local/Cellar/nifi/1.5.0/libexec/lib/nifi-google-pubsub-consumer-1.0.0.nar
cp ./target/*.nar /usr/local/Cellar/nifi/1.5.0/libexec/lib/
nifi restart

echo "nifi processor deployed."
