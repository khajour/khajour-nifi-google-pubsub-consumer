# Google Pub/Sub Nifi Consumer with Proxy Configuration

This NIFI processor consumes Google Pub/Sub directly or behind a proxy.

Author: Abdelaziz KHAJOUR


## Processor settings 
- Authentication: Google account service json
- Topic: Google pubsub topic
- Project ID: Google GCP project
- Subscription: Google pubsub subscription
- Pull Count: number of messages to pull from Google Pub Sub
- Use Proxy: true/false
- Proxy Host: <POROXY_HOST_VALUE>
- Proxy Port: <POROXY_PORT_VALUE>
- Proxy User: <POROXY_USER_VALUE>
- Proxy Password: <POROXY_PASSWORD_VALUE>


## Install NIFI 

``` 
$ brew install nifi 
```  

## Build and deploy to NIFI

```
$ git clone https://github.com/khajour/nifi-google-pubsub-consumer.git
$ cd nifi-google-pubsub-consumer
$ mvn clean package
$ rm -f /usr/local/Cellar/nifi/1.5.0/libexec/lib/google-pubsub-nifi-consumer-1.0.0.nar
$ cp ./target/*.nar /usr/local/Cellar/nifi/1.5.0/libexec/lib/

$ export GOOGLE_APPLICATION_CREDENTIALS=path-to-service-account.json 

$ nifi restart

```
## Test the pubsub processor with the NIFI UI

Go to localhost:8080/nifi

[NifiPubsubProcessor] <------> [PutFile]



## logs
```

tail -f /usr/local/Cellar/nifi/1.5.0/libexec/logs/nifi-app.log

```

