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
- Proxy Host: *******
- Proxy Port: 3128
- Proxy User: ***
- Proxy Password: *********


## Install NIFI 

``` 
$ brew install nifi 
```  

## Build and deploy to NIFI

```
$ mvn clean package
$ rm -f /usr/local/Cellar/nifi/1.5.0/libexec/lib/google-pubsub-nifi-consumer-1.0.0.nar
$ cp ./target/*.nar /usr/local/Cellar/nifi/1.5.0/libexec/lib/
$ nifi restart`

```


## logs
``
tail -f /usr/local/Cellar/nifi/1.5.0/libexec/logs/nifi-app.log
```


