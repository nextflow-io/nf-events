# Kafka plugin for Nextflow

This plugin provides an extension to implement built-in support for Kafka systems and manipulation in Nextflow scripts. 

It provides the ability to create a Nextflow channel to listening from topics as send message.

The current version provides out-of-the-box support for the following systems: 

* [Kafka](https://kafka.apache.org/)
                    
NOTE: THIS IS A PREVIEW TECHNOLOGY, FEATURES AND CONFIGURATION SETTINGS CAN CHANGE IN FUTURE RELEASES.


## Get started

Make sure to have Nextflow `22.10.0` or later. Add the following snippet to your `nextflow.config` file.

```
plugins {
  id 'nf-kafka@0.0.1'
}
```

## Configuration

The plugin configuration is specified using the `kafka` scope: 

| Config option 	               | Description 	                |
|-------------------------------|---	                        |
| `kafka.url`                   | The connection url. 
| `kafka.group`                 | The group where the plugin will be attached.

For example:

```
kafka {
  url = 'localhost:902'
  group = 'group'
}
```

## Available operations

This plugin adds to the Nextflow DSL the following extensions

### fromTopic

The `fromTopic` factory method allows for performing a query against the topic specified and creating a Nextflow channel emitting
a tuple for each record in the corresponding group with the key and value. For example:

```
include { fromTopic } from 'plugin/nf-kafka'

ch = channel.fromTopic('my-topic')
```

### watchTopic

The `watchTopic` factory method allows to watch against the topic specified and creating a Nextflow channel emitting
a tuple for each record in the corresponding group. For example:

```
include { watchTopic } from 'plugin/nf-kafka'

ch = channel.watchTopic('my-topic')
    .subscribe { println "new message received ${it[0]} = ${it[1]}" }
    .until{ it[1] == 'done' }
```

### writeMessage

The `writeMessage` operator allows to send a message (with and optional key) to a topic 

```
include { writeMessage; watchTopic } from 'plugin/nf-kafka'

process listener{
    input: val(msg)
    output: stdout 
    script:
        writeMessage('my-topic', 'Hi folks')                
        "echo ${msg[1]}"
}            
workflow{
    listener( Channel.of("msg") )
}        
```

You need to provide the topic and the message plus an optional key

## Develop

1. build the plugin `./gradlew copyPluginZip`

2. create a `docker-compose.yml` and run `docker-compose up -d` to have a local instance running at localhost:29092

```
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 22181:2181
  
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - 29092:29092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

3. create a Nextflow project and configure it

```
plugins{
   id 'nf-kafka@0.0.1'
}

kafka{    
    url='localhost:29092'
    group='group'
} 
```

4.- create a run a listener pipeline

```
include { watchTopic } from 'plugin/nf-kafka'

process listener1{
    input: val(msg)
    output: stdout 
    script:       
    "echo '${msg[1]}'"
}            

workflow{    
        
    chn1 = channel.watchTopic("test").until{ it[1]=='done' }

    listener1(chn1) | view 

} 
```

NXF_PLUGINS_DIR=/<the-path-to-the-project>/build/plugins/ nextflow run ./kafka-listener.nf

5.- create a run (in another shell) a producer 

``` 
include { publishTopic } from 'plugin/nf-kafka'

workflow{    
        
    Channel.of(
        "Hi folks",
        "Another message",
        "done"
    ).publishTopic("test")

}
```

NXF_PLUGINS_DIR=/<the-path-to-the-project>/build/plugins/ nextflow run ./kafka-producer.nf

If all goes well, producer will send 3 messages to the topic and listener will print them and exit

