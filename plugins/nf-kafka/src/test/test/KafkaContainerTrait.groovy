package test

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName


/**
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
trait KafkaContainerTrait {

    KafkaContainer kafka

    void startKafka() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
        kafka.start()
    }

    String kafkaURL(){
        return kafka.bootstrapServers
    }

    void stopKafka() {
        kafka.stop()
    }

    void writeKafkaMessage(String topic,String msg, String key=null){
        Properties properties = new Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaURL()
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer.class.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer.class.name
        final producer = new KafkaProducer<String, String>(properties)
        final record = new ProducerRecord<String,String>(topic, key, msg)
        producer.send(record)
    }
}