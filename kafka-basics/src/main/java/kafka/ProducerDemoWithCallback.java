package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "hello world " + i);

            // send the data — asynchronous
            producer.send(producerRecord, (metadata, e) -> {
                // executes every time a record is successfully sent, or an exception is thrown.
                if (e == null) {
                    // the record was successfully sent
                    String str = """
                        Received new metadata:
                        Topic: %s
                        Partition: %s
                        Offset: %s
                        Timestamp: %s
                        """;
                    log.info(String.format(str, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
                } else {
                    log.error("Error while producing", e);
                }
            });
        }

        // flush the data — synchronous
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
