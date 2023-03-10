package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static final String STR = """
            Received new metadata:
            Topic: %s
            Key: %s
            Partition: %s
            Offset: %s
            Timestamp: %s
            """;

    public static void main(String[] args) {
        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "demo_java";
            String value = "hello world " + i;
            String key = "id_" + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            // send the data — asynchronous
            producer.send(producerRecord, (metadata, e) -> {
                // executes every time a record is successfully sent, or an exception is thrown.
                if (e == null) {
                    // the record was successfully sent
                    log.info(String.format(STR, metadata.topic(), producerRecord.key(),
                            metadata.partition(), metadata.offset(), metadata.timestamp()));
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
