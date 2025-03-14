package prerna.logging.impl;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import prerna.logging.IQueueLogger;

import java.util.Properties;

public class KafkaLogger implements IQueueLogger {

    private final Producer<String, String> producer;
    private final String topic;

    public KafkaLogger(String config) {
        Properties props = defaultProps(config);
        topic = props.getProperty("topic");
        producer = new KafkaProducer<>(props);
    }
    @Override
    public void send(String message) {
        producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
            if (exception != null) {
                System.err.println(exception.getMessage());
            }
        });
    }

    @Override
    public void close() {
        producer.close();
    }

    private Properties defaultProps(String config) {
        Properties props = getProperties(config);
        props.put("", "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
