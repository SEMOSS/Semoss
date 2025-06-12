package prerna.logger.impl;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import prerna.logger.IQueueLogger;

import java.util.Properties;

public class KafkaLogger implements IQueueLogger {

    private volatile Producer<String, String> producer;
    private final Properties config;
    private final Object lock = new Object();

    public KafkaLogger(String config) {
        this.config = defaultProps(config);
    }

    private Producer<String, String> getOrCreateProducer() {
        if(producer == null){
            synchronized (lock) {
                if(producer == null) {
                    try{
                        producer = new KafkaProducer<>(config);
                    }catch (Exception ex){
                        System.err.println("Failed to initialize KafkaProducer: "+ ex.getMessage());
                    }
                }
            }
        }
        return producer;
    }

    @Override
    public void send(String message) {
        Producer<String, String> kafkaProducer = getOrCreateProducer();
        if(kafkaProducer == null){
            System.err.println("Failed to initialize KafkaProducer: null");
            return;
        }
        try{
            String topic = config.getProperty("topic");
            producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println(exception.getMessage());
                }
            });
        }catch (Exception ex){
            System.err.println("Exception while sending message: "+ ex.getMessage());
        }

    }

    @Override
    public void close() {
        producer.close();
    }

    private void putIfEnvPresent(Properties properties, String envKey, String kafkaKey){
        String value = System.getenv(envKey);
        if(value != null && !value.isEmpty()){
            properties.put(kafkaKey, value);
        }
    }

    private Properties defaultProps(String config) {
        Properties props = getProperties(config);
        props.put("acks", "all");
        putIfEnvPresent(props,ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        putIfEnvPresent(props,CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        putIfEnvPresent(props,SaslConfigs.SASL_MECHANISM,SaslConfigs.SASL_MECHANISM);
        String username = System.getenv("USERNAME");
        String password = System.getenv("PASSWORD");
        if(username !=null && password !=null){
            props.put(SaslConfigs.SASL_JAAS_CONFIG,"org.apache.kafka.common.security.scram.ScramLoginModule required "+"username=\""+username+"\" password=\""+password+"\";");
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("retries", 5);
        return props;
    }
}
