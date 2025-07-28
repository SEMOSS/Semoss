package prerna.logging;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaLogger implements IQueueLogger {
	
		private volatile KafkaProducer<String, String> producer;
	    private final Properties config;
	    private final Object lock = new Object();

	    public KafkaLogger(String config) {
	        this.config = defaultProps(config);
	    }
	    @Override
	    public void init() {
	    
	    }
	    
	    private Producer<String, String> getOrCreateProducer() {
	        if(producer == null){
	            synchronized (this) {
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
	    public void send(String key,String value) {
	    	Producer<String, String> kafkaProducer = getOrCreateProducer();
	        if(kafkaProducer == null){
	            System.err.println("Failed to initialize KafkaProducer: null");
	            return;
	        }
	        
	        try{
	        	
	            String topic = config.getProperty("topic");
	            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
	            producer.send(record, (metadata, ex) -> {
					if (ex != null) {
						System.err.println("Failed to send log to Kafka: " + ex.getMessage());
					}
				});
	          //  System.out.println(send.get());
	        }catch (Exception ex){
	            System.err.println("Exception while sending message: "+ ex.getMessage());
	        }
	    }

	    @Override
	    public void close() {
	        producer.close();
	    }

	    private Properties defaultProps(String config) {
	        Properties props = getProperties(config);
	        props.put(ProducerConfig.ACKS_CONFIG, "1");
	        props.put(ProducerConfig.RETRIES_CONFIG, 3);
	        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	       
	      
	        return props;
	    }
	

}
