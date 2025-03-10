package prerna.log4jqueue.connector.serviceimpl;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.log4jqueue.connector.abstraction.AbstractQueuePushConnector;
import prerna.om.ThreadStore;
import prerna.auth.User;

public class KafkaQueueConnector implements AbstractQueuePushConnector{
	
	private static final Logger classLogger = LogManager.getLogger(KafkaQueueConnector.class);
	
	static Properties props = new Properties();
	
	private static User user = ThreadStore.getUser();
	
	/*
	 * static { props.put("bootstrap.servers", "localhost:9092");
	 * props.put("acks","all"); props.put("retries",Integer.MAX_VALUE);
	 * props.put("enable.idempotence","true"); props.put("key.serializer",
	 * "org.apache.kafka.common.serialization.StringSerializer");
	 * props.put("value.serializer",
	 * "org.apache.kafka.common.serialization.StringSerializer"); }
	 */

	private Producer<String, String> producer = new KafkaProducer<String, String>(props);
	
	@Override
	public void publish(String topic) {
		String kafkaMsg = "User :"+ user + " logged in with Thread: "+Thread.currentThread().getName();
		//String kafkaMsg = "User :"+System.getProperty("user.name")+ " logged in with Thread: "+Thread.currentThread().getName();
		producer.send(new ProducerRecord<String, String>("logs",kafkaMsg));
		classLogger.info(kafkaMsg);
	}

	@Override
	public String consume(String topic) {
		// TODO Auto-generated method stub
		return null;
	}


	
}
