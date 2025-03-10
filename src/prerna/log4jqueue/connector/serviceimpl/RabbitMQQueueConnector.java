package prerna.log4jqueue.connector.serviceimpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import prerna.log4jqueue.connector.abstraction.AbstractQueuePushConnector;

public class RabbitMQQueueConnector implements AbstractQueuePushConnector{

	private ConnectionFactory factory = new ConnectionFactory();;
	private Connection connection;
	private Channel channel;
	
	private static final Logger classLogger = LogManager.getLogger(RabbitMQQueueConnector.class);
	
	public void establishRabbitMQQueueConnector() {
			factory = new ConnectionFactory();
			factory.setHost("localhost");
			try {
				connection = factory.newConnection();
				classLogger.info("RabbitMQ Queue Connection eshtablished");
			} catch (IOException | TimeoutException e) {
				classLogger.error("Error in establishing a new RabbitMQ Queue Connection", e);
			}
			try {
			channel = connection.createChannel();
			channel.queueDeclare("logs_queue", true, false, false, null);
			classLogger.info("RabbitMQ Channels created and Queue created successfully");
			}catch(IOException ioex) {
				classLogger.error("Error in Channel and Queue creation");
			}
			finally {}
	}
	
	@Override
	public void publish(String topic) {
		String rabbitMQMsg = null;
		try {
			rabbitMQMsg = "User :" + System.getProperty("user.name") + " logged in with Thread: "
					+ Thread.currentThread().getName();
			channel.basicPublish("", "logs_queue", null, rabbitMQMsg.getBytes(StandardCharsets.UTF_8));
			classLogger.info(rabbitMQMsg);
		} catch (Exception e) {
			classLogger.error("Error logging to RabbitMQ", e);
		}
	}

	@Override
	public String consume(String topic) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
