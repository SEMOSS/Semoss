package prerna.logger.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import prerna.logger.IQueueLogger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class RabbitMQLogger implements IQueueLogger {


    private  Connection connection;
    private  Channel channel;
    private  String queueName;
    private  String exchangeName;
    private String routingKey;
    private final ConnectionFactory connectionFactory;


    public RabbitMQLogger(String config) {
        connectionFactory = new ConnectionFactory();
        Properties properties = this.getProperties(config);
        connectionFactory.setHost(properties.getProperty("host"));
        connectionFactory.setPort(Integer.parseInt(properties.getProperty("port")));
        connectionFactory.setUsername(properties.getProperty("username"));
        connectionFactory.setPassword(properties.getProperty("password"));
        this.queueName = properties.getProperty("queue");
        this.exchangeName = properties.getProperty("exchange");
        this.routingKey = properties.getProperty("routingKey");
    }

    private void initializeConnectionIfNeeded() throws IOException, TimeoutException {
        if(this.connection == null || !this.connection.isOpen()){
            this.connection = connectionFactory.newConnection();
            this.channel = this.connection.createChannel();
            this.channel.queueDeclare(this.queueName, true, false, false, (Map)null);
            this.channel.exchangeDeclare(this.exchangeName, "fanout", true);
            this.channel.queueBind(this.queueName, this.exchangeName, "");
        }
    }

    public void send(String message) {
        System.out.println("Message: "+message);
        try {
            initializeConnectionIfNeeded();
            this.channel.basicPublish(this.exchangeName, this.routingKey, (AMQP.BasicProperties)null, message.getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println("Failed connection to rabbitmq: "+ e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            this.channel.close();
            this.connection.close();
        } catch (TimeoutException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
