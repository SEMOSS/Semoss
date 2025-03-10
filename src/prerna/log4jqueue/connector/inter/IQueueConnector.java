package prerna.log4jqueue.connector.inter;

public interface IQueueConnector {
	void publish(String topic);
    String consume(String topic);
}
