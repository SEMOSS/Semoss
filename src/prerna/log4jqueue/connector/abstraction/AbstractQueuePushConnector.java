package prerna.log4jqueue.connector.abstraction;

import prerna.log4jqueue.connector.inter.IQueueConnector;

public abstract interface AbstractQueuePushConnector extends IQueueConnector {
	public void publish(String topic);
}
