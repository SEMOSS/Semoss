package prerna.engine.impl.r;

public interface IRserveConnectionPool {

	RserveConnectionMeta getConnection();
	
	void releaseConnection(RserveConnectionMeta connection);
	
	void recoverConnection(RserveConnectionMeta connection) throws Exception;
	
	void shutdown() throws Exception;
		
}
