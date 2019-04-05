package prerna.engine.impl.r;

public class RserveConnectionPool implements IRserveConnectionPool {

	private static final int MAX_POOL_SIZE = 12;
	
	//////////////////////////////////////////////////////////////////////
	// Singleton implementation of IRserveConnectionPool
	//////////////////////////////////////////////////////////////////////
    private static class LazyHolder {
        private static final RserveConnectionPool INSTANCE = new RserveConnectionPool();
    }
	
	public static RserveConnectionPool getInstance() {
		return LazyHolder.INSTANCE;
	}
	
	private RserveConnectionPool() {
		
	}
	
	//////////////////////////////////////////////////////////////////////
	// IRserveConnectionPool implementation
	//////////////////////////////////////////////////////////////////////
	@Override
	public RserveConnectionMeta getConnection() {
		// TODO >>>timb: RCP - Auto-generated method stub
		return null;
	}

	@Override
	public void releaseConnection(RserveConnectionMeta connection) {
		// TODO >>>timb: RCP - Auto-generated method stub
		
	}

	@Override
	public void recoverConnection(RserveConnectionMeta connection) throws Exception {
		// TODO >>>timb: RCP - Auto-generated method stub
		
	}

	@Override
	public void shutdown() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
