package prerna.engine.impl.r;

public class RUserConnectionPooled extends AbstractRUserConnection {

	private final RserveConnectionMeta rconMeta;

	public RUserConnectionPooled(String rDataFileName) {
		super(rDataFileName);
		this.rconMeta = RserveConnectionPool.getInstance().getConnection();
	}

	public RUserConnectionPooled() {
		super();
		this.rconMeta = RserveConnectionPool.getInstance().getConnection();
	}
	
	@Override
	protected String getHost() {
		return rconMeta.getHost();
	}

	@Override
	protected int getPort() {
		return rconMeta.getPort();
	}
		
	@Override
	protected void recoverConnection() throws Exception {
		RserveConnectionPool.getInstance().recoverConnection(rconMeta);
		initializeConnection();
		
		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
	}
	
	@Override
	public void stopR() throws Exception {
		RserveConnectionPool.getInstance().releaseConnection(rconMeta);
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO >>>timb: R - need to complete cancellation here	(later)	
	}

}
