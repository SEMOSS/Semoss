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
