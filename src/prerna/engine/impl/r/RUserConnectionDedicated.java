package prerna.engine.impl.r;

public class RUserConnectionDedicated extends AbstractRUserConnection {
	
	// Host and port
	private final String host;
	private static final String DEFAULT_HOST = "127.0.0.1";
	private int port = 6311;
	private static final int PORT_MAX = 65535;
	
	// TODO >>>timb: R - this constructor is never used, so the host is not really configurable right now (later)
	public RUserConnectionDedicated(String rDataFileName, String host) {
		super(rDataFileName);
		this.host = host;
		init();
	}
	
	public RUserConnectionDedicated(String rDataFileName) {
		super(rDataFileName);
		this.host = DEFAULT_HOST;
		init();
	}
	
	public RUserConnectionDedicated() {
		super();
		this.host = DEFAULT_HOST;
		init();
	}
	
	private void init() {
		try {
			setPort();
			RserveUtil.startR(port);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to initialize R.", e);
		}
	}
	
	@Override
	public void initializeConnection() throws Exception {
		rcon = RserveUtil.connect(host, port);
	}
	
	@Override
	protected void recoverConnection() throws Exception {
		
		// Try to stop R
		try {
			stopR();
		} catch (Exception e) {
			
			// If an error occurs stopping R, then grab a new port to run on
			setPort();
		}
		
		// Try to start R and establish a new connection to it
		RserveUtil.startR(port);
		initializeConnection();
		
		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
	}
	
	@Override
	public void stopR() throws Exception {
		RserveUtil.stopR(port);
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO >>>timb: R - need to complete cancellation here (later)
	}
	
	
	////////////////////////////////////////
	// Port management
	////////////////////////////////////////
	private void setPort() {
		while (!RserveUtil.isPortAvailable(port)) {
			port++;
			if (port > PORT_MAX) throw new IllegalArgumentException("No more ports are available."); 
		}
	}
	
}
