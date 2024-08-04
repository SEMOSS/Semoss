package prerna.engine.impl.r;

import prerna.util.PortAllocator;

public class RUserConnectionDedicated extends AbstractRUserConnection {
	
	// Host and port
	private final String host;
	private static final String DEFAULT_HOST = "127.0.0.1";
	private int port = -1;
	
	// TODO >>>timb: R - this constructor is never used, so the host is not really configurable right now (later)
	public RUserConnectionDedicated(String rDataFile, String host) {
		super(rDataFile);
		this.host = host;
		init();
	}
	
	public RUserConnectionDedicated(String rDataFile) {
		super(rDataFile);
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
			port = PortAllocator.getInstance().getNextAvailablePort();
			process = RserveUtil.startR(port);
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
		// also try to see if the connection is there and we can connect
		try {
			stopR();
		} catch (Exception e) {
			// If an error occurs stopping R, then grab a new port to run on
			port = PortAllocator.getInstance().getNextAvailablePort();
		}
		
		// Try to start R and establish a new connection to it
		process = RserveUtil.startR(port);
		initializeConnection();
		loadDefaultPackages();

		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
		this.stoppedR = false;
	}
	
	@Override
	public void stopR() throws Exception {
		if (rcon != null) {
			rcon.close();
		}
		RserveUtil.stopR(port);
		this.stoppedR = true;
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO >>>timb: R - need to complete cancellation here (later)
	}

	public int getPort() {
		return port;
	}
	
}
