package prerna.engine.impl.r;

import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RUserConnectionSingle  extends AbstractRUserConnection  {

	private static final Logger classLogger = LogManager.getLogger(RUserConnectionSingle.class);

	// Host and port
	private final String host;
	private static final String DEFAULT_HOST = "localhost";
	private int port = 6311;
	

	// TODO >>>timb: R - this constructor is never used, so the host is not really configurable right now (later)
	public RUserConnectionSingle(String rDataFile, String host, int myPort) {
		super(rDataFile);
		this.host = host;
		this.port = myPort;
	}

	public RUserConnectionSingle(String rDataFile) {
		super(rDataFile);
		this.host = DEFAULT_HOST;
	}

	public RUserConnectionSingle() {
		super();
		this.host = DEFAULT_HOST;
	}

	@Override
	public void initializeConnection() throws Exception {
		rcon = RserveUtil.connect(host, port);
	}
	
	@Override
	protected void recoverConnection() throws Exception {
		try {
			stopR();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
		this.stoppedR = true;
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO Auto-generated method stub

	}
}
