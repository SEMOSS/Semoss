package prerna.engine.impl.r;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;

public class RUserConnectionSingle  extends AbstractRUserConnection  {

	// Host and port
	private final String host;
	private static final String DEFAULT_HOST = "localhost";
	private int port = 6311;
	

	// TODO >>>timb: R - this constructor is never used, so the host is not really configurable right now (later)
	public RUserConnectionSingle(String rDataFile, String host, int myPort) {
		super(rDataFile);
		this.host = host;
		this.port=myPort;
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
	public void stopR() throws Exception {
		if (rcon != null) rcon.close();
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	protected void recoverConnection() throws Exception {
		try {
			stopR();
		} catch (Exception e) {
			e.printStackTrace();
		}
		initializeConnection();
		loadDefaultPackages();
		// Make sure R is healthy
				if (!isHealthy()) {
					throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
				}
	}



}
