package prerna.engine.impl.r;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.rosuda.REngine.REXP;

public class RUserConnectionPooled extends AbstractRUserConnection {

	private static final long ACTIVE_HEALTH_TIMEOUT = 12L; // TODO >>>timb: R - make this configurable in rdf map
	private static final TimeUnit ACTIVE_HEALTH_TIMEOUT_UNIT = TimeUnit.SECONDS;
	
	private final RserveConnectionMeta rconMeta;

	public RUserConnectionPooled(String rDataFile) {
		super(rDataFile);
		this.rconMeta = RserveConnectionPool.getInstance().getConnection();
	}

	public RUserConnectionPooled() {
		super();
		this.rconMeta = RserveConnectionPool.getInstance().getConnection();
	}
	
	// Because windows reuses rcon, need to track when active
	// so we can moderate how long a user is allowed to block other user's execution of r scripts
	@Override
	public REXP eval(String rScript) {
		if (SystemUtils.IS_OS_WINDOWS) {
			try {
				if (rconMeta.isActive()) {
					return super.eval(rScript, ACTIVE_HEALTH_TIMEOUT, ACTIVE_HEALTH_TIMEOUT_UNIT);
				} else {
					rconMeta.setActive(true);
					return super.eval(rScript);
				}
			} finally {
				rconMeta.setActive(false);
			}
		} else {
			return super.eval(rScript);
		}
	}
	
	@Override
	public void voidEval(String rScript) {
		if (SystemUtils.IS_OS_WINDOWS) {
			try {
				if (rconMeta.isActive()) {
					super.voidEval(rScript, ACTIVE_HEALTH_TIMEOUT, ACTIVE_HEALTH_TIMEOUT_UNIT);
				} else {
					rconMeta.setActive(true);
					super.voidEval(rScript);
				}
			} finally {
				rconMeta.setActive(false);
			}
		} else {
			super.voidEval(rScript);
		}
	}
	
	@Override
	public void initializeConnection() throws Exception {
		if (SystemUtils.IS_OS_WINDOWS) { // On windows, we need to recycle the rcon
			if (rconMeta.getRcon() != null) {
				rcon = rconMeta.getRcon();
			} else {
				reloadRcon();
				rconMeta.setRcon(rcon);
			}
		} else {
			reloadRcon();
		}
	}
	
	private void reloadRcon() throws Exception {
		if (rcon != null) rcon.close(); // Close the old rcon and get a new one
		rcon = RserveUtil.connect(rconMeta.getHost(), rconMeta.getPort());
	}
	
	@Override
	protected void recoverConnection() throws Exception {

		// First try to reestablish the connection without restarting Rserve itself
		try {
			initializeConnection();
			loadDefaultPackages();
		} catch (Exception e) {
			RserveConnectionPool.getInstance().recoverConnection(rconMeta);
			initializeConnection();
			loadDefaultPackages();
		}
		
		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
	}
	
	@Override
	public void stopR() throws Exception {
		if (rcon != null) rcon.close();
		RserveConnectionPool.getInstance().releaseConnection(rconMeta);
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO >>>timb: R - need to complete cancellation here	(later)	
	}

}
