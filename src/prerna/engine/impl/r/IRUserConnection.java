package prerna.engine.impl.r;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;

public interface IRUserConnection {

	public static final String POOLED = "pooled";
	public static final String DEDICATED = "dedicated";
	public static final String SINGLE = "single";

	public static final String TYPE = RserveUtil.R_USER_CONNECTION_TYPE;
	
	public static IRUserConnection getRUserConnection(String rDataFile) {
		if (TYPE.equals(POOLED)) {
			return new RUserConnectionPooled(rDataFile);
		} else if (TYPE.equals(DEDICATED)) {
			return new RUserConnectionDedicated(rDataFile);
		} else if (TYPE.equals(SINGLE)){
			return new RUserConnectionSingle(rDataFile);
		} else {
			throw new IllegalArgumentException("Unknown R user connection type: " + TYPE);
		}
	}
	
	public static IRUserConnection getRUserConnection() {
		if (TYPE.equals(POOLED)) {
			return new RUserConnectionPooled();
		} else if (TYPE.equals(DEDICATED)) {
			return new RUserConnectionDedicated();
		} else if (TYPE.equals(SINGLE)){
			return new RUserConnectionSingle();
		}else {
			throw new IllegalArgumentException("Unknown R user connection type: " + TYPE);
		}
	}
	
	public void loadDefaultPackages() throws Exception;
	
	public void initializeConnection() throws Exception;

	/**
	 * Really want to get rid of this; should not be manipulating the rcon directly
	 * outside of this class
	 * 
	 * @return
	 */
	@Deprecated
	public RConnection getRConnection();
	
	public REXP eval(String rScript);
	
	public void voidEval(String rScript);
	
	public RSession detach();
		
	/**
	 * Stops just the user-specific R process.
	 * @throws Exception
	 */
	public void stopR() throws Exception;
	
	public void cancelExecution() throws Exception;
	
	public boolean isRecoveryEnabled();
	
	void setRecoveryEnabled(boolean enableRecovery);

	/**
	 * Stops all r processes.
	 * @throws Exception
	 */
	public static void endR() throws Exception {
		
		// Need to allow this process to execute the below commands
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
		
		// End
		try {
			ProcessBuilder pb;
			if (SystemUtils.IS_OS_WINDOWS) {
				pb = new ProcessBuilder("taskkill", "/f", "/IM", "Rserve.exe");
			} else {
				pb = new ProcessBuilder("pkill", "Rserve");
			}
			Process process = pb.start();
			process.waitFor(7L, TimeUnit.SECONDS);
		} finally {
			
			// Restore the prior security manager
			System.setSecurityManager(priorManager);
		}
	}
	
}
