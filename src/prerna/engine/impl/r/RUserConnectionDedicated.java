package prerna.engine.impl.r;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RUserConnectionDedicated extends AbstractRUserConnection {
		
	// File structure
	private static final String R_FOLDER = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "R" + "/" + "Temp" + "/").replace('\\', '/');
	private static final String FS = System.getProperty("file.separator");
	
	// R binary location
	private static String rBin;
	static {
		String rHome = System.getenv("R_HOME").replace("\\", FS);
		if (rHome == null || rHome.isEmpty()) {
			rBin = "R"; // Just hope its in the path
		} else {
			rBin = rHome + FS + "bin" + FS + "R";
			if (SystemUtils.IS_OS_WINDOWS) rBin = rBin.replace(FS, "\\\\");
		}
	}
	
	// Host and port
	private final String host;
	private static final String DEFAULT_HOST = "127.0.0.1";
	private int port = 6311;
	private static final int PORT_MAX = 65535;
	
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
			startR();
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to initialize R.", e);
		}
	}
	
	@Override
	protected String getHost() {
		return host;
	}

	@Override
	protected int getPort() {
		return port;
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
		startR();
		initializeConnection();
		
		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
	}
	
	@Override
	public void stopR() throws Exception {

		// Need to allow this process to execute the below commands
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
		
		// Stop
		File tempFile = new File(R_FOLDER + Utility.getRandomString(12) + ".txt");
		try {
			if (SystemUtils.IS_OS_WINDOWS) {

				// Dump the output of netstat to a file
				ProcessBuilder pbNetstat = new ProcessBuilder("netstat", "-ano");
				pbNetstat.redirectOutput(tempFile);
				Process processNetstat = pbNetstat.start();
				processNetstat.waitFor(7L, TimeUnit.SECONDS);
				
				// Parse netstat output to get the PIDs of processes running on Rserve's port
				List<String> lines = FileUtils.readLines(tempFile, "UTF-8");
				List<String> pids = lines.stream()
						.filter(l -> l.contains("LISTENING")) // Only grab processes in LISTENING state
						.map(l -> l.trim().split("\\s+")) // Trim the empty characters and split into rows
						.filter(r -> r[1].contains(":" + port)) // Only use those that are listening on the right port 
						.map(r -> r[4]) // Grab the pid
						.collect(Collectors.toList());
				for (String pid : pids) {
					
					// Go through and kill these processes
					ProcessBuilder pbTaskkill = new ProcessBuilder("taskkill", "/PID", pid, "/F").inheritIO();
					Process processTaskkill = pbTaskkill.start();
					processTaskkill.waitFor(7L, TimeUnit.SECONDS);
					LOGGER.info("Stopped Rserve running on port " + port + " with the pid " + pid + ".");
				}

			} else {
					
				// Dump the output of lsof to a file
				ProcessBuilder pbLsof = new ProcessBuilder("lsof", "-t", "-i:" + port);
				pbLsof.redirectOutput(tempFile);
				Process processLsof = pbLsof.start();
				processLsof.waitFor(7L, TimeUnit.SECONDS);
				
				// Parse lsof output to get the PIDs of processes (in this case each line is just the pid)
				List<String> lines = FileUtils.readLines(tempFile, "UTF-8");
				for (String pid : lines) {
					ProcessBuilder pbKill = new ProcessBuilder("kill", "-9", pid).inheritIO();
					Process processKill = pbKill.start();
					processKill.waitFor(7L, TimeUnit.SECONDS);
					LOGGER.info("Stopped Rserve running on port " + port + " with the pid " + pid + ".");
				}
			}
		} finally {
			tempFile.delete();
			
			// Restore the prior security manager
			System.setSecurityManager(priorManager);
		}
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO >>>timb: R - need to complete cancellation here (later)
	}
	
	
	////////////////////////////////////////
	// Start Rserve process
	////////////////////////////////////////
	private void startR() throws Exception {
		
		// Need to allow this process to execute the below commands
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
		
		// Start
		try {
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			File output = new File(baseFolder + FS + "Rserve.output.log");
			File error = new File(baseFolder + FS + "Rserve.error.log");

			ProcessBuilder pb;
			if (SystemUtils.IS_OS_WINDOWS) {
				pb = new ProcessBuilder(rBin, "-e",
						"library(Rserve);" +
						"Rserve(FALSE," + port + ",args='--vanilla');");
			} else {
				pb = new ProcessBuilder(rBin, "CMD", "Rserve", "--vanilla", "--RS-port", port + "");
			}
			pb.redirectOutput(output);
			pb.redirectError(error);
			Process process = pb.start();
			process.waitFor(7L, TimeUnit.SECONDS);
		} finally {
			
			// Restore the prior security manager
			System.setSecurityManager(priorManager);
		}
	}
	
	
	////////////////////////////////////////
	// Port management
	////////////////////////////////////////
	private void setPort() {
		while (!isPortAvailable(port)) {
			port++;
			if (port > PORT_MAX) throw new IllegalArgumentException("No more ports are available."); 
		}
	}
	
	private static boolean isPortAvailable(int port) {
		try (ServerSocket s = new ServerSocket(port)) {
			return true;
		} catch (IOException e) {
			return false;
		}
	}

}
