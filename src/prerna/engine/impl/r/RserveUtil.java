package prerna.engine.impl.r;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RserveUtil {

	protected static final Logger logger = LogManager.getLogger(RserveUtil.class);
	
	private static final String R_FOLDER = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "R" + "/" + "Temp" + "/").replace('\\', '/');
	private static final String R_DATA_EXT = ".RData";
	private static final String FS = java.nio.file.FileSystems.getDefault().getSeparator();
	
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
	
	private static final int PORT_MIN = 6311;
	private static final int PORT_MAX = 65535;
	
	
	//////////////////////////////////////////////////////////////////////
	// Rserve properties
	//////////////////////////////////////////////////////////////////////
	// TODO >>>timb: R - pull these from RDF map instead of the env vars
	private static final String IS_USER_RSERVE_KEY = "IS_USER_RSERVE";
	public static final boolean IS_USER_RSERVE = Boolean.parseBoolean(DIHelper.getInstance().getProperty(IS_USER_RSERVE_KEY));
			
	private static final String R_USER_CONNECTION_TYPE_KEY = "R_USER_CONNECTION_TYPE";
	public static final String R_USER_CONNECTION_TYPE = DIHelper.getInstance().getProperty(R_USER_CONNECTION_TYPE_KEY) != null
			? DIHelper.getInstance().getProperty(R_USER_CONNECTION_TYPE_KEY)
			: "undefined";
			
	private static final String RSERVE_CONNECTION_POOL_SIZE_KEY = "RSERVE_CONNECTION_POOL_SIZE";
	public static final int RSERVE_CONNECTION_POOL_SIZE = DIHelper.getInstance().getProperty(RSERVE_CONNECTION_POOL_SIZE_KEY) != null
			? Integer.parseInt(DIHelper.getInstance().getProperty(RSERVE_CONNECTION_POOL_SIZE_KEY))
			: 12;
			
	private static final String R_USER_RECOVERY_DEFAULT_KEY = "R_USER_RECOVERY_DEFAULT";
	public static final boolean R_USER_RECOVERY_DEFAULT = Boolean.parseBoolean(DIHelper.getInstance().getProperty(R_USER_RECOVERY_DEFAULT_KEY));
	
	
	//////////////////////////////////////////////////////////////////////
	// Connections
	//////////////////////////////////////////////////////////////////////
	public static RConnection connect(String host, int port) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<RConnection> future = executor.submit(new Callable<RConnection>() {
				@Override
				public RConnection call() throws Exception {
					return new RConnection(host, port);
				}
			});
			return future.get(7L, TimeUnit.SECONDS); 
		} finally {
			executor.shutdownNow();
		}
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Start and stop
	//////////////////////////////////////////////////////////////////////
	public static void startR(int port) throws Exception {
		
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
	
	public static void stopR(int port) throws Exception {
		
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
					logger.info("Stopped Rserve running on port " + port + " with the pid " + Utility.cleanLogString(pid) + ".");
				}

			} else {
					
				// Dump the output of lsof to a file
				ProcessBuilder pbLsof = new ProcessBuilder("lsof", "-t", "-i:" + port, "-sTCP:LISTEN");
				pbLsof.redirectOutput(tempFile);
				Process processLsof = pbLsof.start();
				processLsof.waitFor(7L, TimeUnit.SECONDS);
				
				// Parse lsof output to get the PIDs of processes (in this case each line is just the pid)
				List<String> lines = FileUtils.readLines(tempFile, "UTF-8");
				for (String pid : lines) {
					ProcessBuilder pbKill = new ProcessBuilder("kill", "-9", pid).inheritIO();
					Process processKill = pbKill.start();
					processKill.waitFor(7L, TimeUnit.SECONDS);
					logger.info("Stopped Rserve running on port " + port + " with the pid " + Utility.cleanLogString(pid) + ".");
				}
			}
		} finally {
			tempFile.delete();
			
			// Restore the prior security manager
			System.setSecurityManager(priorManager);
		}
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Recovery
	//////////////////////////////////////////////////////////////////////
	public static String getRDataFile(String rDataFileName) {
		return R_FOLDER + rDataFileName + R_DATA_EXT;
	}
	
	public static String getRDataFile(String rootDirectory, String rDataFileName) {
		rootDirectory = rootDirectory.replace('\\', '/');
		if (rootDirectory.endsWith("/")) {
			rootDirectory = rootDirectory.substring(0, rootDirectory.length() - 1);
		}
		return rootDirectory.replace('\\', '/') + '/' + rDataFileName + R_DATA_EXT;
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Ports
	//////////////////////////////////////////////////////////////////////
	public static int getOpenPort() {
		int port = PORT_MIN;
		while (!isPortAvailable(port)) {
			port++;
			if (port > PORT_MAX) throw new IllegalArgumentException("No more ports are available."); 
		}
		return port;
	}
	
	public static boolean isPortAvailable(int port) {
		try (ServerSocket s = new ServerSocket(port)) {
			return true;
		} catch (IOException e) {
			return false;
		}
	}
		
}
