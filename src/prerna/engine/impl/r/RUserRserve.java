package prerna.engine.impl.r;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RUserRserve {
	
	private static final Logger LOGGER = LogManager.getLogger(RUserRserve.class.getName());
	
	// File structure
	private static final String R_FOLDER = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "R" + "/" + "Temp" + "/").replace('\\', '/');
	private static final String FS = System.getProperty("file.separator");
	
	// Recovery
	private static final boolean ENABLE_RECOVERY = true;
	private String rDataFile;
	private static final String R_DATA_EXT = ".RData";

	// Host and port
	private String host;
	private static final String DEFAULT_HOST = "127.0.0.1";
	private int port = 6311;
	private static final int PORT_MAX = 65535;
		
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
	
	// R timeout
	private static final long R_TIMEOUT = 7L;
	private static final TimeUnit R_TIMEOUT_UNIT = TimeUnit.HOURS;
	
	// R connection
	private Object rconMonitor = new Object();
	private RConnection rcon;
	
	
	////////////////////////////////////////
	// Constructors, overloaded for defaults
	////////////////////////////////////////
	public RUserRserve(String rDataFileName, String host) {
		this.rDataFile = R_FOLDER + rDataFileName + R_DATA_EXT;
		this.host = host;
		try {
			setPort();
			startR();
			establishConnection();
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to initialize R.", e);
		}
	}

	public RUserRserve(String rDataFile) {
		this(rDataFile, DEFAULT_HOST);
	}

	public RUserRserve() {
		this(Utility.getRandomString(12));
	}
	
	
	////////////////////////////////////////
	// Raw R connection
	////////////////////////////////////////
	// TODO >>>timb: R - should get rid of this (later)
	/**
	 * Really want to get rid of this; should not be manipulating the rcon directly
	 * outside of this class
	 * 
	 * @return
	 */
	@Deprecated
	public RConnection getRConnection() {
		return rcon;
	}
	
	
	////////////////////////////////////////
	// Mirroring RConnection methods
	////////////////////////////////////////
	public REXP eval(String rScript) {
		return eval(rScript, true);
	}
	
	private REXP eval(String rScript, boolean retry) {
		if (isHealthy()) {
			LOGGER.info("Running R: " + rScript);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				Future<REXP> future = executor.submit(new Callable<REXP>() {
					@Override
					public REXP call() throws Exception {
						if (ENABLE_RECOVERY) saveImage();
						synchronized (rconMonitor) {
							return rcon.eval(rScript);
						}
					}
				});
				try {
					return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
				} catch (TimeoutException | InterruptedException e) {
					throw new IllegalArgumentException("Timout occured when running R script.", e);
				} catch (ExecutionException e) {
					throw new IllegalArgumentException("Failed to run R script.", e);
				}
			} finally {
				executor.shutdownNow();
			}
		} else {
			
			// If there was no exception with the recovery, then retry once more
			// Otherwise, throw the exception
			IllegalArgumentException e = recoveryStatus();
			if (e == null) {
				if (retry) {
					return eval(rScript, false);
				} else {
					throw new IllegalArgumentException("A recoverable error occured. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}
	
	public void voidEval(String rScript) {
		voidEval(rScript, true);
	}
	
	private void voidEval(String rScript, boolean retry) {
		if (isHealthy()) {
			LOGGER.info("Running R: " + rScript);
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					Future<Void> future = executor.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							if (ENABLE_RECOVERY) saveImage();
							synchronized (rconMonitor) {
								rcon.voidEval(rScript);
							}
							return null;
						}
					});
					try {
						future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						throw new IllegalArgumentException("Timout occured when running R script.", e);
					} catch (ExecutionException e) {
						throw new IllegalArgumentException("Failed to run R script.", e);
					}
				} finally {
					executor.shutdownNow();
				}
		} else {
			
			// If there was no exception with the recovery, then retry once more
			// Otherwise, throw the exception
			IllegalArgumentException e = recoveryStatus();
			if (e == null) {
				if (retry) {
					voidEval(rScript, false);
				} else {
					throw new IllegalArgumentException("A recoverable error occured. Please try re-running your R script.");
				}
			} else {
				throw e;
			}
		}
	}
	
	public RSession detach() {
		if (isHealthy()) {
			LOGGER.info("Detaching R.");
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					Future<RSession> future = executor.submit(new Callable<RSession>() {
						@Override
						public RSession call() throws Exception {
							if (ENABLE_RECOVERY) saveImage();
							synchronized (rconMonitor) {
								return rcon.detach();
							}
						}
					});
					try {
						return future.get(R_TIMEOUT, R_TIMEOUT_UNIT);
					} catch (TimeoutException | InterruptedException e) {
						throw new IllegalArgumentException("Timout occured when detaching R.", e);
					} catch (ExecutionException e) {
						throw new IllegalArgumentException("Failed to detach R.", e);
					}
				} finally {
					executor.shutdownNow();
				}
		} else {			
			throw recoveryStatus();
		}
	}
	
	
	////////////////////////////////////////
	// Cancellation
	////////////////////////////////////////
	public void cancelExecution() {
		// TODO >>>timb: R - need to complete cancellation here
	}
	
	
	////////////////////////////////////////
	// Recovery
	////////////////////////////////////////
	private IllegalArgumentException recoveryStatus() {
		IllegalArgumentException exception;
		String message = "Failed to connect to R. ";
		
		// Try and recover
		try {			
			recoverConnection();
			
			// If recovery is enabled, also try to reload the R data
			message += "The connection has recovered; however, your R data has been lost.";
			if (ENABLE_RECOVERY) {
				try {
					loadImage();
					exception = null;
				} catch (RserveException e) {
					exception = new IllegalArgumentException(message, e);
				}
			} else {
				exception = new IllegalArgumentException(message);
			}
		} catch (Exception e) {
			exception = new IllegalArgumentException(message, e);
		}
		
		return exception;
	}
	
	private void recoverConnection() throws Exception {
		
		// Try to stop R
		try {
			stopR();
		} catch (Exception e) {
			
			// If an error occurs stopping R, then grab a new port to run on
			setPort();
		}
		
		// Try to start R and establish a new connection to it
		startR();
		establishConnection();
		
		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
	}
	
	private void saveImage() throws RserveException {
		if (rDataFile == null) throw new IllegalArgumentException("Cannot save workspace image, as the RData file location is not defined.");
		if (!new File(rDataFile).getParentFile().exists()) throw new IllegalArgumentException("Cannot save workspace image, as the RData file folder is not defined.");
		synchronized (rconMonitor) {
			rcon.voidEval("save.image(file = \""+ rDataFile + "\")");
		}
	}
	
	private void loadImage() throws RserveException {
		if (rDataFile == null) throw new IllegalArgumentException("Cannot load workspace image, as the RData file location is not defined.");
		if (!new File(rDataFile).exists()) throw new IllegalArgumentException("Cannot load workspace image, as the RData file is not defined.");
		synchronized (rconMonitor) {
			rcon.voidEval("load(\"" + rDataFile + "\")");
		}
	}
	 
	
	////////////////////////////////////////
	// Rserve connection
	////////////////////////////////////////
	private void establishConnection() throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<RConnection> future = executor.submit(new Callable<RConnection>() {
				@Override
				public RConnection call() throws Exception {
					return new RConnection(host, port);
				}
			});
			rcon = future.get(7L, TimeUnit.SECONDS); 
		} finally {
			executor.shutdownNow();
		}
	}
	
	
	////////////////////////////////////////
	// Rserve process
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
				pb = new ProcessBuilder(rBin, "-e", "library(Rserve);Rserve(FALSE," + port
						+ ",args='--vanilla');flush.console <- function(...) {return;};options(error=function() NULL)",
						"--vanilla");
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
	
	/**
	 * Stops just the user-specific R process.
	 * @throws Exception
	 */
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
	
	
	////////////////////////////////////////
	// Health check
	////////////////////////////////////////
	private boolean isHealthy() {
		boolean beating = false; // Healthy skepticism
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<REXP> future = executor.submit(new Callable<REXP>() {
			@Override
			public REXP call() throws Exception {
				synchronized (rconMonitor) {
					return rcon.eval("1+2");
				}
			}
		});

		try {
			REXP heartBeat = future.get(3L, TimeUnit.SECONDS);
			if (((org.rosuda.REngine.REXP) heartBeat).asDouble() == 3L) {
				beating = true;
			}
		} catch (Exception e) {
			LOGGER.warn("R health check failed.");
		} finally {
			executor.shutdownNow();
		}
	
		return beating;
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
