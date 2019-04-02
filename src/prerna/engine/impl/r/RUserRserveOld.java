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
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.auth.User;
import prerna.util.DIHelper;

public class RUserRserveOld {
	
	private static final String FS = System.getProperty("file.separator");
	
	private static final String HOST = "127.0.0.1";
	private static final String R_HOME_KEY = "R_HOME";
	
	private static final int PORT_MAX = 65535;
	
	private static int port = 6311;
	private static String rBin;
		
	// Get the R binary location
	static {
		String rHome = System.getenv(R_HOME_KEY).replace("\\", FS);
		if (rHome == null || rHome.isEmpty()) {
			rBin = "R"; // Just hope its in the path
		} else {
			rBin = rHome + FS + "bin" + FS + "R";
			if (SystemUtils.IS_OS_WINDOWS) rBin = rBin.replace(FS, "\\\\");
		}
	}
		
	public synchronized static RConnection createConnection() {
		RConnection rcon;
		try {
			
			// Try establishing a connection to an existing Rserve
			rcon = getConnection();
			if (!isHealthy(rcon)) throw new IllegalArgumentException("The retrieved R connection failed a basic health check.");
		} catch (Exception e0) {
			try {
				
				// If that doesn't work, try starting Rserve
				startRserve();
				rcon = getConnection();
				if (!isHealthy(rcon)) throw new IllegalArgumentException("The retrieved R connection failed a basic health check.");
			} catch (Exception e1) {
				try {
					
					// If that doesn't work, try restarting Rserve
					stopRserve();
					startRserve();
					rcon = getConnection();
					if (!isHealthy(rcon)) throw new IllegalArgumentException("The retrieved R connection failed a basic health check.");
				} catch (Exception e3) {
					throw new IllegalArgumentException("Unable to establish R connection.", e3);
				}
			}
		}
		return rcon;
	}
	
	private static void setPort() {
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
	
	private static RConnection getConnection() throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<RConnection> future = executor.submit(new Callable<RConnection>() {
				@Override
				public RConnection call() throws Exception {
					return new RConnection(HOST, port);
				}
			});
			return future.get(3L, TimeUnit.SECONDS); 
		} finally {
			executor.shutdownNow();
		}
	}
	
	private static void startRserve() throws Exception {
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
	}

	public static void stopRserve() throws Exception {
		ProcessBuilder pb;
		if (SystemUtils.IS_OS_WINDOWS) {
			pb = new ProcessBuilder("taskkill", "/f", "/IM", "Rserve.exe");
		} else {
			pb = new ProcessBuilder("pkill", "Rserve");
		}
		Process process = pb.start();
		process.waitFor(7L, TimeUnit.SECONDS);
	}
		
	public static boolean isHealthy(RConnection rcon) {
		return isHealthy(rcon, null);
	}
	
	public static boolean isHealthy(RConnection rcon, Logger logger) {
		boolean beating = false; // Healthy skepticism
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<REXP> future = executor.submit(new Callable<REXP>() {
			@Override
			public REXP call() throws Exception {
				return rcon.eval("1+2");
			}
		});

		try {
			REXP heartBeat = future.get(3L, TimeUnit.SECONDS);
			if (((org.rosuda.REngine.REXP) heartBeat).asDouble() == 3L) {
				if (logger != null) logger.info("R health check passed");
				beating = true;
			}
		} catch (Exception e) {
			if (logger != null) logger.warn("R health check failed", e);
		} finally {
			executor.shutdownNow();
		}
	
		return beating;
	}

	public static void main(String[] args) throws Exception {
		port = 6311;
		DIHelper.getInstance().loadCoreProp("/Users/semoss/Documents/workspace/Semoss/RDF_Map.prop");
		startRserve();
		Thread.sleep(3000);
		ProcessBuilder pb = new ProcessBuilder("lsof", "-t", "-i:" + port);
		String tempFile = "/Users/semoss/Documents/workspace/Semoss/temp_safe_to_delete_me.txt";
		pb.redirectOutput(new File(tempFile));
		Process process = pb.start();
		process.waitFor(7L, TimeUnit.SECONDS);
		List<String> lines = FileUtils.readLines(new File(tempFile), "UTF-8");
		lines.stream().forEach(System.out::println);
		// TODO >>>timb: R - remove the file in finally block
		for (String pid : lines) {
			ProcessBuilder pb2 = new ProcessBuilder("kill", "-9", pid).inheritIO();
			Process process2 = pb2.start();
			process2.waitFor(7L, TimeUnit.SECONDS);
		}
		System.out.println("done");
	}
	
	public static void main1(String[] args) throws Exception { // TODO >>>timb: R - remove this main method when done
		port = 6311;
		DIHelper.getInstance().loadCoreProp("C:\\Users\\tbanach\\Documents\\Workspace\\Semoss\\RDF_Map.prop");
		startRserve();
		Thread.sleep(3000);
		ProcessBuilder pb = new ProcessBuilder("netstat", "-ano");
		String tempFile = "C:\\Users\\tbanach\\Documents\\Workspace\\Semoss\\temp_safe_to_delete_me.txt";
		pb.redirectOutput(new File(tempFile));
		Process process = pb.start();
		process.waitFor(7L, TimeUnit.SECONDS);
		List<String> lines = FileUtils.readLines(new File(tempFile), "UTF-8");
		List<String> filteredLines0 = lines.stream().filter(l -> l.contains(":" + port)).collect(Collectors.toList());
		filteredLines0.stream().forEach(System.out::println);
		List<String> filteredLines = lines.stream().filter(l -> l.contains(":" + port)).map(l -> l.trim().split("\\s+")[4]).collect(Collectors.toList());
		filteredLines.stream().forEach(System.out::println); // TODO >>>timb: R - remove the file in finally block
		for (String pid : filteredLines) {
			ProcessBuilder pb2 = new ProcessBuilder("taskkill", "/PID", pid, "/F").inheritIO();
			Process process2 = pb2.start();
			process2.waitFor(7L, TimeUnit.SECONDS);
		}
		// TODO >>>timb: R - test on mac / linux
		
	}
	
}