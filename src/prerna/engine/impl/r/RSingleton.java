package prerna.engine.impl.r;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Utility;

public class RSingleton {
	
	private static final Logger classLogger = LogManager.getLogger(RSingleton.class);

	private static RConnection rcon = null;
	
	public static final String R_HOME = "R_HOME";
	public static final String R_LIBS = "R_LIBS";
	private static final String RSERVE_LOC = "/Rserve/libs/x64/Rserve";
	
	static int port = -1;
	static Hashtable <Integer, RConnection> portToCon = new Hashtable<Integer, RConnection>(); // RServe is not allowing me to inspect the port so I have to do this.. sorry
	
	private RSingleton() {
		port = PortAllocator.getInstance().getNextAvailablePort();
	}
	
	/**
	 * Get the connection and build if doesn't exist
	 * @return
	 */
	public static RConnection getConnection() {
		if(rcon == null) {
			int port = PortAllocator.getInstance().getNextAvailablePort();
			return getConnection("127.0.0.1", port);
		}
		return rcon;
	}
	
	public static RConnection getConnection(int port) {
		return getConnection("127.0.0.1", port);		
	}
	
	/**
	 * Get the rconn object as is. This maybe null
	 * @return
	 */
	public static RConnection getRCon() {
		return rcon;
	}
	
	public static void startRServe(int port) {
		try {
			String rHome = System.getenv(R_HOME);
			String rLibs = System.getenv(R_LIBS);
			classLogger.info("R_HOME env is is " + rHome);
			classLogger.info("R_LIBS env is is " + rLibs);

			// If R_HOME / R_LIBS doesn't exist, then check RDF_Map
			if(rHome == null || rHome.isEmpty()) {
				rHome = Utility.getDIHelperProperty(R_HOME);
			}
			if(rLibs == null || rLibs.isEmpty()) {
				rLibs = Utility.getDIHelperProperty(R_LIBS);
			}
			
			rHome = rHome.replace("\\", "/");
			rLibs = rLibs.replace("\\", "/");

			Path rHomePath = Paths.get(rHome);
			if (!Files.isDirectory(rHomePath)) {
				throw new IllegalArgumentException("rHome does not exist or is not a directory");
			}
			if(rLibs == null) {
				rLibs = rHome + "/library";
			}
			rHome = rHome + "/bin/R";
			classLogger.info("R_HOME for process is " + rHome);
			
			ProcessBuilder pb;
			if (SystemUtils.IS_OS_WINDOWS) {
				pb = new ProcessBuilder(rHome, "CMD", rLibs+RSERVE_LOC, "--vanilla", "--RS-port", port + "");
			} else {
				pb = new ProcessBuilder(rHome, "CMD", "Rserve", "--vanilla", "--RS-port", port + "");
			}
			
			Process process = pb.start();

			classLogger.info("Waiting 1 second to allow Rserve to finish starting up...");
			try {
				process.waitFor(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			classLogger.info("Started RServe process");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public static void stopRServe() {
		stopRServe(port);
	}
	
	public static void stopRServe(int port) {
		try {
			String rHome = System.getenv("R_HOME");
			if(rHome == null || rHome.isEmpty()) {
				rHome = Utility.getDIHelperProperty(R_HOME);
			}
			rHome = rHome.replace("\\", "/");

			Path rHomePath = Paths.get(rHome);
			if (!Files.isDirectory(rHomePath)) {
				throw new IllegalArgumentException("rHome does not exist or is not a directory");
			}
			rHome = rHome + "/bin/R";
			classLogger.info("R_HOME for process is " + rHome);
			
			ProcessBuilder pb = new ProcessBuilder("" + rHome + "", "-e", "library(Rserve);library(RSclient);rsc<-RSconnect(port=" + port + ");RSshutdown(rsc)", "--vanilla");
			Process process = pb.start();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	

	public static RConnection getConnection(String host, int port) {
		// this basically tries to do the same as get connection with a port
		if(portToCon.containsKey(port)) {
			rcon = portToCon.get(port);
		} else {
			classLogger.info("Making a master connection now on port " + port);
			try {
				rcon = new RConnection(host, port);
				portToCon.put(port, rcon);
			} catch(Exception ex) {
				classLogger.error(Constants.STACKTRACE, ex);
				// try to start again and see if that works
				startRServe(port);
				try {
					rcon = new RConnection(host, port);
					portToCon.put(port, rcon);
					RSingleton.port = port;
				} catch (RserveException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		if(rcon == null) {
			classLogger.info("Generating master connection on port " + port + " is null");
		}

		return rcon;
	}
	
	/**
	 * KEEP THIS EVEN THOUGH NOT USED
	 * INCASE WE WNAT TO EVENTUALLY LOOK AT ALL THE PORTS IN PORT ALLOCATOR 
	 * AND ATTEMPT TO USE THAT IF ITS AN RSERVE
	 * @param port
	 * @return
	 */
	private static boolean isRServe(int port) {
		// try to see if this port is already running RServe
		boolean isRserve = false;
		classLogger.info("Trying to see if port " + port + " is already running Rserve.");
		try {
			rcon = new RConnection("127.0.0.1", port);
			portToCon.put(port, rcon);
			classLogger.info("Success! RServe: " + port);
			isRserve = true;
		} catch (Exception ex) {
			// Port isn't open, notify and move on
			classLogger.info("Port " + port + " is unavailable.");
		}
		
		return isRserve;
	}
	
}