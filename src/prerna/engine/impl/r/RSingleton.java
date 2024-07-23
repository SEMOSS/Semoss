package prerna.engine.impl.r;

import java.io.IOException;
import java.net.ServerSocket;
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
import prerna.util.Utility;

public class RSingleton {
	
	private static final Logger classLogger = LogManager.getLogger(RSingleton.class);

	private static RConnection rcon = null;
	
	public static final String R_HOME = "R_HOME";
	public static final String R_LIBS = "R_LIBS";
	public static final String R_PORTS = "R_PORTS";
	private static final String RSERVE_LOC = "/Rserve/libs/x64/Rserve";
	
	static int port = 6311;
	static Hashtable <Integer, RConnection> portToCon = new Hashtable<Integer, RConnection>(); // RServe is not allowing me to inspect the port so I have to do this.. sorry
	
	private RSingleton() {
		
	}
	
	/**
	 * Get the connection and build if doesn't exist
	 * @return
	 */
	public static RConnection getConnection() {
		if(rcon == null) {
			int port = getPortForRserve();
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
				pb = new ProcessBuilder(rHome, "CMD", rLibs+RSERVE_LOC, "--vanilla", "option(error=function() NULL)", "--RS-port", port + "");
			} else {
				pb = new ProcessBuilder(rHome, "CMD", "Rserve", "--vanilla", "option(error=function() NULL)", "--RS-port", port + "");
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
	
	private static int getPortForRserve() {
		int port = 6311;
		int count = 0;
	
		// need to also see if this is already running RServe and if so get that sorted out
		String portsForR = Utility.getDIHelperProperty(R_PORTS);
		if(portsForR != null && !portsForR.isEmpty()) {
			if(portsForR.contains("-")) { // If a range is specified: start-end
				String[] portRange = portsForR.trim().replace(" ", "").split("-");
				int startPort = Integer.parseInt(portRange[0]);
				int endPort = Integer.parseInt(portRange[1]);
				while(startPort <= endPort) {
					if(isRServe(startPort)) {
						// great - already running rserve
						port = startPort;
						break;
					}
					if(isPortOpen(startPort)) {
						port = startPort;
						break;
					} else {
						startPort++;
					}
				}
			} else if(portsForR.contains(",")) { // If multiple individual ports are specified: p1,p2,p3
				String[] portsToTry = portsForR.trim().replace(" ", "").split(",");
				for(int i = 0; i < portsToTry.length; i++) {
					int currPort = Integer.parseInt(portsToTry[i]);
					if(isRServe(currPort))
					{
						// great - already running rserve
						port = currPort;
						break;
					}
					if(isPortOpen(currPort)) {
						port = currPort;
						break;
					}
				}
			} else { // Just one port specified
				int onePort = Integer.parseInt(portsForR);
				if(isPortOpen(onePort)) {
					port = onePort;
				}
			}
		} else { // No port(s) specified so try 5 ports to see if we can find one that's open
			for( ; count < 5; port++, count++) {
				if(isPortOpen(port)) {
					break;
				}
			}
		}
		
		return port;
	}
	
	private static boolean isPortOpen(int port) {
		boolean isOpen = false;
		classLogger.info("Trying to see if port " + port + " is open for Rserve.");
		try {
			ServerSocket s = new ServerSocket(port);
			s.close();
			classLogger.info("Success! Port: " + port);
			isOpen = true;
		} catch (Exception ex) {
			// Port isn't open, notify and move on
			classLogger.info("Port " + port + " is unavailable.");
		}
		
		return isOpen;
	}
	
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