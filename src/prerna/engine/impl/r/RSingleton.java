package prerna.engine.impl.r;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.DIHelper;

public class RSingleton {
	
	private static RConnection rcon = null;
	static int port = 6311;
	public static final String R_HOME = "R_HOME";
	public static final String R_PORTS = "R_PORTS";
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
			String rHome = System.getenv("R_HOME");
			System.out.println("RHome is ... " + rHome);
			
			// If R_HOME doesn't exist, then check RDF_Map
			if(rHome == null || rHome.isEmpty()) {
				rHome = DIHelper.getInstance().getProperty(R_HOME);
			}
			
			rHome = rHome.replace("\\", System.getProperty("file.separator"));

			rHome = rHome + System.getProperty("file.separator") + "bin" + System.getProperty("file.separator") + "R";

			if(SystemUtils.IS_OS_WINDOWS)
				rHome = rHome.replace(System.getProperty("file.separator"), "\\\\");

			System.out.println("RHome is ... " + rHome);
			
			ProcessBuilder pb = new ProcessBuilder("" + rHome + "", "-e", "library(Rserve);Rserve(FALSE," + port + ",args='--vanilla');flush.console <- function(...) {return;};options(error=function() NULL)", "--vanilla");
			Process process = pb.start();
			
			System.out.println("Waiting 1 second to allow Rserve to finish starting up...");
			try {
				process.waitFor(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			//ProcessBuilder pb = new ProcessBuilder("\"" + rHome + "\"", "-e", "\"library(Rserve);Rserve(FALSE,args='--vanilla --RS-port" + port + "');flush.console <- function(...) {return;}; options(error=function() NULL)\"", "--vanilla");

			//Process p = pb.start();
			//p.destroy();
			System.out.println("Started.. process builder.. !!");
			//Runtime.getRuntime().exec(" \"" + rHome + "\" -e \"library(Rserve);Rserve(FALSE,args='--vanilla --RS-port" + port + "');flush.console <- function(...) {return;}; options(error=function() NULL)\" --vanilla");
			//System.out.println("R Started.. going to end");
			//Runtime.getRuntime().exec(" \"C:\\Program Files\\R\\R-3.3.0\\bin\\R.exe\" -e \"library(Rserve); library(RSclient); rsc <- RSconnect(port = 6311); RSshutdown(rsc)\" --vanilla");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void stopRServe() {
		stopRServe(port);
	}
	
	public static void stopRServe(int port) {
		try {
			String rHome = System.getenv("R_HOME");
			
			rHome = rHome.replace("\\", System.getProperty("file.separator"));

			rHome = rHome + System.getProperty("file.separator") + "bin" + System.getProperty("file.separator") + "R";

			if(SystemUtils.IS_OS_WINDOWS)
				rHome = rHome.replace(System.getProperty("file.separator"), "\\\\");

			System.out.println("RHome is ... " + rHome);

//			Runtime.getRuntime().exec(" \"" + rHome+ "\" -e \"library(Rserve);library(RSclient); rsc<-RSconnect(port=" + port + ");RSshutdown(rsc) --vanilla");
			
			ProcessBuilder pb = new ProcessBuilder("" + rHome + "", "-e", "library(Rserve);library(RSclient);rsc<-RSconnect(port=" + port + ");RSshutdown(rsc)", "--vanilla");
			Process process = pb.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	
	public static RConnection getConnection(String host, int port) {
		
		// this basically tries to do the same as get connection with a port
		
		if(portToCon.containsKey(port)) {
			rcon = portToCon.get(port);
		} else {
			System.out.println("Making a master connection now on port " + port);
			try {
				rcon = new RConnection(host, port);
				portToCon.put(port, rcon);
				
			} catch(Exception ex) {
				ex.printStackTrace();
				// try to start again and see if that works
				startRServe(port);
				try {
					rcon = new RConnection(host, port);
					portToCon.put(port, rcon);
					RSingleton.port = port;
				} catch (RserveException e) {
					e.printStackTrace();
				}
			}
		}
		
		return rcon;
	}
	
	private static int getPortForRserve() {
		int port = 6311;
		int count = 0;
		
		String portsForR = DIHelper.getInstance().getProperty(R_PORTS);
		if(portsForR != null && !portsForR.isEmpty()) {
			if(portsForR.contains("-")) { // If a range is specified: start-end
				String[] portRange = portsForR.trim().replace(" ", "").split("-");
				int startPort = Integer.parseInt(portRange[0]);
				int endPort = Integer.parseInt(portRange[1]);
				while(startPort <= endPort) {
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
		
		System.out.println("Trying to see if port " + port + " is open for Rserve.");
		try {
			ServerSocket s = new ServerSocket(port);
			s.close();
			System.out.println("Success! Port: " + port);
			isOpen = true;
		} catch (Exception ex) {
			// Port isn't open, notify and move on
			System.out.println("Port " + port + " is unavailable.");
		}
		
		return isOpen;
	}
}