package prerna.tcp;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SocketServer implements Runnable {
	
	// basically a process which works by looking for commands in TCP space
	private static final String CLASS_NAME = SocketServer.class.getName();

	private static Logger classLogger = null;

	private Properties prop = null; // this is basically reference to the RDF Map
	private String socketDir = null;

	private boolean done = false;
	
	private Socket clientSocket = null;
	private ServerSocket serverSocket = null;
	
	private InputStream is = null;
	Object crash = new Object();
	
	private SocketServerHandler ssh = new SocketServerHandler();
	private String baseFolder = null;
		
	private static boolean multi = false; // allow multiple threads at the same time
	public static boolean testMode = false;
	
	public static void main(String [] args) throws Exception {
		// arg1 - the directory where commands would be thrown
		// arg2 - access to the rdf map to load
		// arg3 - port to start
		
		// create the watch service
		// start this thread
		
		// when event comes write it to the command
		// comment this for main execution
		//-Dlog4j.defaultInitOverride=TRUE
		
		if(args == null || args.length == 0) {
			args = new String[5];
			args[0] = "C:\\workspace\\Semoss_Dev\\InsightCache\\z1";
			args[1] = "C:\\workspace\\Semoss_Dev\\RDF_Map.prop";;
			args[2] = "9999";
			args[3] = "r";
			args[4] = "mixed";
			multi = true;
			testMode = true;
		}
		
		if(args.length < 3) {
			throw new IllegalArgumentException("Must pass in at least 3 inputs - the log4j file, the rdf file map, and the port to run the socket on");
		}
		
		// this socket dir should have the log4j file contianer inside it
		String socketDir = args[0];
		String rdfMapInput = args[1];
		String portInput = args[2];
		
		String log4JPropFile = Paths.get(Utility.normalizePath(socketDir), "log4j2.properties").toAbsolutePath().toString();

		
		// set to say this is not core
		DIHelper.getInstance().setLocalProperty("core", "false");
		
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(Utility.normalizePath(log4JPropFile));
			new ConfigurationSource(fis);
		} catch (IOException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		classLogger = LogManager.getLogger(CLASS_NAME);

		int port = -1;
		try {
			port = Integer.parseInt(portInput);
		} catch(NumberFormatException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Input integer input for port='" + portInput+"'");
		}
		
		String rdfMapLocation = Utility.normalizePath(rdfMapInput);
		Properties rdfMap = Utility.loadProperties(rdfMapLocation);
        System.out.println("loaded rdf map");
        classLogger.info("loaded rdf map");

		SocketServer worker = new SocketServer();
		worker.baseFolder = rdfMap.getProperty(Constants.BASE_FOLDER).replace('\\', '/');
		
		DIHelper.getInstance().loadCoreProp(rdfMapLocation);
		DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		worker.prop = rdfMap;

		worker.socketDir = socketDir;
		String engine = "r";
		if(args.length >= 4) {
			engine = args[3];
		}
		if(args.length >= 5) {
			SocketServer.multi = args[4].equalsIgnoreCase("multi");
		}
		
		worker.bootServer(port, engine);
	}
	
	public void bootServer(final int PORT, String engine) {
        try {
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
            System.err.println("Could not listen on port: " + PORT);
            System.exit(1);
        }
        System.out.println("server started");
        classLogger.info("server started");

        Thread listenerThread = new Thread(this);
        listenerThread.start();
	}
	
	// start listening for connections
	public void run() {
		// do the listening here and then spawn the thread
			while(!done) {
			if(this.clientSocket == null || multi) {
		        try {
		            clientSocket = serverSocket.accept();
		        } catch (IOException e) {
					e.printStackTrace();
					classLogger.error(Constants.STACKTRACE, e);
		            System.err.println("Accept failed.");
		            System.exit(1);
		        }	
		        try {
			        ssh = new SocketServerHandler();
			        DIHelper.getInstance().setLocalProperty("SSH", ssh);
		        	ssh.setLogger(classLogger);
					ssh.setOutputStream(clientSocket.getOutputStream());
					is = clientSocket.getInputStream();
				} catch (IOException e) {
					e.printStackTrace();
					classLogger.error(Constants.STACKTRACE, e);
				}   
		        
		        // start processing
		        // start a new thread
		        //PyExecutorThread pt = startPyExecutor();

		        //ssh.setPyExecutorThread(pt);
		        ssh.is = is;
		        ssh.socket = serverSocket;
		        ssh.server = this;
		        ssh.mainFolder = socketDir;
		        
		        Thread readerThread = new Thread(ssh);
		        readerThread.start();
			} else {
				// just sleep
				// see if something crashed
				synchronized(crash) {
					try {
						crash.wait();
						clientSocket = null;
						closeStream(clientSocket);
						closeStream(serverSocket);
						closeStream(is);
						if(!testMode)	
							ssh.cleanUp();
					} catch (InterruptedException e) {
						e.printStackTrace();
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
    private void closeStream(Closeable closeThis) {
    	try {
			closeThis.close();
		} catch (IOException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		}
    }
	
	public static boolean isMulti() {
		return multi;
	}
}
