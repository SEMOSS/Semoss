package prerna.tcp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;

import jep.Jep;
import prerna.sablecc2.reactor.frame.r.util.RJavaJriTranslator;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SocketServer implements Runnable {
	
	// basically a process which works by looking for commands in TCP space
	private static final String CLASS_NAME = SocketServer.class.getName();

	List <String> commandList = new ArrayList<String>(); // this is giving the file name and that too relative
	public static Logger classLogger = null;

	public Jep jep = null;
	boolean SSL = false;
	
	public volatile boolean keepAlive = true;
	public volatile boolean ready = false;
	public Object driverMonitor = null;
	
	Properties prop = null; // this is basically reference to the RDF Map
	public String mainFolder = null;
	//PyExecutorThread pt = null;
	RJavaJriTranslator rt = null;

	static boolean test = false;
	boolean done = false;
	
	Socket clientSocket = null;
	ServerSocket serverSocket = null;
	
	InputStream is = null;
	Object crash = new Object();
	
	SocketServerHandler ssh = new SocketServerHandler();
	String baseFolder = null;
		
	static boolean multi = false; // allow multiple threads at the same time
	
	public static void main(String [] args) {
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
			args[0] = "/Users/mahkhalil/workspace/Semoss_Dev/InsightCache/a1048966239358036599";
			args[1] = "/Users/mahkhalil/workspace/Semoss_Dev/RDF_Map.prop";;
			args[2] = "7777";
			args[3] = "r";
			args[4] = "mixed";
			test = true;
			multi = true;
		}
		
		String log4JPropFile = Paths.get(Utility.normalizePath(args[0]), "log4j2.properties").toAbsolutePath().toString();

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

		String rdfMapLocation = Utility.normalizePath(args[1]);
		Properties rdfMap = Utility.loadProperties(rdfMapLocation);
        System.out.println("loaded rdf map");
        classLogger.info("loaded rdf map");

		SocketServer worker = new SocketServer();
		worker.baseFolder = rdfMap.getProperty(Constants.BASE_FOLDER).replace('\\', '/');
		
		DIHelper.getInstance().loadCoreProp(rdfMapLocation);
		DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		worker.prop = rdfMap;

		worker.mainFolder = args[0];
		String engine = "r";
		if(args.length >= 4) {
			engine = args[3];
		}
		
		if(args.length >= 5) {
			worker.multi = args[4].equalsIgnoreCase("multi");
		}
		
		worker.bootServer(Integer.parseInt(args[2]), engine);
	}
	
	public void bootServer(int PORT, String engine) {
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
			        ssh.setPyBase(baseFolder + "/" + Constants.PY_BASE_FOLDER); 
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
		        ssh.mainFolder = mainFolder;
		        Thread readerThread = new Thread(ssh);
		        readerThread.start();
			} else {
				// just sleep
				// see if something crashed
				synchronized(crash) {
					try {
						crash.wait();
						clientSocket = null;
						ssh.cleanUp();
					} catch (InterruptedException e) {
						e.printStackTrace();
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
}
