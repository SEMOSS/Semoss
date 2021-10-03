package prerna.tcp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;

import jep.Jep;
import prerna.ds.py.PyExecutorThread;
import prerna.sablecc2.reactor.frame.r.util.RJavaJriTranslator;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SocketServer implements Runnable
{
	
	// basically a process which works by looking for commands in TCP space
	private static final String CLASS_NAME = SocketServer.class.getName();

	List <String> commandList = new ArrayList<String>(); // this is giving the file name and that too relative
	public  static Logger LOGGER = null;

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
		
	static boolean multi = false; // allow multiple threads at the same time
	
	public static void main(String [] args)
	{
		
		
		// arg1 - the directory where commands would be thrown
		// arg2 - access to the rdf map to load
		// arg3 - port to start
		
		// create the watch service
		// start this thread
		
		// when event comes write it to the command
		// comment this for main execution
		//-Dlog4j.defaultInitOverride=TRUE
		
		if(args == null || args.length == 0)
		{
			args = new String[4];
			args[0] = "C:\\users\\pkapaleeswaran\\workspacej3\\SemossDev\\config";
			args[1] = "C:\\users\\pkapaleeswaran\\workspacej3\\SemossDev\\RDF_Map.prop";;
			args[2] = "9999";
			//args[3] = "py";
			args[3] = "r";
			test = true;
			multi = true;
				
		}
		
		String log4JPropFile = Paths.get(Utility.normalizePath(args[0]), "log4j2.properties").toAbsolutePath().toString();
		
		FileInputStream fis = null;
		ConfigurationSource source = null;
		try {
			fis = new FileInputStream(Utility.normalizePath(log4JPropFile));
			source = new ConfigurationSource(fis);
			//Configuration con = PropertiesConfigurationFactory.getInstance().getConfiguration(new LoggerContext(CLASS_NAME), source);
			//LOGGER = con.getLoggerContext().getLogger(CLASS_NAME);
			//LOGGER = Configurator.initialize(null, source).getLogger(CLASS_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		SocketServer worker = new SocketServer();
		DIHelper.getInstance().loadCoreProp(args[1]);
		DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		worker.prop = new Properties();
		
		try(FileInputStream fileInput = new FileInputStream(Utility.normalizePath(args[1]))) {
			worker.prop.load(fileInput);
			System.out.println("Loaded the rdf map");
			
			// get the library for jep
			//String jepLib = worker.prop.getProperty("JEP_LIB");
			
			//System.loadLibrary(jepLib);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		worker.mainFolder = args[0];
		String engine = "r"; // setting it up for r
		if(args.length >= 4)
			engine = args[3];
		
		if(args.length >= 5)
			worker.multi = args[4].equalsIgnoreCase("multi");
		
		worker.bootServer(Integer.parseInt(args[2]), engine);
	}
	
	public PyExecutorThread startPyExecutor()
	{
		PyExecutorThread pt = null;
		//if(this.pt== null)
		{
			pt = new PyExecutorThread();
			//pt.getJep();
			pt.start();
			
			
			while(!pt.isReady())
			{
				try {
					// sleep until we get the py
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			LOGGER.info("PyThread Started");
		}
		
		return pt;
	}
	
	
	public void bootServer(int PORT, String engine)
	{
		LOGGER = LogManager.getLogger(CLASS_NAME);


        try {
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            System.err.println("Could not listen on port: " + PORT);
            System.exit(1);
        }

        Thread listenerThread = new Thread(this);
        listenerThread.start();
        
	}
	
	
	// start listening for connections
	public void run()
	{
		// do the listening here and then spawn the thread
		while(!done)
		{
			if(this.clientSocket == null || multi)
			{
		        try {
		            clientSocket = serverSocket.accept();
		        } catch (IOException e) {
		            System.err.println("Accept failed.");
		            System.exit(1);
		        }	
		        try {
			        ssh = new SocketServerHandler();
		        	ssh.setLogger(LOGGER);
					ssh.setOutputStream(clientSocket.getOutputStream());
					is = clientSocket.getInputStream();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace(); 
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
			}
			else
			{
				// just sleep
				// see if something crashed
				synchronized(crash)
				{
					try {
						crash.wait();
						clientSocket = null;
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
}
