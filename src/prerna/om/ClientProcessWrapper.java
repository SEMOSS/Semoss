package prerna.om;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.tcp.client.NativePySocketClient;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.Utility;

public class ClientProcessWrapper {

	private static final Logger classLogger = LogManager.getLogger(ClientProcessWrapper.class);

	private SocketClient socketClient;
	private Process process;
	private String prefix;
	private int port;
	private String serverDirectory;
	
	private boolean nativePyServer;
	private String classPath;
	private boolean debug;
	
	/**
	 * 
	 * @param nativePyServer
	 * @param port
	 * @param serverDirectory
	 * @param classPath
	 */
	public synchronized void createProcessAndClient(boolean nativePyServer, 
			int port, 
			String serverDirectory, 
			String classPath,
			boolean debug) 
	{
		this.nativePyServer = nativePyServer;
		this.classPath = classPath;
		this.port = calculatePort(port);
		this.serverDirectory = serverDirectory;
		Utility.writeLogConfigurationFile(this.serverDirectory);
		this.debug = debug;
		
		boolean serverRunning = debug && port > 0;
		if(!serverRunning) {
			if(nativePyServer) {
				String timeout = "15";
				Object[] ret = Utility.startTCPServerNativePy(serverDirectory, this.port+"", timeout);
				this.process = (Process) ret[0];
				this.prefix = (String) ret[1];
			} else {
				this.process = Utility.startTCPServer(classPath, serverDirectory, this.port+"");
			}
		}
		
		try {
			if(this.nativePyServer) {
				this.socketClient = new NativePySocketClient();
			} else {
				this.socketClient = new SocketClient();
			}
			this.socketClient.connect("127.0.0.1", this.port, false);
			Thread t = new Thread(socketClient);
			t.start();
			while(!socketClient.isReady())
			{
				synchronized(socketClient)
				{
					try 
					{
						socketClient.wait();
						classLogger.info("Setting the socket client ");
					} catch (InterruptedException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public synchronized void shutdown() {
		if(this.socketClient != null && this.socketClient.isConnected()) {
	        ExecutorService executor = Executors.newSingleThreadExecutor();
	
	        Callable<String> callableTask = () -> {
	        	this.socketClient.stopPyServe(this.serverDirectory);
	    		this.socketClient.disconnect();
	            return "Successfully stopped the process";
	        };
	
	        Future<String> future = executor.submit(callableTask);
	        try {
	        	// wait 1 minute at most
	            String result = future.get(60, TimeUnit.SECONDS);
	            classLogger.info(result);
	        } catch (TimeoutException e) {
	        	classLogger.warn("Task did not finish within the timeout. Forcibly closing the process");
	        	try {
	    			this.process.destroy();
	    		} catch(Exception e2) {
	            	classLogger.error(Constants.STACKTRACE, e2);
	    		}
	            future.cancel(true); 
	        } catch (InterruptedException | ExecutionException e) {
	        	classLogger.error(Constants.STACKTRACE, e);
	        } finally {
	            executor.shutdown();
	        }
		} else if(this.process != null){
			try {
    			this.process.destroy();
    		} catch(Exception e) {
            	classLogger.error(Constants.STACKTRACE, e);
    		}
		}
	}
	
	public void reconnect() {
		createProcessAndClient(nativePyServer, port, serverDirectory, classPath, debug);
	}
	
	private int calculatePort(int port) {
		if(port < 0) {
			port = Integer.parseInt(Utility.findOpenPort());
		}
		
		return port;
	}
	
	public SocketClient getSocketClient() {
		return socketClient;
	}

	public void setSocketClient(SocketClient socketClient) {
		this.socketClient = socketClient;
	}
	
	public String getPrefix() {
		return prefix;
	}
	
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public Process getProcess() {
		return process;
	}
	
	public void setProcess(Process process) {
		this.process = process;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getServerDirectory() {
		return serverDirectory;
	}

	public void setServerDirectory(String serverDirectory) {
		this.serverDirectory = serverDirectory;
	}
	
}
