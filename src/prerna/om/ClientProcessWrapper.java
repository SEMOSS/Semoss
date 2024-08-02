package prerna.om;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;

import prerna.tcp.client.NativePySocketClient;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.MountHelper;
import prerna.util.PortAllocator;
import prerna.util.Utility;
import prerna.util.PythonUtils;

public class ClientProcessWrapper {

	private static final Logger classLogger = LogManager.getLogger(ClientProcessWrapper.class);

	private final Object lockCreate = new Object();
    private final Object lockDestroy = new Object();
	
	private SocketClient socketClient;
	private Process process;
	private String prefix;
	private int port;
	private String venvPath;
	private String serverDirectory;
	
	private boolean nativePyServer;
	private MountHelper chrootMountHelper;
	private String classPath;
	private boolean debug;
	private String timeout;
	private String loggerLevel;
	
	/**
	 * 
	 * @param nativePyServer
	 * @param chrootMountHelper
	 * @param port
	 * @param venvPath
	 * @param serverDirectory
	 * @param classPath
	 * @param debug
	 * @throws Exception
	 */
	public void createProcessAndClient(boolean nativePyServer,
			MountHelper chrootMountHelper,
			int port,
			String venvPath,
			String serverDirectory, 
			String classPath,
			boolean debug,
			String timeout,
			String loggerLevel) throws Exception 
	{
		synchronized(lockCreate) {
			this.nativePyServer = nativePyServer;
			this.chrootMountHelper = chrootMountHelper;
			this.classPath = classPath;
			this.port = calculatePort(port);
			this.venvPath = venvPath;
			this.serverDirectory = serverDirectory;
			this.debug = debug;
			this.loggerLevel = loggerLevel;
			this.timeout = timeout;
			if(this.timeout == null) {
				this.timeout = "-1";
			}
			boolean serverRunning = debug && port > 0;
			if(!serverRunning) {
				if(nativePyServer) {
					if(this.chrootMountHelper != null) {
						// for a user process - this will be something like /opt/user_id_randomid/
						Path chrootPath = Paths.get(this.chrootMountHelper.getTargetDirName());
						// we will be creating a fake semoss home in the chrooted directory
						// so grabbing the current base folder to mock the same pattern
						String baseFolderPath = Utility.getBaseFolder();
						// this is the fake semoss home in the chroot 
						Path chrootBaseFolderPath = Paths.get(chrootPath + baseFolderPath);
						// create a temp folder where we will start the process for the server
						Path serverDirectoryPath = Files.createTempDirectory(chrootBaseFolderPath, "a");
						this.serverDirectory = serverDirectoryPath.toString();
						// we need to have a relative path to replace the log4j file as it is started 
						// in the chroot world and not the base OS world
						// .. technically since i'm hard coding above the base folder from rdf_map, could replace but w/e
						String relative = chrootPath.relativize(serverDirectoryPath).toString();
						if(!relative.startsWith("/")) {
							relative ="/"+relative;
						}
						Utility.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);
						
						Object[] ret = Utility.startTCPServerNativePyChroot(this.chrootMountHelper.getTargetDirName(), relative, this.port+"", this.timeout, this.loggerLevel);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
					} else {
						// write the log4j file in the server directory
						Utility.writeLogConfigurationFile(this.serverDirectory);
											
						Object[] ret = PythonUtils.startTCPServerNativePy(this.serverDirectory, this.port+"", this.venvPath, this.timeout, this.loggerLevel);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
					}
				} else {
					if(chrootMountHelper != null) {
						// for a user process - this will be something like /opt/user_id_randomid/
						Path chrootPath = Paths.get(chrootMountHelper.getTargetDirName());
						// we will be creating a fake semoss home in the chrooted directory
						// so grabbing the current base folder to mock the same pattern
						String baseFolderPath = Utility.getBaseFolder();
						// this is the fake semoss home in the chroot 
						Path chrootBaseFolderPath = Paths.get(chrootPath + baseFolderPath);
						// create a temp folder where we will start the process for the server
						Path serverDirectoryPath = Files.createTempDirectory(chrootBaseFolderPath, "a");
						this.serverDirectory = serverDirectoryPath.toString();
						// we need to have a relative path to replace the log4j file as it is started 
						// in the chroot world and not the base OS world
						// .. technically since i'm hard coding above the base folder from rdf_map, could replace but w/e
						String relative = chrootPath.relativize(serverDirectoryPath).toString();
						if(!relative.startsWith("/")) {
							relative ="/"+relative;
						}
						Utility.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);
						
						this.process = Utility.startTCPServerChroot(classPath, this.chrootMountHelper.getTargetDirName(), relative, this.port+"");
					} else {
						// write the log4j file in the server directory
						Utility.writeLogConfigurationFile(this.serverDirectory);
						this.process = Utility.startTCPServer(classPath, this.serverDirectory, this.port+"");
					}
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
	}
	
	/**
	 * 
	 */
	public void shutdown(boolean cleanUpFolder) {
		synchronized(lockDestroy) {
			if(this.socketClient != null && this.socketClient.isConnected()) {
		        ExecutorService executor = Executors.newSingleThreadExecutor();
		
		        Callable<Boolean> callableTask = () -> {
		        	boolean result = false;
		        	if(cleanUpFolder) {
		        		this.socketClient.stopPyServe();
		        		classLogger.info("Sucessfully stopped the process");
		        		int attempt = 1;
		        		while(!result) {
		        			try {
		        				FileUtils.deleteDirectory(this.serverDirectory);
				        		classLogger.info("Sucessfully cleaned up the directory");
		        				result = true;
		        			} catch (Exception ignored) {
		        				classLogger.info("Failed attempt # " + attempt + " to delete the folder " + this.serverDirectory);
		        				attempt++;
		        				try {
		        					Thread.sleep(attempt * 1000);
		        				} catch (InterruptedException e1) {
		        					classLogger.error(Constants.STACKTRACE, e1);
		        				}
		        			}
		        		}
		        	} else {
		        		this.socketClient.stopPyServe();
		        		classLogger.info("Sucessfully stopped the process");
		        		result = true;
		        	}
		            return result;
		        };
		
		        Future<Boolean> future = executor.submit(callableTask);
		        try {
		        	// dont have the user wait forever...
		            Boolean result = future.get(70, TimeUnit.SECONDS);
		            if(result) {
		            	classLogger.info("Successfully shutdown the process");
		            } else {
		            	classLogger.warn("FAILED TO SUCCESSFULLY SHUTDOWN THE PROCESS / DELETE FOLDER ON PORT " + this.port);
		            	classLogger.warn("FAILED TO SUCCESSFULLY SHUTDOWN THE PROCESS / DELETE FOLDER ON PORT " + this.port);
		            	classLogger.warn("FAILED TO SUCCESSFULLY SHUTDOWN THE PROCESS / DELETE FOLDER ON PORT " + this.port);
		            	classLogger.warn("FAILED TO SUCCESSFULLY SHUTDOWN THE PROCESS / DELETE FOLDER ON PORT " + this.port);
		            	classLogger.warn("FAILED TO SUCCESSFULLY SHUTDOWN THE PROCESS / DELETE FOLDER ON PORT " + this.port);
		            	classLogger.warn("Assigning new port...");
		            	this.port = calculatePort(-1);
		            }
		        } catch (TimeoutException e) {
		        	classLogger.warn("Task did not finish within the timeout. Forcibly closing the process");
		        	try {
		        		// still call the close to shut down the io streams
		        		this.socketClient.close();
		    			this.process.destroy();
		    		} catch(Exception e2) {
		            	classLogger.error(Constants.STACKTRACE, e2);
		    		}
		            future.cancel(true); 
		        } catch (InterruptedException | ExecutionException e) {
		        	classLogger.error(Constants.STACKTRACE, e);
		        } finally {
		            executor.shutdown();
		            
		            // reset the venv path
		            this.venvPath = null;
		        }
			}
	//		// no socket but have a process? try to kill it
	//		else if(this.process != null){
	//			try {
	//    			this.process.destroy();
	//    		} catch(Exception e) {
	//            	classLogger.error(Constants.STACKTRACE, e);
	//    		}
	//		}
			// you know what, always try this...
			if(this.process != null){
				try {
	    			this.process.destroy();
	    		} catch(Exception e) {
	            	classLogger.error(Constants.STACKTRACE, e);
	    		}
			}
		}
		if(this.port > 0) {
			if(!PortAllocator.isPortAvailable(this.port)) {
            	classLogger.warn("PORT IS STILL IN USE BY OS " + this.port);
            	classLogger.warn("PORT IS STILL IN USE BY OS " + this.port);
            	classLogger.warn("PORT IS STILL IN USE BY OS " + this.port);
            	classLogger.warn("PORT IS STILL IN USE BY OS " + this.port);
            	classLogger.warn("PORT IS STILL IN USE BY OS " + this.port);
            	classLogger.warn("Assigning new port...");
				this.port = -1;
			}
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	public void reconnect() throws Exception {
		createProcessAndClient(nativePyServer, chrootMountHelper, port, venvPath, serverDirectory, classPath, debug, timeout, loggerLevel);
	}
	
	/**
	 * 
	 * @param venvEngineId
	 * @throws Exception
	 */
	public void reconnect(String venvEngineId) throws Exception {
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
		createProcessAndClient(nativePyServer, chrootMountHelper, port, venvPath, serverDirectory, classPath, debug, timeout, loggerLevel);
	}
	
	/**
	 * 
	 * @param port
	 * @return
	 */
	private int calculatePort(int port) {
		if(port < 0) {
			port = PortAllocator.getInstance().getNextAvailablePort();
		}
		
		return port;
	}
	
	/**
	 * 
	 * @return
	 */
	public SocketClient getSocketClient() {
		return socketClient;
	}

	/**
	 * 
	 * @param socketClient
	 */
	public void setSocketClient(SocketClient socketClient) {
		this.socketClient = socketClient;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getPrefix() {
		return prefix;
	}
	
	/**
	 * 
	 * @param prefix
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * 
	 * @return
	 */
	public Process getProcess() {
		return process;
	}
	
	/**
	 * 
	 * @param process
	 */
	public void setProcess(Process process) {
		this.process = process;
	}

	/**
	 * 
	 * @return
	 */
	public int getPort() {
		return port;
	}

	/**
	 * 
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * 
	 * @return
	 */
	public String getServerDirectory() {
		return serverDirectory;
	}

	/**
	 * 
	 * @param serverDirectory
	 */
	public void setServerDirectory(String serverDirectory) {
		this.serverDirectory = serverDirectory;
	}
	
	
//	  //chroot dir is created by MountHelper and initialized at /opt/user_id__sessionid
//    // when initalized, it has the semosshome folder present - ex. /opt/user_id__sessionid/opt/semosshome
//    
//    //assume /Users/kunalppatel9/Documents/Semoss_Docs/Daily_Notes/chroot is /opt/user_id__sessionid
//    String chrootDir = "/Users/kunalppatel9/Documents/Semoss_Docs/Daily_Notes/chroot";
//    String dir = "/opt/semosshome";
//    
//    Path chrootPath = Paths.get(Utility.normalizePath(chrootDir)); 
//    System.out.println("chrootDir: "+chrootDir.toString());
//
//    Path mainCachePath = Paths.get(chrootDir+dir); 
//    System.out.println("mainCachePath: "+mainCachePath.toString()); // /opt/user_id__sessionid/opt/semosshome
//    
//
//    //create the user py folder a123456 at /opt/user_id__sessionid/opt/semosshome/a123456
//    Path tempDirForUser = Files.createTempDirectory(mainCachePath, "a");
//    System.out.println("tempDirForUser: "+tempDirForUser.toString());// /opt/user_id__sessionid/opt/semosshome/a123456
//    
//    //i now don't know what the relative folder was inside the chroot from above, so i relativize it here
//    //chrootPath =  /opt/user_id__sessionid
//    //tempDirforUser =  /opt/user_id__sessionid/opt/semosshome/a123456
//    // relative = opt/semosshome/a123456
//    String relative = chrootPath.relativize(tempDirForUser).toString();
//    
//    if(!relative.startsWith("/")) {
//        relative ="/"+relative;
//    }
//    
//    System.out.println("relative: "+relative); // /opt/semosshome/a123456

	
	
}
