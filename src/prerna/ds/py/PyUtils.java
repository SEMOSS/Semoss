package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyUtils {
	
	private static final Logger LOGGER = LogManager.getLogger(PyUtils.class.getName());
	
	private static Boolean pyEnabled = null;
	private static PyUtils instance;
	public Map userTupleMap = new Hashtable();
	Map userProcessMap = new Hashtable();
	
	private PyUtils() {
		
	}
	
	public static PyUtils getInstance() throws IllegalArgumentException {
		if(instance == null) {
			setPyEnabled();
			if(pyEnabled) {
				instance = new PyUtils();
			}
		}
		
		return instance;
	}
	
	/**
	 * Method to set internally for this class if python is enabled
	 */
	private static void setPyEnabled() {
		if(pyEnabled == null) {
			pyEnabled = false;
			String usePythonStr =  DIHelper.getInstance().getProperty(Constants.USE_PYTHON);
			if(usePythonStr != null) {
				pyEnabled = Boolean.parseBoolean(usePythonStr);
			}
		}
	}
	
	/**
	 * Getter if python is enabled
	 * @return
	 */
	public static boolean pyEnabled() {
		if(pyEnabled == null) {
			pyEnabled = false;
			String usePythonStr =  DIHelper.getInstance().getProperty(Constants.USE_PYTHON);
			if(usePythonStr != null) {
				pyEnabled = Boolean.parseBoolean(usePythonStr);
			}
		}
		return pyEnabled;
	}
	
	/**
	 * Get a new JEP thread
	 * @return
	 */
	public PyExecutorThread getJep() {
		LOGGER.info(">>>STARTING PYTHON THREAD FOR USER<<<");
		PyExecutorThread py = new PyExecutorThread();
		py.start();
		return py;
	}
	
	/**
	 * Kill a current JEP thread
	 * @param py
	 */
	public void killPyThread(PyExecutorThread py) {
		if(py != null) {
			LOGGER.info(">>>>>> KILLING THREAD FOR USER <<<<<");
			py.killThread();
			Object monitor = py.getMonitor();
			synchronized(monitor) {
				monitor.notify();
			}
			LOGGER.info(">>>>>> COMPLETE <<<<<");
		}
	}
	
	// just gets the directory of where the main user has started
	public String getTempTupleSpace(Object user, String dir)
	{
		if(user != null && !userTupleMap.containsKey(user)) {
			try {
				LOGGER.info(">>>STARTING PYTHON TUPESPACE FOR USER<<<");
				// going to create this in insight cache dir
				//String mainCache = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
				Path mainCachePath = Paths.get(dir);
				Path tempDirForUser = Files.createTempDirectory(mainCachePath, "a");
				writeLogConfigurationFile(tempDirForUser.toString());
				userTupleMap.put(user, tempDirForUser.toString());
				// this should possibly also launch the thread
				String cp = DIHelper.getInstance().getProperty("PY_WORKER_CP");
				Process p = Utility.startTCPServer(cp, tempDirForUser.toString(), null);
				userProcessMap.put(user,  p);
				LOGGER.info(">>>TUPLS SPACE SET TO  " + tempDirForUser + " <<<");
				return tempDirForUser.toString();
			} catch (Exception e) {
				LOGGER.error(Constants.STACKTRACE, e);
			}
		} else {
			LOGGER.info("=== TUPLE SPACE NOT CREATED ====");
		}
		return null;
	}
	
	
	//chroot
	public String startTCPServe(Object user, String chrootDir, String paramDir, String port)
	{
		if(user != null && !userTupleMap.containsKey(user)) // || (user != null && user instanceof User && !((User)user).getTCPServer(false).isConnected()))
		{
			try {
				LOGGER.info(">>>STARTING PyServe USER<<<");
				// going to create this in insight cache dir
				//String mainCache = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
				Path chrootPath = Paths.get(chrootDir);
				Path mainCachePath = Paths.get(chrootDir+paramDir);
				Path tempDirForUser = Files.createTempDirectory(mainCachePath, "a");
				String relative = chrootPath.relativize(tempDirForUser).toString();
				if(!relative.startsWith("/")) {
					relative ="/"+relative;
				}
				LOGGER.info(">>>Creating Temp Dir at " + tempDirForUser.toString() + " <<<");
				writeLogConfigurationFile(tempDirForUser.toString(), relative);
				userTupleMap.put(user, tempDirForUser.toString());
				// this should possibly also launch the thread
				String cp = DIHelper.getInstance().getProperty("TCP_WORKER_CP");
				if(cp == null)
					LOGGER.info("Unable to see class path ");
				Process  p = Utility.startTCPServer(cp, chrootDir, relative.toString(), port);
				
				
				if(p != null) {
					userProcessMap.put(user,  p);
					
					// set the py process into the user
					if(user instanceof User)
						((User)user).setPyProcess(p);
				}
				LOGGER.info(">>>Pyserve Open on " + port + " <<<");
				return tempDirForUser.toString();
			} catch (Exception e) {
				LOGGER.error(Constants.STACKTRACE, e);
			}
		}
		else
			LOGGER.info("=== TUPLE SPACE NOT CREATED ====");
		return null;
	}
	

	public String startTCPServe(Object user, String dir, String port)
	{
		if(user != null && !userTupleMap.containsKey(user)) // || (user != null && user instanceof User && !((User)user).getTCPServer(false).isConnected()))
		{
			try {
				LOGGER.info(">>>STARTING PyServe USER<<<");
				// going to create this in insight cache dir
				//String mainCache = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
				Path mainCachePath = Paths.get(dir);
				Path tempDirForUser = Files.createTempDirectory(mainCachePath, "a");
				writeLogConfigurationFile(tempDirForUser.toString());
				userTupleMap.put(user, tempDirForUser.toString());
				// this should possibly also launch the thread
				String cp = DIHelper.getInstance().getProperty("TCP_WORKER_CP");
				if(cp == null)
					LOGGER.info("Unable to see class path ");
				Process  p = Utility.startTCPServer(cp, tempDirForUser.toString(), port);
				
				
				if(p != null) {
					userProcessMap.put(user,  p);
					
					// set the py process into the user
					if(user instanceof User)
						((User)user).setPyProcess(p);
				}
				LOGGER.info(">>>Pyserve Open on " + port + " <<<");
				return tempDirForUser.toString();
			} catch (Exception e) {
				LOGGER.error(Constants.STACKTRACE, e);
			}
		}
		else
			LOGGER.info("=== TUPLE SPACE NOT CREATED ====");
		return null;
	}

	
	private void writeLogConfigurationFile(String dir)
	{
		try {
			// read the file first
			dir = dir.replace("\\", "/");
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			File logFile = new File(baseFolder + "/py/log-config/log4j.properties");
			String logConfig = FileUtils.readFileToString(logFile);
			//property.filename = target/rolling/rollingtest.log
			logConfig = logConfig.replace("FILE_LOCATION", dir + "/output.log");
			File newLogFile = new File(dir + "/log4j2.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig);
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		}
		
	}
	
	//chroot
	private void writeLogConfigurationFile(String dir, String paramDir)
	{
		try {
			// read the file first
			dir = dir.replace("\\", "/");
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			File logFile = new File(baseFolder + "/py/log-config/log4j.properties");
			String logConfig = FileUtils.readFileToString(logFile);
			//property.filename = target/rolling/rollingtest.log
			logConfig = logConfig.replace("FILE_LOCATION", paramDir + "/output.log");
			File newLogFile = new File(dir + "/log4j2.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig);
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
		}
		
	}
		
	public void killTempTupleSpace(Object user) {
		// kill the process
		// take out the dir
		LOGGER.info(">>>KILLING PYTHON TUPESPACE FOR USER<<<");
		if(userTupleMap.containsKey(user)) {
			String dir = (String)userTupleMap.get(user);
			// change this to just creating a file so it is simpler
			File closer = new File(dir + "/alldone.closeall");
			try {
				ExecuteWatchdog p = (ExecuteWatchdog)userProcessMap.get(user);
				closer.createNewFile();
				p.destroyProcess();
				// delete the directory fully
				FileUtils.deleteDirectory(new File(dir));
			} catch(Exception e) {
				LOGGER.error(Constants.STACKTRACE, e);
			}
			userTupleMap.remove(user);
		}
		/*		
		if(dirProcessMap.containsKey(dir))
		{
			Process p = (Process)dirProcessMap.get(dir);
			p.destroyForcibly();
			try {
				FileUtils.deleteDirectory(new File(dir));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		*/		
		LOGGER.info(">>>KILLING PYTHON TUPESPACE FOR USER - COMPLETE<<<");
	}
	
	
}
