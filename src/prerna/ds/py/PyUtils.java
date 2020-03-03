package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyUtils {
	
	private static final Logger LOGGER = LogManager.getLogger(PyUtils.class.getName());
	
	private static Boolean pyEnabled = null;
	private static PyUtils instance;
	Map userTupleMap = new Hashtable();
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
			py.killThread();
			LOGGER.info(">>>>>> KILLING THREAD FOR USER <<<<<");
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
		if(user != null && !userTupleMap.containsKey(user))
		{
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
				Process p = Utility.startPyProcess(cp, tempDirForUser.toString());
				userProcessMap.put(user,  p);
				LOGGER.info(">>>TUPLS SPACE SET TO  " + tempDirForUser + " <<<");
				return tempDirForUser.toString();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
			logConfig = logConfig.replace("FILE_LOCATION", dir + "/output.log");
			File newLogFile = new File(dir + "/log4j.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
	public void killTempTupleSpace(Object user)
	{
		// kill the process
		// take out the dir
		LOGGER.info(">>>KILLING PYTHON TUPESPACE FOR USER<<<");
		
		if(userTupleMap.containsKey(user))
		{
			String dir = (String)userTupleMap.get(user);
		// change this to just creating a file so it is simpler
			File closer = new File(dir + "/alldone.closeall");
			try
			{
				Process p = (Process)userProcessMap.get(user);
				closer.createNewFile();
				p.destroyForcibly();
				// delete the directory fully
				FileUtils.deleteDirectory(new File(dir));
			}catch(Exception ex)
			{
				
			}
			userTupleMap.remove(user);
		}
/*		if(dirProcessMap.containsKey(dir))
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
*/		LOGGER.info(">>>KILLING PYTHON TUPESPACE FOR USER - COMPLETE<<<");
		
	}
	
}
