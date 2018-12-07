package prerna.ds.py;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class PyUtils {
	
	private static final Logger LOGGER = LogManager.getLogger(PyUtils.class.getName());
	
	private static Boolean pyEnabled = null;
	private static PyUtils instance;
	
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
}
