package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.Utility;

public class LocalPythonFunctionEngine extends AbstractFunctionEngine {
	
	private static final Logger classLogger = LogManager.getLogger(LocalPythonFunctionEngine.class);
	
	protected static final String INIT_FUNCTION_ENGINE = "INIT_FUNCTION_ENGINE";
	protected static final String PYTHON_FILE_NAME = "PYTHON_FILE_NAME";
	
	protected String pythonFileName;
	protected String engineDirectoryPath = null;
	protected File cacheFolder;
	
	protected ClientProcessWrapper cpw = null;
	protected PyTranslator pyt = null;

	// string substitute vars
	protected Map<String, String> vars = new HashMap<>();
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.pythonFileName = smssProp.getProperty(PYTHON_FILE_NAME, null);
		if(this.pythonFileName == null) {
			throw new IllegalArgumentException("Please enter the name of the python file used to instantiate the function.");
		}
		
		this.engineDirectoryPath = EngineUtility.getSpecificEngineBaseFolder(this.getCatalogType(), this.getEngineId(), this.getEngineName());
		this.engineDirectoryPath = this.engineDirectoryPath.replace("\\", "/");
		this.cacheFolder = new File(this.engineDirectoryPath + "/py");
		
		// vars for string substitution
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
	}

	protected synchronized void startServer(int port) {
		// already created by another thread
		if(this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
				
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands		
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdirs();
		}
		
		// check if we have already created a process wrapper
		ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
		if(this.cpw != null) {
			this.cpw.shutdown(false);
		} 
		
		String timeout = "30";
		if(this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
		
		boolean debug = false;
		
		// pull the relevant values from the smss
		String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
		String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
		String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
		String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
		
		if(port < 0) {
			// port has not been forced
			if(forcePort != null && !(forcePort=forcePort.trim()).isEmpty()) {
				try {
					port = Integer.parseInt(forcePort);
					debug = true;
				} catch(NumberFormatException e) {
					classLogger.warn("Function Engine " + this.getEngineName() + " has an invalid FORCE_PORT value");
				}
			}
		}
		
		String serverDirectory = this.cacheFolder.getAbsolutePath();
		boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from python
		try {
			cpwToInit.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath, debug, timeout, loggerLevel);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to server for local python function engine.");
		}
		
		// create the py translator
		pyt = new PyTranslator();
		pyt.setSocketClient(cpwToInit.getSocketClient());
		
		try {
			String execCommand = "import sys\n" 
					+ "import os\n" 
					+ "sys.path.append('" + this.engineDirectoryPath + "')\n" 
					+ "sys.path.append('" + this.engineDirectoryPath + "/py')\n" 
					+ "os.chdir('" + this.engineDirectoryPath + "')\n"
					+ "exec(open('" + this.engineDirectoryPath + "/" + this.pythonFileName + "').read())";

			// execute all the basic commands
			String initCommands = this.smssProp.getProperty(INIT_FUNCTION_ENGINE);
			if(initCommands != null && !(initCommands=initCommands.trim()).isEmpty()) {
				// break the commands separated by ;
				String [] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
				// replace the Vars
				for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
					execCommand += "\n" + fillVars(commands[commandIndex]);
				}
			}
			
			this.pyt.runScript(execCommand);

			classLogger.info("Initializing " + SmssUtilities.getUniqueName(this.engineName, this.engineId) 
								+ " python process with commands >>> " + String.join("\n", execCommand));
			
			// finally set the cpw in the class
			this.cpw = cpwToInit;
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			if(cpwToInit != null) {
				classLogger.warn("Able to start the python process for local python function engine " 
						+ SmssUtilities.getUniqueName(this.engineName, this.engineId) 
						+ " but the start script failed.");
				cpwToInit.shutdown(false);
			}
			throw e;
		}
		
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	protected String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	protected void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}
	
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		checkSocketStatus();
		
		StringBuilder callMaker = new StringBuilder(this.functionName);
		callMaker.append("(**")
				 .append(PyUtils.determineStringType(parameterValues))
				 .append(")");
		
		return pyt.runScript(callMaker.toString());
	}

	@Override
	public void close() throws IOException {
		if(this.cpw != null) {
			this.cpw.shutdown(true);
		}
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return "PYTHON_SCRIPT";
	}
}
