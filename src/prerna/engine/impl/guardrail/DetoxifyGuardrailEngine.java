package prerna.engine.impl.guardrail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyTransporter;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.Utility;

public class DetoxifyGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractPythonModelEngine.class);

	private static final String DEFAULT_THRESHOLD_KEY = "DEFAULT_THRESHOLD";
	private Double defaultThreshold = .7;
	
	private String engineDirectoryPath = null;
	private File cacheFolder;
	private ClientProcessWrapper cpw = null;
	private PyTranslator pyTranslator = null;

	public DetoxifyGuardrailEngine() {
		this.keysToGet = new String[] {"prompt", "threshold"};
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		String defaultThresholdStr = this.smssProp.getProperty(DEFAULT_THRESHOLD_KEY);
		if(defaultThresholdStr != null && !(defaultThresholdStr=defaultThresholdStr.trim()).isEmpty()) {
			try {
				defaultThreshold = Double.parseDouble(defaultThresholdStr);
			} catch(NumberFormatException e) {
				classLogger.warn("Invalid default threshold value " +defaultThresholdStr+". Revert to default value of "+defaultThreshold);
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		this.engineDirectoryPath = EngineUtility.getSpecificEngineBaseFolder(this.getCatalogType(), this.getEngineId(), this.getEngineName());
		this.engineDirectoryPath = this.engineDirectoryPath.replace("\\", "/");
		this.cacheFolder = new File(this.engineDirectoryPath + "/py");
		
		this.functionDescription = "Applying toxicity analysis on the following categoires ['toxicity', 'severe_toxicity', 'obscene', 'threat', 'insult', 'identity_attack']";
		this.parameters = new ArrayList<>();
		this.parameters.add(
				new FunctionParameter("prompt", 
				"String", 
				"This is the prompt we are applying the guardrail to"));
		this.parameters.add(new FunctionParameter("threshold", 
				"Double", 
				"Number between 0-1 for the probability threshold to apply across the categories to reject a prompt. The larger the value, the higher the probability of the prompt containing the category. The default value is "+defaultThreshold));
		this.requiredParameters = new ArrayList<>(Arrays.asList("prompt"));
	}
	
	@Override
	public GuardrailNounMetadata execute() {
		checkSocketStatus();
		organizeKeys();
		String prompt = this.keyValue.get(this.keysToGet[0]);
		double threshold = this.defaultThreshold;
		if(this.keyValue.containsKey("threshold")) {
			threshold = Double.parseDouble(this.keyValue.get("threshold"));
		}
		String script = "model.predict(\"\"\""+prompt+"\"\"\")";
		Map<String, Object> value = (Map<String, Object>) pyTranslator.runDirectPy(script);
		
		boolean pass = true;
		for(String category : value.keySet()) {
			// account if the type is return 
			Object categoryScore = value.get(category);
			double score = 0;
			if(categoryScore instanceof Number) {
				score = ((Number) categoryScore).doubleValue();
			} else {
				score = Double.parseDouble(categoryScore+"");
			}
			
			if(score > threshold) {
				pass = false;
			}
		}
		
		Map<String, Object> retValue = new HashMap<>();
		retValue.put("threshold", threshold);
		retValue.put("return", value);
		// we do not manipulate the prompt
		// so return as is
		return new GuardrailNounMetadata(pass, prompt, retValue);
	}

	private void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}
	
	private synchronized void startServer(int port) {
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
		PyTransporter pyTransporter = new PyTransporter();
		pyTransporter.setSocketClient(cpwToInit.getSocketClient());
		pyTranslator = new PyTranslator();
		pyTranslator.setInsight(new Insight());
		pyTranslator.setPyTransporter(pyTransporter);
		
		try {
			String execCommand = "from detoxify import Detoxify\n" 
					+ "model = Detoxify('original')"
					;

			this.pyTranslator.runScript(execCommand);
			
			// for debugging...
			classLogger.info("Initializing " + SmssUtilities.getUniqueName(this.engineName, this.engineId) 
								+ " python process with commands >>> " + execCommand);
			
			// finally set the cpw in the class
			this.cpw = cpwToInit;
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			if(cpwToInit != null) {
				classLogger.warn("Able to start the python process for detoxify guardrail engine " 
						+ SmssUtilities.getUniqueName(this.engineName, this.engineId) 
						+ " but the start script failed.");
				cpwToInit.shutdown(false);
			}
			throw e;
		}
	}
}
