/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
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
	protected PyTranslator pyTranslator = null;
	private final ReentrantLock startServerLock = new ReentrantLock();

	// string substitute vars
	protected Map<String, String> vars = new HashMap<>();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.pythonFileName = smssProp.getProperty(PYTHON_FILE_NAME, null);
		if (this.pythonFileName == null) {
			throw new IllegalArgumentException(
					"Please enter the name of the python file used to instantiate the function.");
		}

		this.engineDirectoryPath = EngineUtility.getSpecificEngineAssetsFolder(this.getCatalogType(),
				this.getEngineId(), this.getEngineName());
		this.engineDirectoryPath = this.engineDirectoryPath.replace("\\", "/");
		this.cacheFolder = new File(this.engineDirectoryPath + "/py");

		// vars for string substitution
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
	}

	protected void startServer(int port) {
		this.startServerLock.lock();
		try {
			// already created by another thread
			if (this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
				return;
			}

			// spin the server
			// start the client
			// get the startup command and parameters - at some point we need a better way
			// than the command

			// execute all the basic commands
			if (!this.cacheFolder.exists()) {
				this.cacheFolder.mkdirs();
			}

			// check if we have already created a process wrapper
			ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
			if (this.cpw != null) {
				this.cpw.shutdown(false);
			}

			String timeout = "30";
			if (this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
				timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
			}

			boolean debug = false;

			// pull the relevant values from the smss
			String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
			String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
			String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
			String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
			String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;

			if (port < 0) {
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						classLogger.warn("Function engine {} has an invalid FORCE_PORT value",
								SmssUtilities.getUniqueName(this.engineName, this.engineId));
					}
				}
			}

			String serverDirectory = this.cacheFolder.getAbsolutePath();
			boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from
											// python
			try {
				cpwToInit.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath,
						debug, timeout, loggerLevel);
			} catch (Exception e) {
				classLogger.error("Failed to create python process client for local function engine: {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				throw new IllegalArgumentException("Unable to connect to server for local python function engine.");
			}

			// create the py translator
			Insight processInsight = new Insight();
			InsightStore.getInstance().put(processInsight);
			this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

			try {
				// @formatter:off
				String execCommand = "import sys\n" 
						+ "import os\n" 
						+ "sys.path.append('" + this.engineDirectoryPath + "')\n" 
						+ "sys.path.append('" + this.engineDirectoryPath + "/py')\n" 
						+ "os.chdir('" + this.engineDirectoryPath + "')\n"
						+ "exec(open('" + this.engineDirectoryPath + "/" + this.pythonFileName + "').read())";
				// @formatter:on

				// execute all the basic commands
				String initCommands = this.smssProp.getProperty(INIT_FUNCTION_ENGINE);
				if (initCommands != null && !(initCommands = initCommands.trim()).isEmpty()) {
					// break the commands separated by ;
					String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
					// replace the Vars
					for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
						execCommand += "\n" + fillVars(commands[commandIndex]);
					}
				}

				this.pyTranslator.runScriptNoCancelTrace(execCommand);

				classLogger.info("Initializing '{}' python process with commands >>> {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), execCommand);

				// finally set the cpw in the class
				this.cpw = cpwToInit;
			} catch (Exception e) {
				classLogger.error("Failed to initialize python startup script for local function engine: {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				if (cpwToInit != null) {
					classLogger.warn(
							"Started python process for local function engine '{}' but the start script failed",
							SmssUtilities.getUniqueName(this.engineName, this.engineId));
					cpwToInit.shutdown(false);
				}
				throw e;
			}
		} finally {
			this.startServerLock.unlock();
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
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Insight executingInsight = (Insight) parameterValues.remove(Constants.INSIGHT);

		checkSocketStatus();

		StringBuilder callMaker = new StringBuilder(this.functionName);
		callMaker.append("(**").append(PyUtils.determineStringType(parameterValues)).append(")");

		return pyTranslator.runScriptNoCancelTrace(executingInsight, callMaker.toString());
	}

	@Override
	public void close() throws IOException {
		if (this.cpw != null) {
			this.cpw.shutdown(true);
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.LOCAL_PYTHON.name();
	}
}
