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
package prerna.engine.impl.guardrail;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * Base class for guardrail engines that are backed by a local python process.
 * Handles the shared lifecycle of standing up (and reconnecting to) the python
 * server and running the model startup script. Concrete engines only need to
 * provide the python startup script via {@link #getStartupScript()}.
 */
public abstract class AbstractPythonGuardrailReactorFunctionEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractPythonGuardrailReactorFunctionEngine.class);

	protected String engineDirectoryPath = null;
	protected File cacheFolder;
	protected ClientProcessWrapper cpw = null;
	protected PyTranslator pyTranslator = null;
	private final ReentrantLock startServerLock = new ReentrantLock();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.engineDirectoryPath = EngineUtility.getSpecificEngineAssetsFolder(this.getCatalogType(),
				this.getEngineId(), this.getEngineName());
		this.engineDirectoryPath = this.engineDirectoryPath.replace("\\", "/");
		this.cacheFolder = new File(this.engineDirectoryPath + "/py");
	}

	/**
	 * The python startup script that imports and initializes the model backing this
	 * guardrail engine. This is run once against the python server when it is first
	 * stood up.
	 *
	 * @return the python command(s) to initialize the guardrail model
	 */
	protected abstract String getStartupScript();

	protected void checkSocketStatus() {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
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
				classLogger.error("Unable to connect to python server for guardrail engine: {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				throw new IllegalArgumentException("Unable to connect to server for local python function engine.");
			}

			// create the py translator
			Insight processInsight = new Insight();
			InsightStore.getInstance().put(processInsight);
			this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

			try {
				String execCommand = getStartupScript();

				this.pyTranslator.runScriptNoCancelTrace(execCommand);

				// for debugging...
				classLogger.info("Initializing '{}' python process with commands >>> {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), execCommand);

				// finally set the cpw in the class
				this.cpw = cpwToInit;
			} catch (Exception e) {
				classLogger.error("Started python process for guardrail engine '{}' but the start script failed",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				if (cpwToInit != null) {
					cpwToInit.shutdown(false);
				}
				throw e;
			}
		} finally {
			this.startServerLock.unlock();
		}
	}

}
