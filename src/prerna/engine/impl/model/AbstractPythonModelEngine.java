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
package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.BatchListResponse;
import prerna.engine.impl.model.responses.BatchResultsResponse;
import prerna.engine.impl.model.responses.BatchStatusResponse;
import prerna.engine.impl.model.responses.BatchSubmissionResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.MultiModalEmbeddingsModelEngineResponse;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * This class is responsible for creating a {@code IModelEngine} class that is
 * directly linked to a python process. The corresponding python class should
 * handle all method implementations. This java class is simply mechanism to
 * forward calls to the python process.
 */
public abstract class AbstractPythonModelEngine extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractPythonModelEngine.class);

	// python server
	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;

	protected PyTranslator pyTranslator = null;
	protected File cacheFolder;
	private ClientProcessWrapper cpw = null;
	private final ReentrantLock startServerLock = new ReentrantLock();

	protected String varName = null;

	// string substitute vars
	protected Map<String, String> vars = new HashMap<>();

	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		if (!this.smssProp.containsKey(Settings.VAR_NAME)) {
			String randomString = "v_" + Utility.getRandomString(6);
			this.varName = randomString;
			this.smssProp.put(Settings.VAR_NAME, randomString);
		} else {
			this.varName = this.smssProp.getProperty(Settings.VAR_NAME);
		}

		// vars for string substitution
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
	}

	/**
	 * Gets a PyTranslator instance
	 * 
	 * @return A configured PyTranslator instance
	 * @throws IllegalArgumentException if insight is null
	 * @throws IllegalStateException    if the engine is not properly initialized or
	 *                                  connection fails
	 */
	public PyTranslator getEnginePyTranslator() {
		try {
			this.checkSocketStatus();
			return this.pyTranslator;
		} catch (Exception e) {
			classLogger.error("Failed to create PyTranslator for engine: {}",
					SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
			throw new IllegalStateException("Failed to get PyTranslator: " + e.getMessage(), e);
		}
	}

	/**
	 * This method is responsible for starting the python process that is linked to
	 * this model engine.
	 * 
	 * @param port The port number to use when creating the server/client
	 *             connection.
	 */
	protected void startServer(int port) {
		this.startServerLock.lock();
		try {
			if (this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
				return;
			}
			if (this.workingDirectoryBasePath == null) {
				this.createCacheFolder();
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
			if (cpwToInit.getSocketClient() == null) {
				boolean debug = false;

				// pull the relevant values from the smss
				String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
				String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
				String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "INFO");
				String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
				String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;

				if (port < 0) {
					// port has not been forced
					if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
						try {
							port = Integer.parseInt(forcePort);
							debug = true;
						} catch (NumberFormatException e) {
							// ignore
							classLogger.warn("Model {} has an invalid FORCE_PORT value",
									SmssUtilities.getUniqueName(this.engineName, this.engineId));
						}
					}
				}

				String serverDirectory = this.cacheFolder.getAbsolutePath();
				// it has to be -- don't change this unless you can send engine calls from
				// python
				boolean nativePyServer = true;
				try {
					cpwToInit.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory,
							customClassPath, debug, timeout, loggerLevel);
				} catch (Exception e) {
					classLogger.error("Failed to create the python process for engine: {}",
							SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
					throw new IllegalArgumentException("Unable to connect to server for python model engine.");
				}
			} else if (!cpwToInit.getSocketClient().isConnected()) {
				cpwToInit.shutdown(false);
				try {
					cpwToInit.reconnect();
				} catch (Exception e) {
					classLogger.error("Failed to reconnect to the python process for engine: {}",
							SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
					throw new IllegalArgumentException("Failed to start TCP Server for Python Model Engine: "
							+ SmssUtilities.getUniqueName(this.engineName, this.engineId));
				}
			}

			// create the py translator
			Insight processInsight = new Insight();
			InsightStore.getInstance().put(processInsight);
			this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

			try {
				// execute all the basic commands
				String initCommands = this.smssProp.getProperty(Constants.INIT_MODEL_ENGINE);
				// break the commands seperated by ;
				String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
				// replace the Vars
				for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
					commands[commandIndex] = fillVars(commands[commandIndex]);
				}
				this.pyTranslator.runEmptyPyNoCancelTrace(commands);
				// for debugging...
				classLogger.info("Initializing '{}' python process with commands >>> {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), String.join("\n", commands));

				// run a prefix command
				setPrefix(cpwToInit);

				// finally set the cpw in the class
				this.cpw = cpwToInit;
			} catch (Exception e) {
				classLogger.error("Failed to  to the python process for engine: {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				if (cpwToInit != null) {
					classLogger.warn(
							"Able to start the python process for the python model engine {} but the start script failed",
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
	 * Whether the caller already named an output token limit under any of the
	 * aliases the python message builders accept.
	 */
	private static boolean hasMaxTokensParam(Map<String, Object> parameters) {
		return parameters.containsKey(MAX_TOKENS) || parameters.containsKey("max_completion_tokens")
				|| parameters.containsKey("max_output_tokens") || parameters.containsKey("max_new_tokens");
	}

	/**
	 * This method checks whether the socket client is instantiated and connected.
	 */
	protected void checkSocketStatus() {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}

	/**
	 * 
	 */
	private void setPrefix(ClientProcessWrapper cpwToInit) {
		this.prefix = cpwToInit.getPrefix();
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = new String[] { "prefix", this.prefix };
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		cpwToInit.getSocketClient().executeCommand(prefixPayload);
	}

	@Override
	public AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> parameters) {
		if (ModelInferenceLogsUtils.isRoomInActive(insight.getUserId(), roomId)) {
			throw new IllegalArgumentException(
					"The room being referenced has been permanently closed. Please open a new room");
		}
		checkSocketStatus();

		// ride the engine's saved built-in tool selection and max output
		// tokens along on the request unless the caller supplied their own
		if (this.builtinTools != null || this.maxTokens != null) {
			if (parameters == null) {
				parameters = new HashMap<>();
			}
			if (this.builtinTools != null && !parameters.containsKey(BUILT_IN_TOOLS)) {
				parameters.put(BUILT_IN_TOOLS, this.builtinTools);
			}
			if (this.maxTokens != null && !hasMaxTokensParam(parameters)) {
				parameters.put(MAX_TOKENS, this.maxTokens);
			}
		}

		parameters = applyReasoningParameters(parameters);
		parameters = applyTemperatureParameter(parameters);

		StringBuilder callMaker = new StringBuilder(varName + ".ask(");
		if (parameters != null && !parameters.isEmpty()) {
			Iterator<Map.Entry<String, Object>> paramEntries = parameters.entrySet().iterator();
			boolean isFirst = true;
			while (paramEntries.hasNext()) {
				Map.Entry<String, Object> entry = paramEntries.next();
				if (!isFirst) {
					callMaker.append(", ");
				}
				callMaker.append(entry.getKey()).append("=").append(PyUtils.determineStringType(entry.getValue()));
				isFirst = false;
			}
		}

		if (this.prefix != null) {
			callMaker.append(", prefix='").append(prefix).append("'");
		}

		callMaker.append(")");

		classLogger.debug("Running model command {}", callMaker.toString());

		Object output = pyTranslator.runDirectPyNoCancelTrace(insight, callMaker.toString());
		AskModelEngineResponse response = null;
		try {
			response = AskModelEngineResponse.fromObject(output);
		} catch (Exception e) {
			classLogger.error("Could not create response object from output: {}", output, e);
			throw new IllegalArgumentException(e.getMessage());
		}

		// DON'T UPDATE CHAT HISTORY IF RESPONSE IS AN ERRROR
		if (response instanceof AskErrorModelEngineResponse) {
			classLogger.warn("Model returned an error: {}", response.getStringResponse());
			return response;
		}

		return response;
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		checkSocketStatus();

		String pythonListAsString = PyUtils.determineStringType(stringsToEmbed);

		StringBuilder callMaker = new StringBuilder();
		callMaker.append(varName).append(".embeddings(strings_to_embed = ").append(pythonListAsString);

		if (this.prefix != null) {
			callMaker.append(", prefix='").append(this.prefix).append("'");
		}

		if (parameters != null && !parameters.isEmpty()) {
			Iterator<String> paramKeys = parameters.keySet().iterator();
			while (paramKeys.hasNext()) {
				String key = paramKeys.next();
				Object value = parameters.get(key);
				callMaker.append(",").append(key).append("=").append(PyUtils.determineStringType(value));
			}
		}

		callMaker.append(")");

		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		EmbeddingsModelEngineResponse response = null;
		try {
			response = EmbeddingsModelEngineResponse.fromObject(output);
		} catch (Exception e) {
			classLogger.error("Could not create response object from output: {}", output, e);
			throw new IllegalArgumentException(e.getMessage());
		}
		return response;
	}

	@Override
	public MultiModalEmbeddingsModelEngineResponse multiModalEmbeddings(List<String> text, List<String> image,
			List<String> video, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();

		if (text == null) {
			text = new ArrayList<>();
		}
		if (image == null) {
			image = new ArrayList<>();
		}
		if (video == null) {
			video = new ArrayList<>();
		}

		StringBuilder callMaker = new StringBuilder();
		callMaker.append(varName).append(".multi_modal_embeddings(").append("text = ")
				.append(PyUtils.determineStringType(text)).append(", image = ")
				.append(PyUtils.determineStringType(image)).append(", video = ")
				.append(PyUtils.determineStringType(video));

		if (parameters != null && !parameters.isEmpty()) {
			Iterator<String> paramKeys = parameters.keySet().iterator();
			while (paramKeys.hasNext()) {
				String key = paramKeys.next();
				Object value = parameters.get(key);
				callMaker.append(",").append(key).append("=").append(PyUtils.determineStringType(value));
			}
		}

		callMaker.append(")");

		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		MultiModalEmbeddingsModelEngineResponse response = null;
		try {
			response = MultiModalEmbeddingsModelEngineResponse.fromObject(output);
		} catch (Exception e) {
			classLogger.error("Could not create response object from output: {}", output, e);
			throw new IllegalArgumentException(e.getMessage(), e);
		}
		return response;
	}

	// ------------------------------------------------------------------
	// Batch model calls -- delegate to the Python client (same hop as askCall).
	// Native batch is gated to OpenAI / Azure OpenAI / Anthropic model types;
	// other Python engines inherit these overrides but reject via supportsBatch().
	// ------------------------------------------------------------------

	private static final Gson BATCH_GSON = new Gson();

	@Override
	public boolean supportsBatch() {
		ModelTypeEnum type = this.getModelType();
		return type == ModelTypeEnum.OPEN_AI || type == ModelTypeEnum.AZURE_OPEN_AI || type == ModelTypeEnum.ANTHROPIC;
	}

	private void assertBatchSupported() {
		if (!supportsBatch()) {
			throw new UnsupportedOperationException(
					"Batch model calls are not supported for model type " + this.getModelType());
		}
	}

	private void appendKwargs(StringBuilder callMaker, Map<String, Object> parameters, boolean hasPrior) {
		if (parameters == null || parameters.isEmpty()) {
			return;
		}
		boolean prior = hasPrior;
		for (Map.Entry<String, Object> entry : parameters.entrySet()) {
			if (entry.getKey() == null) {
				continue;
			}
			if (prior) {
				callMaker.append(", ");
			}
			callMaker.append(entry.getKey()).append("=").append(PyUtils.determineStringType(entry.getValue()));
			prior = true;
		}
	}

	@Override
	public BatchSubmissionResponse submitBatch(List<Map<String, Object>> requests, Map<String, Object> parameters) {
		assertBatchSupported();
		checkSocketStatus();
		String requestsJson = BATCH_GSON.toJson(requests);
		StringBuilder callMaker = new StringBuilder(varName + ".submit_batch(");
		callMaker.append("requests=").append(PyUtils.determineStringType(requestsJson));
		appendKwargs(callMaker, parameters, true);
		callMaker.append(")");
		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		return BatchSubmissionResponse.fromObject(output);
	}

	@Override
	public BatchStatusResponse getBatchStatus(String providerBatchId, Map<String, Object> parameters) {
		assertBatchSupported();
		checkSocketStatus();
		StringBuilder callMaker = new StringBuilder(varName + ".get_batch_status(");
		callMaker.append("provider_batch_id=").append(PyUtils.determineStringType(providerBatchId));
		appendKwargs(callMaker, parameters, true);
		callMaker.append(")");
		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		return BatchStatusResponse.fromObject(output);
	}

	@Override
	public BatchResultsResponse getBatchResults(String providerBatchId, Map<String, Object> parameters) {
		assertBatchSupported();
		checkSocketStatus();
		StringBuilder callMaker = new StringBuilder(varName + ".get_batch_results(");
		callMaker.append("provider_batch_id=").append(PyUtils.determineStringType(providerBatchId));
		appendKwargs(callMaker, parameters, true);
		callMaker.append(")");
		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		return BatchResultsResponse.fromObject(output);
	}

	@Override
	public BatchListResponse listBatches(Map<String, Object> parameters) {
		assertBatchSupported();
		checkSocketStatus();
		StringBuilder callMaker = new StringBuilder(varName + ".list_batches(");
		appendKwargs(callMaker, parameters, false);
		callMaker.append(")");
		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		return BatchListResponse.fromObject(output);
	}

	@Override
	public BatchStatusResponse cancelBatch(String providerBatchId, Map<String, Object> parameters) {
		assertBatchSupported();
		checkSocketStatus();
		StringBuilder callMaker = new StringBuilder(varName + ".cancel_batch(");
		callMaker.append("provider_batch_id=").append(PyUtils.determineStringType(providerBatchId));
		appendKwargs(callMaker, parameters, true);
		callMaker.append(")");
		Object output = pyTranslator.runDirectPyNoCancelTrace(callMaker.toString());
		return BatchStatusResponse.fromObject(output);
	}

	@Override
	public void close() throws IOException {
		if (this.cpw != null) {
			this.cpw.shutdown(true);
		}
	}

	/**
	 * 
	 */
	private void createCacheFolder() {
		String engineId = this.getEngineId();

		if (engineId == null || engineId.isEmpty()) {
			engineId = "";
		}
		// create a generic folder
		this.workingDirectory = "MODEL_" + engineId + "_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);

		// make the folder if one does not exist
		if (!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}

}
