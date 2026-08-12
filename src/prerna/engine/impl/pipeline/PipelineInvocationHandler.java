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
package prerna.engine.impl.pipeline;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.json.JSONArray;
import org.json.JSONObject;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.logging.IgnoreEngineLogging;
import prerna.logging.LoggingEngineSerializer;
import prerna.logging.LoggingIReactorSerializer;
import prerna.logging.LoggingInsightAdapter;
import prerna.logging.LoggingRoomAdapter;
import prerna.logging.LoggingSQLConnectionSerializer;
import prerna.logging.LoggingSQLDataSourceSerializer;
import prerna.logging.LoggingSQLResultSetSerializer;
import prerna.logging.LoggingSQLStatementSerializer;
import prerna.logging.LoggingThrowableSerializer;
import prerna.logging.SemossLogUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.IReactor;
import prerna.reactor.interceptor.IInputReactor;
import prerna.reactor.interceptor.IOutputReactor;
import prerna.reactor.interceptor.PipelineReactorUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.LocalDateTimeAdapter;
import prerna.util.gson.ZoneIdTypeAdapter;
import prerna.util.gson.ZoneOffsetTypeAdapter;
import prerna.util.gson.ZonedDateTimeAdapter;

/**
 * The invocation handler for the dynamic proxy. This class intercepts all
 * method calls, executes the appropriate pipelines, and then calls the real
 * engine method.
 */
public class PipelineInvocationHandler implements InvocationHandler {

	private static final Logger classLogger = LogManager.getLogger(PipelineInvocationHandler.class);
	private static final boolean USE_ENGINE_LOGGER = isEngineLoggerConfigured();

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.registerTypeHierarchyAdapter(IEngine.class, new LoggingEngineSerializer())
			.registerTypeHierarchyAdapter(IReactor.class, new LoggingIReactorSerializer())
			.registerTypeHierarchyAdapter(Connection.class, new LoggingSQLConnectionSerializer())
			.registerTypeHierarchyAdapter(DataSource.class, new LoggingSQLDataSourceSerializer())
			.registerTypeHierarchyAdapter(Statement.class, new LoggingSQLStatementSerializer())
			.registerTypeHierarchyAdapter(ResultSet.class, new LoggingSQLResultSetSerializer())
			.registerTypeHierarchyAdapter(Throwable.class, new LoggingThrowableSerializer())
			.registerTypeAdapter(Room.class, new LoggingRoomAdapter())
			.registerTypeHierarchyAdapter(ZoneId.class, new ZoneIdTypeAdapter())
			.registerTypeAdapter(ZoneOffset.class, new ZoneOffsetTypeAdapter())
			.registerTypeAdapter(Insight.class, new LoggingInsightAdapter())
			.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
			.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeAdapter()).create();

	private final String REQUEST_NOT_TRACKED = "REQUEST NOT TRACKED";
	private final String RESPONSE_NOT_TRACKED = "RESPONSE NOT TRACKED";

	// GUARDRAIL_ACTION audit column values - the notable action a guardrail row
	// took
	private static final String GUARDRAIL_ACTION_MASK = "MASK";
	private static final String GUARDRAIL_ACTION_BLOCK = "BLOCK";
	private static final String GUARDRAIL_ACTION_RESPOND = "RESPOND";

	private final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
	private final Map<String, Pipeline> pipelinesMap = new HashMap<>();

	// Store engine operations as function references instead
	private final MethodInvoker engineInvoker;

	@FunctionalInterface
	private interface MethodInvoker {
		Object invoke(Method method, Object[] args) throws Throwable;
	}

	private final String engineId;
	private final String engineName;
	private final String engineType;
	private final String engineSubType;

	private final Supplier<Logger> getEngineLogger;
	private final Supplier<Boolean> keepInputOutput;

	/**
	 * 
	 * @param realEngine
	 * @param jsonFile
	 */
	public PipelineInvocationHandler(IEngine realEngine, File jsonFile) {
		// Capture realEngine in a closure - not accessible via reflection
		this.engineInvoker = (method, args) -> method.invoke(realEngine, args);
		this.engineId = realEngine.getEngineId();
		this.engineName = realEngine.getEngineName();
		this.engineType = realEngine.getCatalogType().name();
		this.engineSubType = realEngine.getCatalogSubType(realEngine.getSmssProp());

		this.getEngineLogger = () -> realEngine.getEngineLogger("EngineLogger");
		this.keepInputOutput = () -> realEngine.keepInputOutput();

		String pipelineJson = getJsonData(jsonFile);
		parseAndLoadPipelines(pipelineJson);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (method.isAnnotationPresent(IgnoreEngineLogging.class)) {
			try {
				return this.engineInvoker.invoke(method, args);
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
		}

		Logger engineSpecificLogger = this.getEngineLogger.get();
		boolean keepInputOutput = this.keepInputOutput.get();

		String methodName = method.getName();

		// Capture existing MDC values
		Map<String, String> newMdc = new HashMap<>(ThreadContext.getImmutableContext());
		// set a span id to group all input/output guardrails together for this engine
		// method call
		newMdc.put(SemossLogUtils.SPAN_ID, GUID.v7().toUUID().toString());
		// store the method name
		newMdc.put(SemossLogUtils.METHOD_NAME, methodName);
		{
			newMdc.put(SemossLogUtils.ENGINE_ID, this.engineId);
			newMdc.put(SemossLogUtils.ENGINE_NAME, this.engineName);
			newMdc.put(SemossLogUtils.ENGINE_TYPE, this.engineType);
			newMdc.put(SemossLogUtils.ENGINE_SUBTYPE, this.engineSubType);

			String insightId = ThreadStore.getInsightId();
			if (insightId != null) {
				newMdc.put(SemossLogUtils.INSIGHT_ID, insightId);
				Insight insight = InsightStore.getInstance().get(insightId);
				newMdc.put(SemossLogUtils.PROJECT_ID, insight.getContextProjectId());
				newMdc.put(SemossLogUtils.PROJECT_NAME, insight.getContextProjectName());
				newMdc.put(SemossLogUtils.ROOM_ID, insight.getRoomId());
			}
		}

		boolean success = true;

		try (CloseableThreadContext.Instance ctc = CloseableThreadContext.putAll(newMdc)) {
			Object result = null;

			// Find the correct pipeline for the called method
			Pipeline specificPipeline = this.pipelinesMap.get(methodName);
			if (specificPipeline == null) {
				specificPipeline = this.pipelinesMap.get("*");
			}

			List<IInputReactor> inputPipelines = null;
			List<IOutputReactor> outputPipelines = null;
			if (specificPipeline != null) {
				inputPipelines = specificPipeline.getInputPipeline();
				outputPipelines = specificPipeline.getOutputPipeline();
			}
			Map<String, Object> processedArguments = new HashMap<>();
			if (specificPipeline == null || ((inputPipelines == null || inputPipelines.isEmpty())
					&& (outputPipelines == null || outputPipelines.isEmpty()))) {
				// No pipeline defined for this method, so just invoke the real method
				// But wrap for logging purposes
				Instant start = Instant.now();
				try {
					result = this.engineInvoker.invoke(method, args);
					return result;
				} catch (InvocationTargetException e) {
					success = false;
					result = e.getTargetException();
					throw e.getTargetException();
				} finally {
					Instant end = Instant.now();
					processedArguments = mapArguments(null, method, args, processedArguments);
					String request = null;
					String response = null;
					Integer tokensInPrompt = null;
					Integer tokensInResponse = null;
					if (keepInputOutput) {
						request = GSON.toJson(processedArguments);
						response = result == null ? "" : GSON.toJson(result);
					} else {
						request = REQUEST_NOT_TRACKED;
						response = RESPONSE_NOT_TRACKED;
					}
					Integer cacheReadTokens = null;
					Integer cacheCreationTokens = null;
					if (result instanceof AbstractModelEngineResponse) {
						AbstractModelEngineResponse modelResponse = (AbstractModelEngineResponse) result;
						tokensInPrompt = modelResponse.getNumberOfTokensInPrompt();
						tokensInResponse = modelResponse.getNumberOfTokensInResponse();
						cacheReadTokens = modelResponse.getNumberOfCacheReadTokens();
						cacheCreationTokens = modelResponse.getNumberOfCacheCreationTokens();
					}

					logEngineCall(engineSpecificLogger, start, end, success, request, response, null, null,
							tokensInPrompt, tokensInResponse, cacheReadTokens, cacheCreationTokens, null);
				}
			}

			// === INPUT PIPELINE EXECUTION ===
			{
				int size = inputPipelines.size();
				if (size == 0) {
					// need to map the arguments even if no input pipeline
					mapArguments(null, method, args, processedArguments);
				}
				for (int pipelineIndex = 0; pipelineIndex < size; pipelineIndex++) {
					IInputReactor reactor = inputPipelines.get(pipelineIndex);
					// mapArguments will now also add argN parameters and set Insight
					processedArguments = mapArguments(reactor, method, args, processedArguments);

					NounStore inputNouns = new NounStore("input-pipeline");
					GenRowStruct grs = new GenRowStruct();

					// Add core context to processedArguments
					processedArguments.put(PipelineReactorUtils.INPUT_REACTOR_NAME, reactor.getClass().getSimpleName());
					processedArguments.put(PipelineReactorUtils.METHOD_NAME, method);
					processedArguments.put(PipelineReactorUtils.CONFIG,
							specificPipeline.getInputParams().get(pipelineIndex));

					grs.add(new NounMetadata(processedArguments, PixelDataType.MAP));
					inputNouns.addNoun(PipelineReactorUtils.ARGUMENTS, grs);
					reactor.setNounStore(inputNouns);

					// Snapshot the request as this reactor RECEIVES it (only when tracking
					// I/O), before execute() can mutate the shared arguments (e.g. mask arg0).
					// Without this the audit row serializes the post-mask value and the
					// original input is lost.
					String requestSnapshot = keepInputOutput ? GSON.toJson(processedArguments) : null;

					Instant start = Instant.now();
					NounMetadata resultNoun = reactor.execute();
					Instant end = Instant.now();

					// Reactors return a NounMetadata whose value is the
					// updated processedArguments map
					processedArguments = (Map<String, Object>) resultNoun.getValue();

					// Fold any argument rewrites (e.g. PII masking) back into the args
					// array so they (a) survive the next input reactor's mapArguments
					// reseed and (b) are passed to the real engine method below.
					args = unmapArguments(method, processedArguments);

					Map<String, Object> resultMap = (Map<String, Object>) processedArguments
							.get(PipelineReactorUtils.INTERIM_RESULT);
					boolean pass = (boolean) resultMap.get(PipelineReactorUtils.PASS);
					boolean masked = Boolean.TRUE.equals(resultMap.get(PipelineReactorUtils.MASKED));
					String cannedResponse = (String) resultMap.get(PipelineReactorUtils.SHORT_CIRCUIT_RESPONSE);
					// MASK when the guardrail neutralized content, BLOCK when it stopped the
					// request, RESPOND when it supplied the answer itself, null when it ran clean
					// - queryable via the GUARDRAIL_ACTION column
					String guardrailAction = cannedResponse != null ? GUARDRAIL_ACTION_RESPOND
							: masked ? GUARDRAIL_ACTION_MASK : (!pass ? GUARDRAIL_ACTION_BLOCK : null);

					String request = null;
					String response = null;
					if (keepInputOutput || !pass) {
						// requestSnapshot holds the pre-mask input; fall back to the current
						// args only for the block case when I/O tracking is off.
						request = requestSnapshot != null ? requestSnapshot : GSON.toJson(processedArguments);
						response = GSON.toJson(resultMap);
					} else {
						request = REQUEST_NOT_TRACKED;
						response = RESPONSE_NOT_TRACKED;
					}

					logEngineCall(engineSpecificLogger, start, end, pass, request, response,
							reactor.getClass().getSimpleName(), null, null, null, guardrailAction);

					if (cannedResponse != null) {
						if (!AskModelEngineResponse.class.isAssignableFrom(method.getReturnType())) {
							throw new SemossPixelException(
									"Unable to process this request due to content policy (guardrail input exception)");
						}
						classLogger.warn("Guardrail {} short-circuited the model call with a canned response",
								reactor.getClass().getSimpleName());
						return new AskStringModelEngineResponse(cannedResponse, 0, 0);
					}

					if (!pass) {
						throw new SemossPixelException(
								"Unable to process this request due to content policy (guardrail input exception)");
					}
				}
			}

			// Any input-reactor rewrites (e.g. masking) have already been folded back
			// into `args` inside the input loop above, so the real method is invoked
			// with the (possibly masked) arguments.

			// === ACTUAL METHOD EXECUTION ===
			{
				Instant start = Instant.now();
				try {
					result = this.engineInvoker.invoke(method, args);
					processedArguments.put(PipelineReactorUtils.RESULT, result);
				} catch (InvocationTargetException e) {
					success = false;
					result = e.getTargetException();
					throw e.getTargetException();
				} finally {
					Instant end = Instant.now();

					String request = null;
					String response = null;
					Integer tokensInPrompt = null;
					Integer tokensInResponse = null;
					if (keepInputOutput) {
						request = GSON.toJson(processedArguments);
						response = result == null ? "" : GSON.toJson(result);
					} else {
						request = REQUEST_NOT_TRACKED;
						response = RESPONSE_NOT_TRACKED;
					}
					Integer cacheReadTokens = null;
					Integer cacheCreationTokens = null;
					if (result instanceof AbstractModelEngineResponse) {
						AbstractModelEngineResponse modelResponse = (AbstractModelEngineResponse) result;
						tokensInPrompt = modelResponse.getNumberOfTokensInPrompt();
						tokensInResponse = modelResponse.getNumberOfTokensInResponse();
						cacheReadTokens = modelResponse.getNumberOfCacheReadTokens();
						cacheCreationTokens = modelResponse.getNumberOfCacheCreationTokens();
					}

					logEngineCall(engineSpecificLogger, start, end, success, request, response, null, null,
							tokensInPrompt, tokensInResponse, cacheReadTokens, cacheCreationTokens, null);
				}
			}

			// === OUTPUT PIPELINE EXECUTION ===
			{
				int size = outputPipelines.size();
				for (int pipelineIndex = 0; pipelineIndex < size; pipelineIndex++) {
					IOutputReactor reactor = outputPipelines.get(pipelineIndex);
					// mapArguments will now also add argN parameters and set Insight
					// For output reactors, we need to ensure Insight is set if present.
					// We can reuse mapArguments, but it will re-map the original args.
					// It's better to just set Insight directly here if mapArguments is not called.
					for (int argsIndex = 0; argsIndex < args.length; argsIndex++) {
						if (args[argsIndex] instanceof Insight) {
							reactor.setInsight((Insight) args[argsIndex]);
							break;
						}
					}

					NounStore outputNouns = new NounStore("output-pipeline");
					GenRowStruct grs = new GenRowStruct();

					// Add core context to processedArguments for output reactors
					processedArguments.put(PipelineReactorUtils.OUTPUT_REACTOR_NAME,
							reactor.getClass().getSimpleName());
					processedArguments.put(PipelineReactorUtils.METHOD_NAME, method);
					processedArguments.put(PipelineReactorUtils.CONFIG,
							specificPipeline.getOutputParams().get(pipelineIndex));

					grs.add(new NounMetadata(processedArguments, PixelDataType.MAP));
					outputNouns.addNoun(PipelineReactorUtils.ARGUMENTS, grs);
					reactor.setNounStore(outputNouns);

					Instant start = Instant.now();
					NounMetadata resultNoun = reactor.execute();
					Instant end = Instant.now();

					processedArguments = (Map<String, Object>) resultNoun.getValue();

					Map<String, Object> resultMap = (Map<String, Object>) processedArguments
							.get(PipelineReactorUtils.INTERIM_RESULT);
					boolean pass = (boolean) resultMap.get(PipelineReactorUtils.PASS);
					boolean masked = Boolean.TRUE.equals(resultMap.get(PipelineReactorUtils.MASKED));
					String guardrailAction = masked ? GUARDRAIL_ACTION_MASK : (!pass ? GUARDRAIL_ACTION_BLOCK : null);

					String request = null;
					String response = null;
					if (keepInputOutput || !pass) {
						request = GSON.toJson(processedArguments);
						response = GSON.toJson(resultMap);
					} else {
						request = REQUEST_NOT_TRACKED;
						response = RESPONSE_NOT_TRACKED;
					}

					logEngineCall(engineSpecificLogger, start, end, pass, request, response, null,
							reactor.getClass().getSimpleName(), null, null, guardrailAction);

					if (!pass) {
						throw new SemossPixelException(
								"Unable to process this request due to content policy (guardrail output exception)");
					}
				}
			}

			return processedArguments.get(PipelineReactorUtils.RESULT);
		}
	}

	/**
	 * 
	 * @param engineSpecificLogger
	 * @param start
	 * @param end
	 * @param isSuccess
	 * @param inputReactorName
	 * @param outputReactorName
	 */
	private void logEngineCall(Logger engineSpecificLogger, Instant start, Instant end, Boolean isSuccess,
			String request, String response, String inputReactorName, String outputReactorName, Integer tokensInPrompt,
			Integer tokensInResponse, String guardrailAction) {
		logEngineCall(engineSpecificLogger, start, end, isSuccess, request, response, inputReactorName,
				outputReactorName, tokensInPrompt, tokensInResponse, null, null, guardrailAction);
	}

	private void logEngineCall(Logger engineSpecificLogger, Instant start, Instant end, Boolean isSuccess,
			String request, String response, String inputReactorName, String outputReactorName, Integer tokensInPrompt,
			Integer tokensInResponse, Integer cacheReadTokens, Integer cacheCreationTokens, String guardrailAction) {
		Logger logger = null;
		if (engineSpecificLogger != null) {
			logger = engineSpecificLogger;
		} else if (USE_ENGINE_LOGGER) {
			logger = SemossLogUtils.getEngineLevelLogger();
		}
		if (logger != null) {
			Map<String, Object> auditMap = new HashMap<>();
			auditMap.put(SemossLogUtils.REQUEST_START_TIME, start.atZone(UTC_ZONE_ID));
			auditMap.put(SemossLogUtils.RESPONSE_END_TIME, end.atZone(UTC_ZONE_ID));
			auditMap.put(SemossLogUtils.IS_SUCCESS, isSuccess);
			if (inputReactorName != null && !(inputReactorName = inputReactorName.trim()).isEmpty()) {
				auditMap.put(SemossLogUtils.INPUT_REACTOR_NAME, inputReactorName);
			}
			if (outputReactorName != null && !(outputReactorName = outputReactorName.trim()).isEmpty()) {
				auditMap.put(SemossLogUtils.OUTPUT_REACTOR_NAME, outputReactorName);
			}
			if (guardrailAction != null) {
				auditMap.put(SemossLogUtils.GUARDRAIL_ACTION, guardrailAction);
			}
			auditMap.put(SemossLogUtils.REQUEST, request);
			auditMap.put(SemossLogUtils.RESPONSE, response);
			auditMap.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT, tokensInPrompt);
			auditMap.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE, tokensInResponse);
			if (cacheReadTokens != null) {
				auditMap.put(SemossLogUtils.NUMBER_OF_CACHE_READ_TOKENS, cacheReadTokens);
			}
			if (cacheCreationTokens != null) {
				auditMap.put(SemossLogUtils.NUMBER_OF_CACHE_CREATION_TOKENS, cacheCreationTokens);
			}
			logger.info(auditMap);
		}
	}

	/**
	 * Reconstructs the method arguments array from the processed arguments map.
	 * 
	 * @param method The method being invoked.
	 * @param argMap The map containing the processed arguments.
	 * @return An array of arguments.
	 */
	private Object[] unmapArguments(Method method, Map<String, Object> argMap) {
		Parameter[] parameters = method.getParameters();
		Object[] args = new Object[parameters.length];
		for (int i = 0; i < parameters.length; i++) {
			args[i] = argMap.get(parameters[i].getName());
		}
		return args;
	}

	/**
	 * 
	 * @param pipelineFile
	 * @return
	 */
	private static String getJsonData(File pipelineFile) {
		if (pipelineFile == null || !pipelineFile.exists() || !pipelineFile.isFile()) {
			return "";
		}
		String jsonString = null;
		try {
			jsonString = FileUtils.readFileToString(pipelineFile, "UTF-8");
		} catch (IOException e) {
			classLogger.error(
					"Unable to read pipeline configuration file '{}'; pipeline interceptors will not be loaded.",
					pipelineFile.getAbsolutePath(), e);
		}
		return jsonString;
	}

	private static boolean isEngineLoggerConfigured() {
		try {
			LoggerContext context = (LoggerContext) LogManager.getContext(false);
			Configuration config = context.getConfiguration();
			LoggerConfig engineLoggerConfig = config.getLoggerConfig("EngineLogger");
			return engineLoggerConfig != null && "EngineLogger".equals(engineLoggerConfig.getName());
		} catch (Exception e) {
			classLogger.warn(
					"Unable to verify whether '{}' logger is configured. Falling back to engine-specific logger only.",
					"EngineLogger", e);
			return false;
		}
	}

	/**
	 * 
	 * @param pipelineJson
	 */
	private void parseAndLoadPipelines(String pipelineJson) {
		if (pipelineJson == null || pipelineJson.isBlank()) {
			return;
		}

		JSONObject root = new JSONObject(pipelineJson);
		JSONObject pipelines = root.optJSONObject("pipelines");
		if (pipelines == null) {
			// Allow no/invalid pipelines object as an empty interceptor configuration.
			return;
		}

		for (String methodName : pipelines.keySet()) {
			JSONObject pipelineConfig = pipelines.getJSONObject(methodName);
			List<IInputReactor> inputReactors = new ArrayList<>();
			List<IOutputReactor> outputReactors = new ArrayList<>();
			List<Map<String, Object>> inputParams = new ArrayList<>();
			List<Map<String, Object>> outputParams = new ArrayList<>();

			if (pipelineConfig.has("input")) {
				JSONArray inputArray = pipelineConfig.getJSONArray("input");
				for (int i = 0; i < inputArray.length(); i++) {
					JSONObject reactorConfig = inputArray.getJSONObject(i);
					IInputReactor inputReactor = createReactor(reactorConfig, IInputReactor.class);
					Map<String, Object> inputParam = (Map<String, Object>) inputReactor.getNounStore()
							.getGenRowStruct("param").get(0);
					inputReactors.add(inputReactor);
					inputParams.add(inputParam);
				}
			}

			if (pipelineConfig.has("output")) {
				JSONArray outputArray = pipelineConfig.getJSONArray("output");
				for (int i = 0; i < outputArray.length(); i++) {
					JSONObject reactorConfig = outputArray.getJSONObject(i);
					IOutputReactor outputReactor = createReactor(reactorConfig, IOutputReactor.class);
					Map<String, Object> outputParam = (Map<String, Object>) outputReactor.getNounStore()
							.getGenRowStruct("param").get(0);
					outputParams.add(outputParam);
					outputReactors.add(outputReactor);
				}
			}

			this.pipelinesMap.put(methodName, new Pipeline(inputReactors, outputReactors, inputParams, outputParams));
		}
	}

	/**
	 * 
	 * @param <T>
	 * @param config
	 * @param reactorType
	 * @return
	 */
	private <T extends IReactor> T createReactor(JSONObject config, Class<T> reactorType) {
		String className = config.getString("reactorClass");
		try {
			Class<?> clazz = Class.forName(className);
			T reactor = reactorType.cast(clazz.getDeclaredConstructor().newInstance());
			GenRowStruct grs = new GenRowStruct();
			if (config.has("params")) {
				NounStore nounStore = new NounStore("Reactor-params");
				Map<String, Object> paramMap = config.getJSONObject("params").toMap();
				grs.add(new NounMetadata(paramMap, PixelDataType.MAP));
				nounStore.addNoun("param", grs);
				reactor.setNounStore(nounStore);
			}

			return reactor;
		} catch (Exception e) {
			classLogger.error("Failed to create reactor: {}", className, e);
			throw new RuntimeException("Failed to create reactor: " + className, e);
		}
	}

	/**
	 * 
	 * 
	 * @param reactor
	 * @param method
	 * @param args
	 * @param processedArguments
	 * @return
	 */
	private Map<String, Object> mapArguments(IReactor reactor, Method method, Object[] args,
			Map<String, Object> processedArguments) {
		Parameter[] parameters = method.getParameters();
		for (int i = 0; i < parameters.length; i++) {
			if (reactor != null && args[i] instanceof Insight) {
				reactor.setInsight((Insight) args[i]);
			}
			processedArguments.put(parameters[i].getName(), args[i]);
		}
		return processedArguments;
	}

	/*
	 * Helper class to hold the input and output pipelines for a method.
	 */
	private static class Pipeline {

		private final List<IInputReactor> inputPipeline;
		private final List<IOutputReactor> outputPipeline;
		private final List<Map<String, Object>> inputParams;
		private final List<Map<String, Object>> outputParams;

		/**
		 * 
		 * @param inputPipeline
		 * @param outputPipeline
		 * @param inputParams
		 * @param outputParams
		 */
		Pipeline(List<IInputReactor> inputPipeline, List<IOutputReactor> outputPipeline,
				List<Map<String, Object>> inputParams, List<Map<String, Object>> outputParams) {
			this.inputPipeline = inputPipeline;
			this.outputPipeline = outputPipeline;
			this.inputParams = inputParams;
			this.outputParams = outputParams;

		}

		/**
		 * 
		 * @return
		 */
		List<IInputReactor> getInputPipeline() {
			return inputPipeline;
		}

		/**
		 * 
		 * @return
		 */
		List<IOutputReactor> getOutputPipeline() {
			return outputPipeline;
		}

		/**
		 * 
		 * @return
		 */
		List<Map<String, Object>> getInputParams() {
			return this.inputParams;
		}

		/**
		 * 
		 * @return
		 */
		List<Map<String, Object>> getOutputParams() {
			return this.outputParams;
		}
	}

}
