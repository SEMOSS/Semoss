package prerna.engine.impl.pipeline;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import prerna.engine.api.IEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.logging.GsonSerializer;
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
import prerna.util.Constants;

/**
 * The invocation handler for the dynamic proxy. This class intercepts all
 * method calls, executes the appropriate pipelines, and then calls the real
 * engine method.
 */
public class PipelineInvocationHandler implements InvocationHandler {

	private static final Logger classLogger = LogManager.getLogger(PipelineInvocationHandler.class);
	private static boolean USE_ENGINE_LOGGER = false;
	static {
		// Get the logger configuration
		LoggerContext context = (LoggerContext) LogManager.getContext(false);
		Configuration config = context.getConfiguration();

		// Check if EngineLogger is specifically defined
		LoggerConfig engineLoggerConfig = config.getLoggerConfig("EngineLogger");
		USE_ENGINE_LOGGER = engineLoggerConfig.getName().equals("EngineLogger");
	}

	private final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
	private final IEngine realEngine;
	private final Map<String, Pipeline> pipelinesMap = new HashMap<>();

	/**
	 * 
	 * @param realEngine
	 * @param jsonFile
	 */
	public PipelineInvocationHandler(IEngine realEngine, File jsonFile) {
		this.realEngine = realEngine;
		String pipelineJson = getJsonData(jsonFile);
		parseAndLoadPipelines(pipelineJson);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Logger engineSpecificLogger = this.realEngine.getEngineLogger("EngineLogger");

		String methodName = method.getName();

		// Capture existing MDC values
		Map<String, String> newMdc = new HashMap<>(ThreadContext.getImmutableContext());
		// set a span id to group all input/output guardrails together for this engine
		// method call
		newMdc.put(SemossLogUtils.SPAN_ID, GUID.v7().toUUID().toString());
		// store the method name
		newMdc.put(SemossLogUtils.METHOD_NAME, methodName);
		{
			newMdc.put(SemossLogUtils.ENGINE_ID, this.realEngine.getEngineId());
			newMdc.put(SemossLogUtils.ENGINE_NAME, this.realEngine.getEngineName());
			newMdc.put(SemossLogUtils.ENGINE_TYPE, this.realEngine.getCatalogType().name());
			newMdc.put(SemossLogUtils.ENGINE_SUBTYPE, this.realEngine.getCatalogSubType(this.realEngine.getSmssProp()));

			String insightId = ThreadStore.getInsightId();
			if (insightId != null) {
				newMdc.put(SemossLogUtils.INSIGHT_ID, insightId);
				Insight insight = InsightStore.getInstance().get(insightId);
				newMdc.put(SemossLogUtils.PROJECT_ID, insight.getContextProjectId());
				newMdc.put(SemossLogUtils.PROJECT_NAME, insight.getContextProjectName());
			}
		}
		boolean success = true;
		String request = null;
		String response = null;
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
					result = method.invoke(this.realEngine, args);
					return result;
				} catch (InvocationTargetException e) {
					success = false;
					throw e.getTargetException();
				} finally {
					Instant end = Instant.now();
					processedArguments = mapArguments(null, method, args, processedArguments);
					request = convertToGson(processedArguments);
					response = convertGsonResponse(result);
						logEngineCall(engineSpecificLogger, start, end, success, request, response, null,
								null);
					
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
					processedArguments.put(PipelineReactorUtils.ENGINE, realEngine);
					processedArguments.put(PipelineReactorUtils.METHOD_NAME, method);
					processedArguments.put(PipelineReactorUtils.CONFIG,
							specificPipeline.getInputParams().get(pipelineIndex));

					grs.add(new NounMetadata(processedArguments, PixelDataType.MAP));
					inputNouns.addNoun(PipelineReactorUtils.ARGUMENTS, grs);
					reactor.setNounStore(inputNouns);

					Instant start = Instant.now();
					NounMetadata resultNoun = reactor.execute();
					Instant end = Instant.now();

					// Reactors return a NounMetadata whose value is the
					// updated processedArguments map
					processedArguments = (Map<String, Object>) resultNoun.getValue();

					Map<String, Object> resultMap = (Map<String, Object>) processedArguments
							.get(PipelineReactorUtils.INTERIM_RESULT);
					boolean pass = (boolean) resultMap.get(PipelineReactorUtils.PASS);
					request = convertToGson(processedArguments);
					response =  convertToGson(resultMap);
					logEngineCall(engineSpecificLogger, start, end, pass, request, response,reactor.getClass().getSimpleName(), null);

					if (!pass) {
						String msg = null;
						Map<String, Object> guardrailEngineResult = (Map<String, Object>) resultMap.get("guardrailEngineParams");
						if(guardrailEngineResult !=null) {
							msg =	extractGuardRailResultAndMessage(guardrailEngineResult,processedArguments,resultMap);
						}
						logEngineCall(engineSpecificLogger, start, end, pass, request, response,reactor.getClass().getSimpleName(), null);
						throw new SemossPixelException("Input Guardrail issue detected",msg);
					}
				}
			}

			// The unmapArguments method will now correctly use the updated
			// processedArguments
			Object[] finalArgs = unmapArguments(method, processedArguments);

			// === ACTUAL METHOD EXECUTION ===
			{
				Instant start = Instant.now();
				try {
					result = method.invoke(this.realEngine, finalArgs);
					processedArguments.put(PipelineReactorUtils.RESULT, result);
				} catch (InvocationTargetException e) {
					success = false;
					result = e.getTargetException();
					throw e.getTargetException();
				} finally {
					Instant end = Instant.now();
					request = convertToGson(finalArgs);
					response = convertGsonResponse(result);
					//response = extractedResponse(result);
					logEngineCall(engineSpecificLogger, start, end, success, request, response, null, null);
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
					processedArguments.put(PipelineReactorUtils.ENGINE, realEngine);
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
					pipelineIndex++;

					Map<String, Object> resultMap = (Map<String, Object>) processedArguments
							.get(PipelineReactorUtils.INTERIM_RESULT);
					boolean pass = (boolean) resultMap.get(PipelineReactorUtils.PASS);
					//request =  extractRequest(processedArguments);
					request = convertToGson(processedArguments);
					response =  convertToGson(resultMap);
					logEngineCall(engineSpecificLogger, start, end, pass, request, response, null,
							reactor.getClass().getSimpleName());
					if (!pass) {
						    String msg = null;
							Map<String, Object> guardrailEngineResult = (Map<String, Object>) resultMap.get("guardrailEngineParams");
							if(guardrailEngineResult !=null) {
								msg =	extractGuardRailResultAndMessage(guardrailEngineResult,processedArguments,resultMap);
							}
							logEngineCall(engineSpecificLogger, start, end, pass, request, response,reactor.getClass().getSimpleName(), null);
							throw new SemossPixelException("Output Guardrail issue detected",msg);
						}
				}
			}

			return processedArguments.get(PipelineReactorUtils.RESULT);
		}
	}

	private String extractGuardRailResultAndMessage(Map<String, Object> guardrailEngineResult,Map<String, Object> processedArguments,Map<String, Object> resultMap) {
		String guardRailMessage = null;
		
		Map<String, Object> configMap = (Map<String, Object>) processedArguments.get(PipelineReactorUtils.CONFIG);
		String guardrailEngineId = (String) configMap.get("guardrailEngineId");

		String guardRailType = (String) guardrailEngineResult.get(guardrailEngineId);
		
		if (guardRailType != null && guardRailType.equals("GLINER")) {
			guardRailMessage = "GliNer detected entities : ";
		} else if (guardRailType != null && guardRailType.equals("Detoxify")) {
			guardRailMessage = "Toxic prompt detected : " + resultMap.get("returnPrompt") + " Toxcity score is : ";
		}
		
		Map<String, Object> fullDetails = (Map<String, Object>) resultMap.get("fullDetails");
		guardRailMessage = convertToGson(guardRailMessage + fullDetails.get("return"));
		
		return convertToGson(guardRailMessage);
	}

	private String convertGsonResponse(Object result) {
		if (result != null) {
			if (result instanceof AskModelEngineResponse) {
				AskModelEngineResponse response = (AskModelEngineResponse) result;
				return convertToGson(response.getStringResponse());
			} else if (result instanceof InstructModelEngineResponse) {
				InstructModelEngineResponse response = (InstructModelEngineResponse) result;
				List<Map<String, String>> responseList = (List<Map<String, String>>) response;
				return convertToGson(responseList);
				
			} else if (result instanceof EmbeddingsModelEngineResponse) {
				EmbeddingsModelEngineResponse response = (EmbeddingsModelEngineResponse) result;
				List<List<Double>> responseList = (List<List<Double>>) response;
				return convertToGson(responseList);	
			}else {
				convertToGson(result);
			}
		}
		return null;
	}

	public String getGuardRailMessage(Map<String, Object> interim_result, String msg) {

		Map<String, Object> fullDetails = (Map<String, Object>) interim_result.get("fullDetails");
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String json = gson.toJson(msg + fullDetails.get("return"));
		return json;
	}
	
	private String convertToGson(Object obj) {
		String json = GsonSerializer.toJson(obj);	
		return json;
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
			String request, String response, String inputReactorName, String outputReactorName) {
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
			auditMap.put(SemossLogUtils.REQUEST, request);
			auditMap.put(SemossLogUtils.RESPONSE, response);
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
		if (!pipelineFile.exists() || !pipelineFile.isFile()) {
			return "";
		}
		String jsonString = null;
		try {
			jsonString = FileUtils.readFileToString(pipelineFile, "UTF-8");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return jsonString;
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
		JSONObject pipelines = root.getJSONObject("pipelines");

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
					Map<String, Object> inputParam = (Map<String, Object>) inputReactor.getNounStore().getNoun("param")
							.get(0);
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
							.getNoun("param").get(0);
					outputParams.add(outputParam);
					outputReactors.add(outputReactor);
				}
			}

			this.pipelinesMap.put(methodName, new Pipeline(inputReactors, outputReactors, inputParams, outputParams));
		}
	}

	private <T extends IReactor> T createReactor(JSONObject config, Class<T> reactorType) {
		String className = config.getString("reactorClass");
		try {
			Class<?> clazz = Class.forName(className);
			T reactor = reactorType.cast(clazz.newInstance());
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new RuntimeException("Failed to create reactor: " + className, e);
		}
	}

	/**
	 * <<<<<<< HEAD /**
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