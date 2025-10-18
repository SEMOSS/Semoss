package prerna.reactor.interceptor;

import java.io.File;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.logging.SemossLogUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class LoggingOutputReactor extends AbstractReactor implements IOutputReactor {

	private static final Logger logger = SemossLogUtils.getEngineLevelLogger();

	public LoggingOutputReactor() {
		this.keysToGet = new String[] { PipelineReactorUtils.ARGUMENTS };
	}

	@Override
	public NounMetadata execute() {
		GenRowStruct grs = this.getNounStore().getGenRowStruct(keysToGet[0]);
		Map<String, Object> arguments = (Map<String, Object>) grs.get(0);
		MapMessage<?, ?> mapMessage = new MapMessage();

		java.lang.reflect.Method method = (Method) arguments.get(PipelineReactorUtils.METHOD_NAME);
		String methodName = method.getName();

		Object result = arguments.get(PipelineReactorUtils.RESULT);

		boolean isSuccess = (boolean) arguments.get(PipelineReactorUtils.IS_SUCCESS);
		mapMessage.put(SemossLogUtils.IS_SUCCESS, String.valueOf(isSuccess));

		String reactorName = (String) arguments.get(PipelineReactorUtils.OUTPUT_REACTOR_NAME);
		mapMessage.put(SemossLogUtils.OUTPUT_REACTOR_NAME, reactorName);

		IEngine engine = (IEngine) arguments.get(PipelineReactorUtils.ENGINE);
		mapMessage.put(SemossLogUtils.ENGINE_ID, engine.getEngineId());
		mapMessage.put(SemossLogUtils.ENGINE_NAME, engine.getEngineName());

		if (engine instanceof IModelEngine) {

			mapMessage = extractArguments(arguments, mapMessage);
			if (isSuccess) {
				mapMessage = getIModelEngineResponse(engine, mapMessage, result);
			} else {
				mapMessage.put(SemossLogUtils.RESPONSE,
						result != null ? (String) result : isSuccess ? "Success" : "Failed");
			}
			mapMessage.put(SemossLogUtils.ENGINE_TYPE, String.valueOf(((IModelEngine) engine).getModelType()));

		} else if (engine instanceof IDatabaseEngine) {

			mapMessage.put(SemossLogUtils.RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
			mapMessage = extractArguments(arguments, mapMessage);
			mapMessage.put(SemossLogUtils.ENGINE_TYPE, String.valueOf(((IDatabaseEngine) engine).getDatabaseType()));

		} else if (engine instanceof IStorageEngine) {
			mapMessage = extractResult(result, isSuccess, mapMessage);
			mapMessage.put(SemossLogUtils.ENGINE_TYPE, String.valueOf(((IStorageEngine) engine).getStorageType()));

		} else if (engine instanceof IVectorDatabaseEngine) {

			mapMessage.put(SemossLogUtils.RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
			mapMessage = extractArguments(arguments, mapMessage);
			mapMessage.put(SemossLogUtils.ENGINE_TYPE,
					String.valueOf(((IVectorDatabaseEngine) engine).getVectorDatabaseType()));

		} else if (engine instanceof IVenvEngine) {

			mapMessage.put(SemossLogUtils.ENGINE_TYPE, String.valueOf(((IVenvEngine) engine).getVenvType()));

		} else if (engine instanceof IProject) {

			mapMessage.put(SemossLogUtils.ENGINE_TYPE, String.valueOf(((IProject) engine).getProjectType()));

		} else if (engine instanceof IFunctionEngine) {

			mapMessage.put(SemossLogUtils.RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
			mapMessage = extractArguments(arguments, mapMessage);
			mapMessage.put(SemossLogUtils.ENGINE_TYPE, String.valueOf(((IFunctionEngine) engine).getFunctionName()));

		}

		Map<String, Object> config = (Map<String, Object>) arguments.get(PipelineReactorUtils.CONFIG);
		String logLevel = "INFO";
		if (config.containsKey("logLevel")) {
			logLevel = config.get("logLevel").toString();
		}

		String logMessage = "Executing method: " + methodName;
		if (config.containsKey("logMessage")) {
			logMessage = config.get("logMessage").toString();
		}

		mapMessage.put(SemossLogUtils.METHOD_NAME, methodName);
		mapMessage.put(SemossLogUtils.LEVEL, logLevel);
		mapMessage.put(SemossLogUtils.MESSAGE, logMessage);

		mapMessage.put(SemossLogUtils.RESPONSE_END_TIME,
				ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

		logger.info(mapMessage);

		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(arguments, PixelDataType.MAP);
	}

	/**
	 * 
	 * @param engine
	 * @param mapMessage
	 * @param result
	 * @return
	 */
	private MapMessage<?, ?> getIModelEngineResponse(IEngine engine, MapMessage<?, ?> mapMessage, Object result) {
		if (engine instanceof IModelEngine && result instanceof AskModelEngineResponse) {
			AskModelEngineResponse response = (AskModelEngineResponse) result;
			String responseString = response.getStringResponse();
			response.setResponse(responseString);

			mapMessage.put(SemossLogUtils.ROOM_ID, response.getRoomId());
			mapMessage.put(SemossLogUtils.MESSAGE_ID, response.getMessageId());
			mapMessage.put(SemossLogUtils.MESSAGE_TYPE, response.getMessageType());
			mapMessage.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT,
					String.valueOf(response.getNumberOfTokensInPrompt()));
			mapMessage.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE,
					String.valueOf(response.getNumberOfTokensInResponse()));
			mapMessage.put(SemossLogUtils.RESPONSE, responseString);
		} else if (engine instanceof IModelEngine && result instanceof InstructModelEngineResponse) {
			InstructModelEngineResponse response = (InstructModelEngineResponse) result;
			if (response instanceof List) {
				List<Map<String, String>> responseList = (List<Map<String, String>>) response;
				response.setResponse(responseList);
				mapMessage.put(SemossLogUtils.RESPONSE, responseList.toString());
			}
			mapMessage.put(SemossLogUtils.ROOM_ID, response.getRoomId());
			mapMessage.put(SemossLogUtils.MESSAGE_ID, response.getMessageId());
			mapMessage.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT,
					String.valueOf(response.getNumberOfTokensInPrompt()));
			mapMessage.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE,
					String.valueOf(response.getNumberOfTokensInResponse()));

		} else if (engine instanceof IModelEngine && result instanceof EmbeddingsModelEngineResponse) {
			EmbeddingsModelEngineResponse response = (EmbeddingsModelEngineResponse) result;
			if (response instanceof List) {
				List<List<Double>> responseList = (List<List<Double>>) response;
				response.setResponse(responseList);
				mapMessage.put(SemossLogUtils.RESPONSE, responseList.toString());
				mapMessage.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT,
						String.valueOf(response.getNumberOfTokensInPrompt()));
				mapMessage.put(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE,
						String.valueOf(response.getNumberOfTokensInResponse()));
			}
		}
		return mapMessage;
	}

	/**
	 * 
	 * @param result
	 * @param isSuccess
	 * @param mapMessage
	 * @return
	 */
	private MapMessage<?, ?> extractResult(Object result, boolean isSuccess, MapMessage<?, ?> mapMessage) {
		if (result instanceof String) {
			mapMessage.put(SemossLogUtils.RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
		} else if (result instanceof List) {
			if (checkListType(result, String.class)) {
				List<String> listOfResponse = (List<String>) result;
				String response = listOfResponse.stream().collect(Collectors.joining(","));
				mapMessage.put(SemossLogUtils.RESPONSE, response);
			} else if (checkListType(result, Map.class)) {
				List<Map<String, Object>> listOfResponse = (List<Map<String, Object>>) result;
				String response = listOfResponse.stream().map(Map::toString).collect(Collectors.joining(","));
				mapMessage.put(SemossLogUtils.RESPONSE, response);
			}
		} else if (result instanceof Exception) {
			result = String.valueOf(result);
			mapMessage.put(SemossLogUtils.RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
		} else if (result == null) {
			mapMessage.put(SemossLogUtils.RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
		}
		return mapMessage;
	}

	/**
	 * 
	 * @param arguments
	 * @param mapMessage
	 * @return
	 */
	private MapMessage<?, ?> extractArguments(Map<String, Object> arguments, MapMessage<?, ?> mapMessage) {
		for (Map.Entry<String, Object> entry : arguments.entrySet()) {

			if (entry.getKey().equals("arg0")) {
				if (entry.getValue() instanceof String) {

					String request = (String) arguments.get("arg0");
					mapMessage.put(SemossLogUtils.REQUEST, request);

				} else if (entry.getValue() instanceof File) {

					File fileName = (File) entry.getValue();
					String request = fileName.getName();
					mapMessage.put(SemossLogUtils.REQUEST, request);

				} else if (entry.getValue() instanceof List) {

					if (checkListType(entry.getValue(), String.class)) {

						List<String> listOfRequests = (List<String>) arguments.get("arg0");
						String request = listOfRequests.stream().collect(Collectors.joining(","));
						mapMessage.put(SemossLogUtils.REQUEST, request);

					} else if (checkListType(entry.getValue(), Number.class)) {

						List<? extends Number> listOfRequests = (List<? extends Number>) arguments.get("arg0");
						String request = joinNumbers(listOfRequests);
						mapMessage.put(SemossLogUtils.REQUEST, request);

					} else if (checkListType(entry.getValue(), File.class)) {

						List<File> listOfRequests = (List<File>) arguments.get("arg0");
						String request = listOfRequests.stream().map(File::getName).collect(Collectors.joining(","));
						mapMessage.put(SemossLogUtils.REQUEST, request);

					} else if (checkListType(entry.getValue(), Map.class)) {

						List<Map<String, Object>> listOfRequests = (List<Map<String, Object>>) arguments.get("arg0");
						String result = listOfRequests.stream().map(Map::toString).collect(Collectors.joining(","));
						mapMessage.put(SemossLogUtils.REQUEST, result);
					} else if (entry.getValue() instanceof VectorDatabaseMetadataCSVTable) {

						VectorDatabaseMetadataCSVTable vectorDatabaseMetadataCSVTable = (VectorDatabaseMetadataCSVTable) arguments
								.get("arg0");
						mapMessage.put(SemossLogUtils.REQUEST, String
								.valueOf("Csv file rows count : " + vectorDatabaseMetadataCSVTable.getRows().size()));
					}
				} else if (entry.getValue() instanceof Map) {

					Map<String, Object> map = (Map<String, Object>) arguments.get("arg0");
					map.entrySet().stream().forEach((e) -> {

						if (entry.getValue() instanceof Insight) {

							Insight insight = (Insight) entry.getValue();
							mapMessage.put(SemossLogUtils.USER_ID, insight.getUserId());
							mapMessage.put(SemossLogUtils.PROJECT_ID,
									insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
							mapMessage.put(SemossLogUtils.PROJECT_NAME,
									insight.getContextProjectName() != null ? insight.getContextProjectName() : "");

						} else if (entry.getValue() instanceof Map) {

							Map<String, Object> mapObj = (Map<String, Object>) entry.getValue();
							mapObj.entrySet().stream().forEach((ele) -> {
								if (ele.getValue() instanceof Insight) {
									Insight insight = (Insight) ele.getValue();
									mapMessage.put(SemossLogUtils.USER_ID, insight.getUserId());
									mapMessage.put(SemossLogUtils.PROJECT_ID,
											insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
									mapMessage.put(SemossLogUtils.PROJECT_NAME,
											insight.getContextProjectName() != null ? insight.getContextProjectName()
													: "");
								} else {
									mapMessage.put(e.getKey(), String.valueOf(e.getValue()));
								}
							});
							mapMessage.put(SemossLogUtils.REQUEST, mapObj.toString());
						}

					});
				}

			} else if (entry.getValue() instanceof Insight) {

				Insight insight = (Insight) entry.getValue();
				mapMessage.put(SemossLogUtils.USER_ID, insight.getUserId());
				mapMessage.put(SemossLogUtils.PROJECT_ID,
						insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
				mapMessage.put(SemossLogUtils.PROJECT_NAME,
						insight.getContextProjectName() != null ? insight.getContextProjectName() : "");

			} else if (entry.getValue() instanceof Room) {

				Room room = (Room) entry.getValue();
				mapMessage.put(SemossLogUtils.ROOM_ID, room.getId());
				mapMessage.put(SemossLogUtils.USER_ID, room.getUserId());
				mapMessage.put(SemossLogUtils.PROJECT_ID,
						room.getInsight().getContextProjectId() != null ? room.getInsight().getContextProjectId() : "");
				mapMessage.put(SemossLogUtils.PROJECT_NAME,
						room.getInsight().getContextProjectName() != null ? room.getInsight().getContextProjectName()
								: "");

			} else if (entry.getValue() instanceof List) {

				if (checkListType(entry.getValue(), String.class)) {

					List<String> listOfRequests = (List<String>) entry.getValue();
					String request = listOfRequests.stream().collect(Collectors.joining(","));
					mapMessage.put(SemossLogUtils.REQUEST, request);

				} else if (checkListType(entry.getValue(), Number.class)) {

					List<? extends Number> listOfRequests = (List<? extends Number>) entry.getValue();
					String request = joinNumbers(listOfRequests);
					mapMessage.put(SemossLogUtils.REQUEST, request);

				} else if (checkListType(entry.getValue(), File.class)) {

					List<File> listOfRequests = (List<File>) entry.getValue();
					String request = listOfRequests.stream().map(File::getName).collect(Collectors.joining(","));
					mapMessage.put(SemossLogUtils.REQUEST, request);

				} else if (checkListType(entry.getValue(), Map.class)) {

					List<Map<String, Object>> listOfRequests = (List<Map<String, Object>>) entry.getValue();
					listOfRequests.forEach((map) -> {
						map.forEach((key, value) -> {
							mapMessage.put(key, String.valueOf(value));
						});
					});
				}
			} else if (entry.getValue() instanceof Map) {

				Map<String, Object> map = (Map<String, Object>) entry.getValue();
				map.entrySet().stream().forEach((e) -> {

					if (entry.getValue() instanceof Insight) {

						Insight insight = (Insight) entry.getValue();
						mapMessage.put(SemossLogUtils.USER_ID, insight.getUserId());
						mapMessage.put(SemossLogUtils.PROJECT_ID,
								insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
						mapMessage.put(SemossLogUtils.PROJECT_NAME,
								insight.getContextProjectName() != null ? insight.getContextProjectName() : "");

					} else if (entry.getValue() instanceof Map) {

						Map<String, Object> mapObj = (Map<String, Object>) entry.getValue();
						mapObj.entrySet().stream().forEach((ele) -> {
							if (ele.getValue() instanceof Insight) {
								Insight insight = (Insight) ele.getValue();
								mapMessage.put(SemossLogUtils.USER_ID, insight.getUserId());
								mapMessage.put(SemossLogUtils.PROJECT_ID,
										insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
								mapMessage.put(SemossLogUtils.PROJECT_NAME,
										insight.getContextProjectName() != null ? insight.getContextProjectName() : "");
							} else {
								mapMessage.put(e.getKey(), String.valueOf(e.getValue()));
							}
						});
					}

				});
			}
		}
		return mapMessage;
	}

	/**
	 * 
	 * @param obj
	 * @param type
	 * @return
	 */
	public static boolean checkListType(Object obj, Class<?> type) {
		if (!(obj instanceof Collection<?> list)) {
			return false;
		}
		return !list.isEmpty() && list.stream().allMatch(type::isInstance);
	}

	/**
	 * 
	 * @param numbers
	 * @return
	 */
	public static String joinNumbers(Collection<? extends Number> numbers) {
		String result = numbers.stream().map(num -> {
			if (num instanceof Integer) {
				return String.valueOf(num);
			} else if (num instanceof Long) {
				return String.valueOf(num);
			} else if (num instanceof Double) {
				return String.format("%.2f", num);
			} else if (num instanceof Float) {
				return String.format("%.3f", num);
			} else {
				return "Unknown:" + num;
			}
		}).collect(Collectors.joining(","));
		return result;
	}

}