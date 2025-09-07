package prerna.reactor.interceptor;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class LoggingOutputReactor extends AbstractReactor implements IOutputReactor {

	private static final Logger logger = SemossLogUtils.getEngineLevelLogger();

	public LoggingOutputReactor() {
		this.keysToGet = new String[] { PipelineReactorUtils.ARGUMENTS, PipelineReactorUtils.CONFIG };
	}

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		GenRowStruct grs = this.getNounStore().getNoun(keysToGet[0]);
		Map<String, Object> arguments = new HashMap<String, Object>();
		MapMessage<?, ?> mapMessage = new MapMessage();
		if (grs != null && grs.size() > 0) {
			arguments = (Map<String, Object>) grs.get(0);
			String methodName = arguments.get(PipelineReactorUtils.METHOD_NAME) + "";

			Map<String, Object> config = (Map<String, Object>) arguments.get(PipelineReactorUtils.CONFIG);
			Object result = arguments.get(PipelineReactorUtils.RESULT);
			boolean isSuccess = (boolean) arguments.get(PipelineReactorUtils.IS_SUCCESS);
			mapMessage.put(SemossLogUtils.AUDIT_LOG_IS_SUCCESS, String.valueOf(isSuccess));
			// String reactorSpanId = (String)
			// arguments.get(PipelineReactorUtils.REACTOR_SPAN_ID);
			String reactorName = (String) arguments.get(PipelineReactorUtils.OUTPUT_REACTOR_NAME);

			// mapMessage.put(SemossLogUtils.AUDIT_LOG_REACTOR_SPAN_ID, reactorSpanId);
			mapMessage.put(SemossLogUtils.AUDIT_LOG_OUTPUT_REACTOR_NAME, reactorName);

			mapMessage.put(SemossLogUtils.AUDIT_LOG_SESSION_ID, ThreadStore.getSessionId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_INSIGHT_ID, ThreadStore.getInsightId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, ThreadStore.getUser().getPrimaryLoginToken().getId());

			IEngine engine = (IEngine) arguments.get(PipelineReactorUtils.ENGINE);

			mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_ID, engine.getEngineId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_NAME, engine.getEngineName());

			if (engine instanceof IModelEngine) {

				mapMessage = extractArguments(arguments, mapMessage);
				if (isSuccess) {
					mapMessage = getIModelEngineResponse(engine, mapMessage, result);
				} else {
					mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
							result != null ? (String) result : isSuccess ? "Success" : "Failed");
				}
				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IModelEngine) engine).getModelType()));

			} else if (engine instanceof IDatabaseEngine) {

				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
						result != null ? (String) result : isSuccess ? "Success" : "Failed");
				mapMessage = extractArguments(arguments, mapMessage);
				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IDatabaseEngine) engine).getDatabaseType()));

			} else if (engine instanceof IStorageEngine) {
				mapMessage = extractResult(result, isSuccess, mapMessage);
				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IStorageEngine) engine).getStorageType()));

			} else if (engine instanceof IVectorDatabaseEngine) {

				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
						result != null ? (String) result : isSuccess ? "Success" : "Failed");
				mapMessage = extractArguments(arguments, mapMessage);
				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IVectorDatabaseEngine) engine).getVectorDatabaseType()));

			} else if (engine instanceof IVenvEngine) {

				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IVenvEngine) engine).getVenvType()));

			} else if (engine instanceof IProject) {

				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IProject) engine).getProjectType()));

			} else if (engine instanceof IFunctionEngine) {

				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
						result != null ? (String) result : isSuccess ? "Success" : "Failed");
				mapMessage = extractArguments(arguments, mapMessage);
				mapMessage.put(SemossLogUtils.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IFunctionEngine) engine).getFunctionName()));

			} /*
				 * else if(engine instanceof IReactorFunctionEngine) {
				 * mapMessage.put("engineType",String.valueOf(((IFunctionEngine) engine).getF));
				 * }
				 */

			String logLevel = "INFO";
			if (arguments.containsKey("logLevel")) {
				logLevel = isSuccess ? arguments.get("logLevel").toString() : "ERROR";
			}

			String logMessage = "Executing method: " + methodName;
			if (this.getNounStore().getNoun("logMessage") != null) {
				logMessage = this.getNounStore().getNoun("logMessage").get(0).toString();
			}

			mapMessage.put(SemossLogUtils.AUDIT_LOG_METHOD_NAME, methodName);
			mapMessage.put(SemossLogUtils.AUDIT_LOG_LEVEL, logLevel);
			mapMessage.put(SemossLogUtils.AUDIT_LOG_MESSAGE, logMessage);
		}
		LocalDateTime dateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
				.toLocalDateTime();
		String dateTimeStr = dateTime.toString();
		mapMessage.put("responseTimestamp", dateTimeStr);

		String methodSpanId = (String) arguments.get(PipelineReactorUtils.METHOD_SPAN_ID);
		mapMessage.put(SemossLogUtils.AUDIT_LOG_METHOD_SPAN_ID, methodSpanId);

		logger.info(mapMessage);

		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(arguments, PixelDataType.MAP);
	}

	private MapMessage<?, ?> getIModelEngineResponse(IEngine engine, MapMessage<?, ?> mapMessage, Object result) {
		if (engine instanceof IModelEngine && result instanceof AskModelEngineResponse) {
			AskModelEngineResponse response = (AskModelEngineResponse) result;
			String responseString = response.getStringResponse();
			response.setResponse(responseString);

			mapMessage.put(SemossLogUtils.AUDIT_LOG_ROOM_ID, response.getRoomId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_MESSAGE_ID, response.getMessageId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_MESSAGE_TYPE, response.getMessageType());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT,
					String.valueOf(response.getNumberOfTokensInPrompt()));
			mapMessage.put(SemossLogUtils.AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE,
					String.valueOf(response.getNumberOfTokensInResponse()));
			mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE, responseString);
		} else if (engine instanceof IModelEngine && result instanceof InstructModelEngineResponse) {
			InstructModelEngineResponse response = (InstructModelEngineResponse) result;
			if (response instanceof List) {
				List<Map<String, String>> responseList = (List<Map<String, String>>) response;
				response.setResponse(responseList);
				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE, responseList.toString());
			}
			mapMessage.put(SemossLogUtils.AUDIT_LOG_ROOM_ID, response.getRoomId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_MESSAGE_ID, response.getMessageId());
			mapMessage.put(SemossLogUtils.AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT,
					String.valueOf(response.getNumberOfTokensInPrompt()));
			mapMessage.put(SemossLogUtils.AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE,
					String.valueOf(response.getNumberOfTokensInResponse()));

		} else if (engine instanceof IModelEngine && result instanceof EmbeddingsModelEngineResponse) {
			EmbeddingsModelEngineResponse response = (EmbeddingsModelEngineResponse) result;
			if (response instanceof List) {
				List<List<Double>> responseList = (List<List<Double>>) response;
				response.setResponse(responseList);
				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE, responseList.toString());
				mapMessage.put(SemossLogUtils.AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT,
						String.valueOf(response.getNumberOfTokensInPrompt()));
				mapMessage.put(SemossLogUtils.AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE,
						String.valueOf(response.getNumberOfTokensInResponse()));
			}
		}
		return mapMessage;
	}

	private MapMessage<?, ?> extractResult(Object result, boolean isSuccess, MapMessage<?, ?> mapMessage) {
		if (result instanceof String) {
			mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
		} else if (result instanceof List) {
			if (checkListType(result, String.class)) {
				List<String> listOfResponse = (List<String>) result;
				String response = listOfResponse.stream().collect(Collectors.joining(","));
				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE, response);
			} else if (checkListType(result, Map.class)) {
				List<Map<String, Object>> listOfResponse = (List<Map<String, Object>>) result;
				String response = listOfResponse.stream().map(Map::toString).collect(Collectors.joining(","));
				mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE, response);
			}
		} else if (result instanceof Exception) {
			result = String.valueOf(result);
			mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
		} else if (result == null) {
			mapMessage.put(SemossLogUtils.AUDIT_LOG_RESPONSE,
					result != null ? (String) result : isSuccess ? "Success" : "Failed");
		}
		return mapMessage;
	}

	private MapMessage<?, ?> extractArguments(Map<String, Object> arguments, MapMessage<?, ?> mapMessage) {
		for (Map.Entry<String, Object> entry : arguments.entrySet()) {

			if (entry.getKey().equals("arg0")) {
				if (entry.getValue() instanceof String) {

					String request = (String) arguments.get("arg0");
					mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

				} else if (entry.getValue() instanceof File) {

					File fileName = (File) entry.getValue();
					String request = fileName.getName();
					mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

				} else if (entry.getValue() instanceof List) {

					if (checkListType(entry.getValue(), String.class)) {

						List<String> listOfRequests = (List<String>) arguments.get("arg0");
						String request = listOfRequests.stream().collect(Collectors.joining(","));
						mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

					} else if (checkListType(entry.getValue(), Number.class)) {

						List<? extends Number> listOfRequests = (List<? extends Number>) arguments.get("arg0");
						String request = joinNumbers(listOfRequests);
						mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

					} else if (checkListType(entry.getValue(), File.class)) {

						List<File> listOfRequests = (List<File>) arguments.get("arg0");
						String request = listOfRequests.stream().map(File::getName).collect(Collectors.joining(","));
						mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

					} else if (checkListType(entry.getValue(), Map.class)) {

						List<Map<String, Object>> listOfRequests = (List<Map<String, Object>>) arguments.get("arg0");
						String result = listOfRequests.stream().map(Map::toString).collect(Collectors.joining(","));
						mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, result);
					} else if (entry.getValue() instanceof VectorDatabaseMetadataCSVTable) {

						VectorDatabaseMetadataCSVTable vectorDatabaseMetadataCSVTable = (VectorDatabaseMetadataCSVTable) arguments
								.get("arg0");
						mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, String
								.valueOf("Csv file rows count : " + vectorDatabaseMetadataCSVTable.getRows().size()));
					}
				} else if (entry.getValue() instanceof Map) {

					Map<String, Object> map = (Map<String, Object>) arguments.get("arg0");
					map.entrySet().stream().forEach((e) -> {

						if (entry.getValue() instanceof Insight) {

							Insight insight = (Insight) entry.getValue();
							mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, insight.getUserId());
							mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_ID,
									insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
							mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_NAME,
									insight.getContextProjectName() != null ? insight.getContextProjectName() : "");

						} else if (entry.getValue() instanceof Map) {

							Map<String, Object> mapObj = (Map<String, Object>) entry.getValue();
							mapObj.entrySet().stream().forEach((ele) -> {
								if (ele.getValue() instanceof Insight) {
									Insight insight = (Insight) ele.getValue();
									mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, insight.getUserId());
									mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_ID,
											insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
									mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_NAME,
											insight.getContextProjectName() != null ? insight.getContextProjectName()
													: "");
								} else {
									mapMessage.put(e.getKey(), String.valueOf(e.getValue()));
								}
							});
							mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, mapObj.toString());
						}

					});
				}

			} else if (entry.getValue() instanceof Insight) {

				Insight insight = (Insight) entry.getValue();
				mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, insight.getUserId());
				mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_ID,
						insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
				mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_NAME,
						insight.getContextProjectName() != null ? insight.getContextProjectName() : "");

			} else if (entry.getValue() instanceof Room) {

				Room room = (Room) entry.getValue();
				mapMessage.put(SemossLogUtils.AUDIT_LOG_ROOM_ID, room.getId());
				mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, room.getUserId());
				mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_ID,
						room.getInsight().getContextProjectId() != null ? room.getInsight().getContextProjectId() : "");
				mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_NAME,
						room.getInsight().getContextProjectName() != null ? room.getInsight().getContextProjectName()
								: "");

			} else if (entry.getValue() instanceof List) {

				if (checkListType(entry.getValue(), String.class)) {

					List<String> listOfRequests = (List<String>) entry.getValue();
					String request = listOfRequests.stream().collect(Collectors.joining(","));
					mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

				} else if (checkListType(entry.getValue(), Number.class)) {

					List<? extends Number> listOfRequests = (List<? extends Number>) entry.getValue();
					String request = joinNumbers(listOfRequests);
					mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

				} else if (checkListType(entry.getValue(), File.class)) {

					List<File> listOfRequests = (List<File>) entry.getValue();
					String request = listOfRequests.stream().map(File::getName).collect(Collectors.joining(","));
					mapMessage.put(SemossLogUtils.AUDIT_LOG_REQUEST, request);

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
						mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, insight.getUserId());
						mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_ID,
								insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
						mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_NAME,
								insight.getContextProjectName() != null ? insight.getContextProjectName() : "");

					} else if (entry.getValue() instanceof Map) {

						Map<String, Object> mapObj = (Map<String, Object>) entry.getValue();
						mapObj.entrySet().stream().forEach((ele) -> {
							if (ele.getValue() instanceof Insight) {
								Insight insight = (Insight) ele.getValue();
								mapMessage.put(SemossLogUtils.AUDIT_LOG_USER_ID, insight.getUserId());
								mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_ID,
										insight.getContextProjectId() != null ? insight.getContextProjectId() : "");
								mapMessage.put(SemossLogUtils.AUDIT_LOG_PROJECT_NAME,
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

	public static boolean checkListType(Object obj, Class<?> type) {
		if (!(obj instanceof List<?> list)) {
			return false;
		}
		return !list.isEmpty() && list.stream().allMatch(type::isInstance);
	}

	public static String joinNumbers(List<? extends Number> numbers) {

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
