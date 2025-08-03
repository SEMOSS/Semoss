package prerna.reactor.interceptor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.message.MapMessage;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.logging.CustomLogger;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class LoggingOutputReactor extends AbstractReactor implements IOutputReactor {

	private static final CustomLogger customLogger = CustomLogger.getLogger(LoggingOutputReactor.class);

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
			
			//String reactorSpanId = (String) arguments.get(PipelineReactorUtils.REACTOR_SPAN_ID);
	        String reactorName = (String) arguments.get(PipelineReactorUtils.OUTPUT_REACTOR_NAME);
	        
	        //mapMessage.put(Constants.AUDIT_LOG_REACTOR_SPAN_ID, reactorSpanId);
	        mapMessage.put(Constants.AUDIT_LOG_OUTPUT_REACTOR_NAME, reactorName);

			mapMessage.put(Constants.AUDIT_LOG_SESSION_ID, ThreadStore.getSessionId());
			mapMessage.put(Constants.AUDIT_LOG_INSIGHT_ID, ThreadStore.getInsightId());

			IEngine engine = (IEngine) arguments.get(PipelineReactorUtils.ENGINE);

			mapMessage.put(Constants.AUDIT_LOG_ENGINE_ID, engine.getEngineId());
			mapMessage.put(Constants.AUDIT_LOG_ENGINE_NAME, engine.getEngineName());

			if (engine instanceof IModelEngine) {
				String request = (String) arguments.get("arg0");
				mapMessage = extractedArguments(arguments, mapMessage);
				mapMessage = getIModelEngineResponse(engine, mapMessage, result);
				mapMessage.put(Constants.AUDIT_LOG_REQUEST, request);
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE, String.valueOf(((IModelEngine) engine).getModelType()));
			} else if (engine instanceof IDatabaseEngine) {
				
				//String request = (String)arguments.get("arg0");
	        	 mapMessage.put(Constants.AUDIT_LOG_RESPONSE, String.valueOf(result) );
	        	 
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IDatabaseEngine) engine).getDatabaseType()));
			} else if (engine instanceof IStorageEngine) {
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IStorageEngine) engine).getStorageType()));
			} else if (engine instanceof IVectorDatabaseEngine) {
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,
						String.valueOf(((IVectorDatabaseEngine) engine).getVectorDatabaseType()));
			} else if (engine instanceof IVenvEngine) {
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE, String.valueOf(((IVenvEngine) engine).getVenvType()));
			} else if (engine instanceof IProject) {
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE, String.valueOf(((IProject) engine).getProjectType()));
			} /*
				 * else if(engine instanceof IReactorFunctionEngine) {
				 * mapMessage.put("engineType",String.valueOf(((IFunctionEngine) engine).getF));
				 * }
				 */

			String logLevel = "INFO";
			if (arguments.containsKey("logLevel")) {
				logLevel = arguments.get("logLevel").toString();
			}

			String logMessage = "Executing method: " + methodName;
			if (this.getNounStore().getNoun("logMessage") != null) {
				logMessage = this.getNounStore().getNoun("logMessage").get(0).toString();
			}

			mapMessage.put(Constants.AUDIT_LOG_METHOD_NAME, methodName);
			mapMessage.put(Constants.AUDIT_LOG_LEVEL, logLevel);
			mapMessage.put(Constants.AUDIT_LOG_MESSAGE, logMessage);
		}
		LocalDateTime dateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
				.toLocalDateTime();
		String dateTimeStr = dateTime.toString();
        mapMessage.put("responseTimestamp", dateTimeStr);
        
        String methodSpanId  = (String) arguments.get(PipelineReactorUtils.METHOD_SPAN_ID);
        mapMessage.put(Constants.AUDIT_LOG_METHOD_SPAN_ID, methodSpanId);
        
		customLogger.info(mapMessage);
		
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

			mapMessage.put(Constants.AUDIT_LOG_ROOM_ID, response.getRoomId());
			mapMessage.put(Constants.AUDIT_LOG_MESSAGE_ID, response.getMessageId());
			mapMessage.put(Constants.AUDIT_LOG_MESSAGE_TYPE, response.getMessageType());
			mapMessage.put(Constants.AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT, String.valueOf(response.getNumberOfTokensInPrompt()));
			mapMessage.put(Constants.AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE, String.valueOf(response.getNumberOfTokensInResponse()));
			mapMessage.put(Constants.AUDIT_LOG_RESPONSE, responseString);
		} else if (engine instanceof IModelEngine && result instanceof InstructModelEngineResponse) {
			InstructModelEngineResponse response = (InstructModelEngineResponse) result;
			if (response instanceof List) {
				List<Map<String, String>> responseList = (List<Map<String, String>>) response;
				response.setResponse(responseList);
				mapMessage.put(Constants.AUDIT_LOG_RESPONSE, responseList.toString());
			}
			mapMessage.put(Constants.AUDIT_LOG_ROOM_ID, response.getRoomId());
			mapMessage.put(Constants.AUDIT_LOG_MESSAGE_ID, response.getMessageId());
			mapMessage.put(Constants.AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT, String.valueOf(response.getNumberOfTokensInPrompt()));
			mapMessage.put(Constants.AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE, String.valueOf(response.getNumberOfTokensInResponse()));

		} else if (engine instanceof IModelEngine && result instanceof EmbeddingsModelEngineResponse) {
			EmbeddingsModelEngineResponse response = (EmbeddingsModelEngineResponse) result;
			if (response instanceof List) {
				List<List<Double>> responseList = (List<List<Double>>) response;
				response.setResponse(responseList);
				mapMessage.put(Constants.AUDIT_LOG_RESPONSE, responseList.toString());
				mapMessage.put(Constants.AUDIT_LOG_NUMBER_OF_TOKENS_IN_PROMPT, String.valueOf(response.getNumberOfTokensInPrompt()));
				mapMessage.put(Constants.AUDIT_LOG_NUMBER_OF_TOKENS_IN_RESPONSE, String.valueOf(response.getNumberOfTokensInResponse()));
			}
		}
		return mapMessage;
	}
	
	private MapMessage<?, ?> extractedArguments(Map<String, Object> arguments, MapMessage<?, ?> mapMessage) {
		for(Map.Entry<String,Object> entry : arguments.entrySet()) {
			if(entry.getValue() instanceof Insight) {
				Insight insight = (Insight)entry.getValue();
				mapMessage.put(Constants.AUDIT_LOG_USER_ID, insight.getUserId());
				mapMessage.put(Constants.AUDIT_LOG_PROJECT_ID,insight.getContextProjectId() !=null ? insight.getContextProjectId() : "");
				mapMessage.put(Constants.AUDIT_LOG_PROJECT_NAME,insight.getContextProjectName() !=null ? insight.getContextProjectName() : "");
			}else if(entry.getValue() instanceof Room) {
				Room room = (Room) entry.getValue();
				mapMessage.put(Constants.AUDIT_LOG_ROOM_ID,room.getId());
				mapMessage.put(Constants.AUDIT_LOG_USER_ID, room.getUserId());
				mapMessage.put(Constants.AUDIT_LOG_PROJECT_ID,room.getInsight().getContextProjectId() !=null ? room.getInsight().getContextProjectId() : "");
				mapMessage.put(Constants.AUDIT_LOG_PROJECT_NAME,room.getInsight().getContextProjectName() !=null ? room.getInsight().getContextProjectName() : "");
			}else if(entry.getValue() instanceof List<?>) {
				 mapMessage.put(Constants.AUDIT_LOG_REQUEST,String.valueOf(entry.getValue()));	
			}else if(entry.getValue() instanceof Map<?,?>) {
				 Map<String, Object> map =  (Map<String, Object>) entry.getValue();
				 map.entrySet().stream().forEach((e) -> {
					 mapMessage.put(e.getKey(), String.valueOf(e.getValue()));
				 });
			}
		}
		return mapMessage;
	}

}
