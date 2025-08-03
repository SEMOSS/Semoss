package prerna.reactor.interceptor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.message.MapMessage;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.logging.CustomLogger;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class PHIOutputReactor extends AbstractReactor implements IOutputReactor {

	private static final CustomLogger customLogger = CustomLogger.getLogger(PHIOutputReactor.class);
	
	public PHIOutputReactor() {
		this.keysToGet = new String[]{ PipelineReactorUtils.ARGUMENTS, PipelineReactorUtils.CONFIG };
	}

	@Override
	public NounMetadata execute() {
		GenRowStruct grs = this.getNounStore().getNoun(keysToGet[0]);
		Map <String, Object> arguments = new HashMap<String, Object>();
		MapMessage<?, ?> mapMessage = new MapMessage();
		if(grs != null && grs.size() > 0)
		{
			arguments = (Map<String, Object>) grs.get(0);
			String methodName = arguments.get(PipelineReactorUtils.METHOD_NAME) + "";

			Map <String, Object> config = (Map <String, Object>)arguments.get(PipelineReactorUtils.CONFIG);
			
			//String reactorSpanId = (String) arguments.get(PipelineReactorUtils.REACTOR_SPAN_ID);
	        String reactorName = (String) arguments.get(PipelineReactorUtils.OUTPUT_REACTOR_NAME);
	        
	        //mapMessage.put(Constants.AUDIT_LOG_REACTOR_SPAN_ID, reactorSpanId);
	        mapMessage.put(Constants.AUDIT_LOG_OUTPUT_REACTOR_NAME, reactorName);

			mapMessage.put(Constants.AUDIT_LOG_SESSION_ID, ThreadStore.getSessionId());
			mapMessage.put(Constants.AUDIT_LOG_INSIGHT_ID, ThreadStore.getInsightId());

			IEngine engine = (IEngine)arguments.get(PipelineReactorUtils.ENGINE);
			
			mapMessage.put(Constants.AUDIT_LOG_ENGINE_ID, engine.getEngineId());
			mapMessage.put(Constants.AUDIT_LOG_ENGINE_NAME, engine.getEngineName());

			Object result = arguments.get(PipelineReactorUtils.RESULT);

			if (engine instanceof IModelEngine && methodName.equals("ask") && result instanceof AskModelEngineResponse) {
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE, String.valueOf(((IModelEngine) engine).getModelType()));
				String redactionMask = "[REDACTED_PHI]";
				if (arguments.containsKey("redactionMask")) 
				{
					redactionMask = arguments.get("redactionMask").toString();
				}

				AskModelEngineResponse response = (AskModelEngineResponse) result;
				String responseString = response.getStringResponse();
				mapMessage.put(Constants.AUDIT_LOG_RESPONSE, responseString);
				// a very basic example of redaction
				if (responseString.toLowerCase().contains("diagnosis")) {
					response.setResponse(responseString.replaceAll("(?i)diagnosis:.*", "diagnosis: " + redactionMask));
				}
			}
			
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
			
			LocalDateTime dateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
					.toLocalDateTime();
			String dateTimeStr = dateTime.toString();
	        mapMessage.put("responseTimestamp", dateTimeStr);
	        
	        String methodSpanId  = (String) arguments.get(PipelineReactorUtils.METHOD_SPAN_ID);
	        mapMessage.put(Constants.AUDIT_LOG_METHOD_SPAN_ID, methodSpanId);
	        
	        customLogger.info(mapMessage);
		}
		Map <String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(arguments, PixelDataType.MAP);
	}
}