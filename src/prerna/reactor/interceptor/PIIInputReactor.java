package prerna.reactor.interceptor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.logging.CustomLogger;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class PIIInputReactor extends AbstractReactor implements IInputReactor {

	private static final CustomLogger customLogger = CustomLogger.getLogger(PIIInputReactor.class);

	public PIIInputReactor() {
		this.keysToGet = new String[]{ PipelineReactorUtils.ARGUMENTS, PipelineReactorUtils.CONFIG };
	}

	@Override
	public NounMetadata execute() 
	{ 
		GenRowStruct grs = this.getNounStore().getNoun(keysToGet[0]);
		Map <String, Object> arguments = new HashMap<String, Object>();
		if(grs != null && grs.size() > 0)
		{
			arguments = (Map<String, Object>) grs.get(0);
			String methodName = arguments.get(PipelineReactorUtils.METHOD_NAME) + "";

			Map <String, Object> config = (Map <String, Object>)arguments.get(PipelineReactorUtils.CONFIG);
			MapMessage<?, ?> mapMessage =new MapMessage();
			//String reactorSpanId = (String) arguments.get(PipelineReactorUtils.REACTOR_SPAN_ID);
	        String reactorName = (String) arguments.get(PipelineReactorUtils.INPUT_REACTOR_NAME);
	        
	       // mapMessage.put(Constants.AUDIT_LOG_REACTOR_SPAN_ID, reactorSpanId);
	        mapMessage.put(Constants.AUDIT_LOG_INPUT_REACTOR_NAME, reactorName);
	        mapMessage.put(Constants.AUDIT_LOG_SESSION_ID, ThreadStore.getSessionId());
	        mapMessage.put(Constants.AUDIT_LOG_INSIGHT_ID, ThreadStore.getInsightId());
	        
	        IEngine engine = (IEngine)arguments.get(PipelineReactorUtils.ENGINE);
	        mapMessage.put(Constants.AUDIT_LOG_ENGINE_ID,engine.getEngineId());
	        mapMessage.put(Constants.AUDIT_LOG_ENGINE_NAME,engine.getEngineName());
			if (engine instanceof IModelEngine && methodName.equals("ask")) {
				mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IModelEngine) engine).getModelType()));
				boolean blockOnPII = true;
				if (arguments.containsKey("blockOnPII")) {
					blockOnPII = (boolean) arguments.get("blockOnPII");
				}

				List<String> piiTypes = null;
				if (arguments.containsKey("piiTypes")) {
					piiTypes = (List<String>) arguments.get("piiTypes");
				}

				String question = (String) arguments.get("question");
				mapMessage.put(Constants.AUDIT_LOG_REQUEST, question !=null ? question : "");
				if (question != null) {
					if (piiTypes != null) {
						for (String piiType : piiTypes) {
							if (question.toLowerCase().contains(piiType.toLowerCase())) {
								if (blockOnPII) {
									throw new SecurityException("PII type '" + piiType + "' detected in the input. Request blocked.");
								} else {
									customLogger.info("PII type '" + piiType + "' detected in the input.");
								}
							}
						}
					}
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
	       
	        mapMessage.put(Constants.AUDIT_LOG_METHOD_NAME,methodName);
	        mapMessage.put(Constants.AUDIT_LOG_LEVEL,logLevel);
	        mapMessage.put(Constants.AUDIT_LOG_MESSAGE,logMessage);
	        LocalDateTime dateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
					.toLocalDateTime();
			String dateTimeStr = dateTime.toString();
	        mapMessage.put("requestTimestamp", dateTimeStr);
	        
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