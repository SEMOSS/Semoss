package prerna.reactor.interceptor;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.message.MapMessage;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.impl.model.Room;
import prerna.logging.CustomLogger;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class LoggingAskInputReactor extends AbstractReactor implements IInputReactor {

    private static final CustomLogger customLogger = CustomLogger.getLogger(LoggingAskInputReactor.class);

    public LoggingAskInputReactor() {
        this.keysToGet = new String[]{ PipelineReactorUtils.ARGUMENTS, PipelineReactorUtils.CONFIG };
    }

    @Override
    public NounMetadata execute() {

    		// get arguments
		GenRowStruct grs = this.getNounStore().getNoun(keysToGet[0]);
		Map <String, Object> arguments = new HashMap<String, Object>();
		MapMessage<?, ?> mapMessage =new MapMessage();
		if(grs != null && grs.size() > 0)
		{
	    		arguments = (Map<String, Object>) grs.get(0);
	        String methodName = arguments.get(PipelineReactorUtils.METHOD_NAME) + "";
	        
	        Map <String, Object> config = (Map <String, Object>)arguments.get(PipelineReactorUtils.CONFIG);
	        
	       
	        mapMessage.put(Constants.AUDIT_LOG_SESSION_ID, ThreadStore.getSessionId());
	        mapMessage.put(Constants.AUDIT_LOG_INSIGHT_ID, ThreadStore.getInsightId());
	        
	        IEngine engine = (IEngine)arguments.get(PipelineReactorUtils.ENGINE);
	        
	        mapMessage.put(Constants.AUDIT_LOG_ENGINE_ID,engine.getEngineId());
	        mapMessage.put(Constants.AUDIT_LOG_ENGINE_NAME,engine.getEngineName());
	       
	        if(engine instanceof IModelEngine) {
        		String request = (String)arguments.get("arg0");
        		 for(Map.Entry<String,Object> entry : arguments.entrySet()) {
     	        	if(entry.getValue() instanceof Insight) {
     	        		Insight insight = (Insight)entry.getValue();
     	        		mapMessage.put(Constants.AUDIT_LOG_USER_ID, insight.getUserId());
     	        		mapMessage.put(Constants.AUDIT_LOG_PROJECT_ID,insight.getContextProjectId());
     	        		mapMessage.put(Constants.AUDIT_LOG_PROJECT_NAME,insight.getContextProjectName());
     	        	}if(entry.getValue() instanceof Room) {
     	        		Room room = (Room) entry.getValue();
     	        		mapMessage.put(Constants.AUDIT_LOG_ROOM_ID,room.getId());
     	        		mapMessage.put(Constants.AUDIT_LOG_USER_ID, room.getUserId());
     	        		mapMessage.put(Constants.AUDIT_LOG_PROJECT_ID,room.getInsight().getContextProjectId());
     	        		mapMessage.put(Constants.AUDIT_LOG_PROJECT_NAME,room.getInsight().getContextProjectName());
     	        	}
     	        }
        		 mapMessage.put(Constants.AUDIT_LOG_REQUEST, request);
	        	 mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IModelEngine) engine).getModelType()));
	        }else if(engine instanceof IDatabaseEngine) {
	        	 mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IDatabaseEngine) engine).getDatabaseType()));
	        }else if(engine instanceof IStorageEngine) {
	        	mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IStorageEngine) engine).getStorageType()));
	        }else if(engine instanceof IVectorDatabaseEngine) {
	        	mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IVectorDatabaseEngine) engine).getVectorDatabaseType()));
	        }else if(engine instanceof IVenvEngine) {
	        	mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IVenvEngine) engine).getVenvType()));
	        }else if(engine instanceof IProject) {
	        	mapMessage.put(Constants.AUDIT_LOG_ENGINE_TYPE,String.valueOf(((IProject) engine).getProjectType()));
	        }/*else if(engine instanceof IReactorFunctionEngine) {
	        	mapMessage.put("engineType",String.valueOf(((IFunctionEngine) engine).getF));
	        }*/
   		  	
	        
	        // get the param that you want to track
	        String targetParamValue = null;
	        if (config.containsKey(PipelineReactorUtils.TARGET_PARAM))
	        		targetParamValue = (String)arguments.get(PipelineReactorUtils.TARGET_PARAM);

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
	       //logContext.put("config", config);
	       
	       customLogger.info(mapMessage);
		}
		Map <String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
        return new NounMetadata(arguments, PixelDataType.MAP);
    }
}