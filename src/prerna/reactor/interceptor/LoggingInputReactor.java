package prerna.reactor.interceptor;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class LoggingInputReactor extends AbstractReactor implements IInputReactor {

    private static final Logger classLogger = LogManager.getLogger(LoggingInputReactor.class);

    public LoggingInputReactor() {
        this.keysToGet = new String[]{ PipelineReactorUtils.ARGUMENTS, PipelineReactorUtils.CONFIG };
    }

    @Override
    public NounMetadata execute() {

    		// get arguments
		GenRowStruct grs = this.getNounStore().getNoun(keysToGet[0]);
		Map <String, Object> arguments = new HashMap<String, Object>();
		if(grs != null && grs.size() > 0)
		{
	    		arguments = (Map<String, Object>) grs.get(0);
	        String methodName = arguments.get(PipelineReactorUtils.METHOD_NAME) + "";
	        
	        Map <String, Object> config = (Map <String, Object>)arguments.get(PipelineReactorUtils.CONFIG);
	        
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
	
	        classLogger.log(org.apache.logging.log4j.Level.toLevel(logLevel), logMessage + " with arguments: " + arguments);
		}
		Map <String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
        return new NounMetadata(arguments, PixelDataType.MAP);
    }
}