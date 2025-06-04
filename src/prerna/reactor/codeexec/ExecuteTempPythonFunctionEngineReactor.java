package prerna.reactor.codeexec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.text.StringEscapeUtils;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ExecuteTempPythonFunctionEngineReactor extends AbstractReactor  {

    private static final Logger classLogger = LogManager.getLogger(ExecuteTempPythonFunctionEngineReactor.class);

    public ExecuteTempPythonFunctionEngineReactor() {
        this.keysToGet = new String[]{
        	ReactorKeysEnum.ENGINE.getKey(),		
            ReactorKeysEnum.PAYLOAD.getKey()
        };
    }

	@Override
	public NounMetadata execute() {
		organizeKeys();
	      String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
			if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Function Engine " + engineId + " does not exist or user does not have access to this function");
			}
			String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";
			Properties prop = Utility.loadProperties(smssFile);

			String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
			
	        if (engineName == null) {
	            throw new IllegalArgumentException("Function Engine Name must be provided");
	        }

	        // Directory Path Generation
	        String funtionEnginePath = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.FUNCTION, engineId, engineName);
	        
	        Map<String, Object> payload = getPayload();
	        if (payload == null || payload.isEmpty()) {
	            throw new IllegalArgumentException("Payload must contain a 'content' key with Python code.");
	        }
	        
	        Object contentObj = payload.get("content");

	        if (contentObj == null || !(contentObj instanceof String)) {
	            throw new IllegalArgumentException("Payload must contain a 'content' key with Python code.");
	        }

	        String contentEscaped = (String) contentObj;
	        String content = StringEscapeUtils.unescapeJava(contentEscaped);
	       

	        try {
	            return executeFunctionEngine(funtionEnginePath, content);
	        } catch (Exception e) {
	            classLogger.error("Failed to execute Python Function Engine", e);
	            throw new RuntimeException("Error while executing Python code ");
	        } 
	    }

	private NounMetadata executeFunctionEngine(String funtionEnginePath, String content) throws Exception {
		Path pyFolderPath = getSemossSubfolderPath(funtionEnginePath, Constants.PY_BASE_FOLDER);
		String fileOutputs = UploadUtilities.processPythonFile(funtionEnginePath, content, pyFolderPath.toString());
		return new NounMetadata(fileOutputs, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
	}

	private Path getSemossSubfolderPath(String functionPath, String subfolder) {
		Path path = Paths.get(functionPath);
		Path semossPath = path.getRoot().resolve(path.getName(0)) // workspace
				.resolve(path.getName(1)); // Semoss
		return semossPath.resolve(subfolder);
	}
	
    private Map<String, Object> getPayload() {
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PAYLOAD.getKey());

        if (grs != null && !grs.isEmpty()) {
            Object allValues = grs.getAllValues();

            if (allValues instanceof Map) {
                return (Map<String, Object>) allValues;
            } else if (allValues instanceof Vector) {
                Vector<?> vector = (Vector<?>) allValues;
                
                if (!vector.isEmpty() && vector.get(0) instanceof Map) {
                    // Directly return the first element if it's a Map
                    return (Map<String, Object>) vector.get(0);
                } else {
                    throw new ClassCastException("Expected Map inside Vector, but found: " + vector.get(0).getClass().getName());
                }
            } else {
                throw new ClassCastException("Payload is of unexpected type: " + allValues.getClass().getName());
            }
        }
        throw new NullPointerException("Payload must be defined for the Function Engine");
    }


}



