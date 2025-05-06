package prerna.reactor.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetPythonFunctionEngineFilesReactor extends AbstractReactor {
	
	public GetPythonFunctionEngineFilesReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
	}
	
    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
            throw new IllegalArgumentException("Function Engine " + engineId + " does not exist or user does not have access to this function");
        }
        
        IFunctionEngine engine = Utility.getFunctionEngine(engineId);
        Map<String, Object> execValue = new HashMap<>();

        if (engine instanceof LocalPythonFunctionEngine) {
            LocalPythonFunctionEngine pythonEngine = (LocalPythonFunctionEngine) engine;
            
            try {
                execValue = pythonEngine.getPythonFilesAndFolders();
            } catch (Exception e) {
                // You can add logging here if needed
                throw new RuntimeException("Error retrieving Python files and folders: " + e.getMessage(), e);
            }
        } else {
            // Handle the case where the engine is not a LocalPythonFunctionEngine
            throw new IllegalArgumentException("This function only works with Python function engines");
        }

        return new NounMetadata(execValue, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
