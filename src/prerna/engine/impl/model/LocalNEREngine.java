package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;

public class LocalNEREngine extends AbstractPythonModelEngine {
    
    private static final Logger classLogger = LogManager.getLogger(LocalNEREngine.class);
    
    /**
     * Perform named entity recognition on the input text
     * 
     * @param text The input text to process
     * @param entities List of entity types to detect
     * @param maskEntities List of entity types to mask (optional)
     * @param insight The insight context
     * @param parameters Additional parameters (optional)
     * @return NerModelEngineResponse containing the NER results
     */
    public NerModelEngineResponse predict(String text, List<String> entities, List<String> maskEntities, Insight insight, Map<String, Object> parameters) {
        checkSocketStatus();
        
        final String TRIPLE_QUOTE = "\"\"\"";
        
        // Escape the text input
        if (text.startsWith("\"")) {
            text = " " + text;
        }
        if (text.endsWith("\"")) {
            text = text + " ";
        }
        text = text.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
        
        // Build the Python method call
        StringBuilder callMaker = new StringBuilder(varName + ".predict(");
        
        // Add text parameter
        callMaker.append("text=")
                 .append(TRIPLE_QUOTE)
                 .append(text)
                 .append(TRIPLE_QUOTE);
        
        // Add entities parameter
        callMaker.append(",entities=")
                 .append(PyUtils.determineStringType(entities));
        
        // Add mask_entities parameter if provided
        if (maskEntities != null && !maskEntities.isEmpty()) {
            callMaker.append(",mask_entities=")
                     .append(PyUtils.determineStringType(maskEntities));
        }
        
        // Add any additional parameters
        if (parameters != null && !parameters.isEmpty()) {
            Iterator<String> paramKeys = parameters.keySet().iterator();
            while (paramKeys.hasNext()) {
                String key = paramKeys.next();
                Object value = parameters.get(key);
                callMaker.append(",")
                         .append(key)
                         .append("=")
                         .append(PyUtils.determineStringType(value));
            }
        }
        
        // Add prefix if available
        if (this.prefix != null) {
            callMaker.append(", prefix='")
                     .append(prefix)
                     .append("'");
        }
        
        callMaker.append(")");
        
        classLogger.debug("Running NER predict >>> " + callMaker.toString());
        
        try {
            // Execute the Python call
            Object output = pyTranslator.runDirectPy(callMaker.toString());
            
            // Convert the output to NerModelEngineResponse
            NerModelEngineResponse response = NerModelEngineResponse.fromPython(output);
            
            return response;
            
        } catch (Exception e) {
            classLogger.error("Error executing NER predict call", e);
            classLogger.error(Constants.STACKTRACE, e);
            
            // Return error response
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("status", "error");
            errorMap.put("message", e.getMessage());
            
            return new NerModelEngineResponse(errorMap, 0, 0);
        }
    }
    
    @Override
    public ModelTypeEnum getModelType() {
        return ModelTypeEnum.LOCAL_NER;
    }
}