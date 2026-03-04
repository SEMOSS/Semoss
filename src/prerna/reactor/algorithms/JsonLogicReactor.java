package prerna.reactor.algorithms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;

/**
 * Evaluates JSON Logic rules against provided data using the base Semoss Python implementation.
 * 
 * JSON Logic is a declarative way to express complex rules that can be serialized as JSON.
 * 
 * Usage:
 * JsonLogic(rule=["{\">=\": [{\"var\": \"age\"}, 21]}"], data=["{\"age\": 25}"])
 * Returns: true
 */
public class JsonLogicReactor extends AbstractReactor {
    
    private static final Logger logger = LogManager.getLogger(JsonLogicReactor.class);
    private static final Gson gson = new Gson();
    
    public JsonLogicReactor() {
        this.keysToGet = new String[]{"rule", "data"};
        this.keyRequired = new int[]{1, 0};
    }
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        String ruleJson = Utility.decodeURIComponent(this.keyValue.get("rule"));
        if (ruleJson == null || ruleJson.trim().isEmpty()) {
            throw new SemossPixelException("Rule parameter is required and cannot be empty");
        }
        
        String dataJson = Utility.decodeURIComponent(this.keyValue.get("data"));
        
        try {
            logger.info("Evaluating JSON Logic rule");
            
            Object result = evaluateWithPython(ruleJson, dataJson);
            
            logger.info("JSON Logic evaluation completed successfully");
            
            PixelDataType returnType = determineReturnType(result);
            
            return new NounMetadata(result, returnType);
            
        } catch (com.google.gson.JsonSyntaxException e) {
            logger.error("Invalid JSON syntax in rule or data: {}", e.getMessage());
            throw new SemossPixelException("Invalid JSON syntax: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error evaluating JSON Logic rule", e);
            throw new SemossPixelException("Failed to evaluate JSON Logic rule: " + e.getMessage(), e);
        }
    }
    
    /**
     * Evaluates JSON Logic using the base Semoss Python implementation (utils/json_logic.py)
     * 
     * @param ruleJson The JSON Logic rule as a JSON string
     * @param dataJson The data to evaluate against as a JSON string (can be null)
     * @return The evaluation result
     */
    private Object evaluateWithPython(String ruleJson, String dataJson) {
        try {
            PyTranslator pt = this.insight.getPyTranslator();
            
            // Build Python script to import and call evaluate_json
            StringBuilder script = new StringBuilder();
            script.append("from utils.json_logic import evaluate_json\n");
            script.append("evaluate_json(");
            script.append(PyUtils.determineStringType(ruleJson));
            script.append(", ");
            script.append(dataJson != null && !dataJson.trim().isEmpty() ? PyUtils.determineStringType(dataJson) : "None");
            script.append(")");
            
            // Execute the script and get the result
            Object pyResponse = pt.runScript(script.toString());
            
            if (pyResponse instanceof String) {
                String resultJson = (String) pyResponse;
                // Parse the result JSON back to a Java object
                return gson.fromJson(resultJson, Object.class);
            }
            
            return pyResponse;
            
        } catch (Exception e) {
            logger.error("Error calling Python JSON Logic evaluator", e);
            throw new SemossPixelException("Python evaluation failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Determines the appropriate PixelDataType based on the result object
     */
    private PixelDataType determineReturnType(Object result) {
        if (result == null) {
            return PixelDataType.NULL_VALUE;
        } else if (result instanceof Boolean) {
            return PixelDataType.BOOLEAN;
        } else if (result instanceof Number) {
            return PixelDataType.CONST_DECIMAL;
        } else if (result instanceof String) {
            return PixelDataType.CONST_STRING;
        } else if (result instanceof java.util.Map) {
            return PixelDataType.MAP;
        } else if (result instanceof Iterable) {
            return PixelDataType.VECTOR;
        } else {
            return PixelDataType.CUSTOM_DATA_STRUCTURE;
        }
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals("rule")) {
            return "The JSON Logic rule to evaluate (as a JSON string)";
        } else if (key.equals("data")) {
            return "The data context to evaluate the rule against (as a JSON string, optional)";
        }
        return super.getDescriptionForKey(key);
    }
    
    @Override
    public String getReactorDescription() {
        return "Evaluates JSON Logic rules against provided data. "
                + "JSON Logic provides a declarative way to express complex conditional logic that can be serialized as JSON. "
                + "Supports standard operations (comparisons, arithmetic, logic, arrays) plus Semoss extensions "
                + "(regex matching, fuzzy string comparison, date operations, type casting, and collection helpers).";
    }
}
