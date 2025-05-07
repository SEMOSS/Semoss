package prerna.engine.impl.model.kserve;

import java.util.List;
import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KServeAdapter {
    /**
     * Converts a model-specific payload to a KServe inference request
     */
	public static JSONObject toKServeRequest(JSONObject modelPayload) {
	    Logger adapterLogger = LogManager.getLogger(KServeAdapter.class);
	    
	    JSONObject kserveRequest = new JSONObject();
	    JSONArray inputs = new JSONArray();
	    
	    adapterLogger.debug("Converting model payload to KServe format: {}", modelPayload.toString(2));
	    
	    for (String key : modelPayload.keySet()) {
	        if (key.equals("model")) continue;
	        
	        JSONObject input = new JSONObject();
	        input.put("name", key);
	        
	        Object value = modelPayload.get(key);
	        if (value instanceof String) {
	            input.put("shape", new int[]{1});
	            input.put("datatype", "BYTES");
	            JSONArray data = new JSONArray();
	            data.put(value);
	            input.put("data", data);
	        } else if (value instanceof List) {
	            List<?> list = (List<?>) value;
	            input.put("shape", new int[]{list.size()});
	            input.put("datatype", "BYTES");
	            
	            JSONArray dataArray = new JSONArray();
	            for (Object item : list) {
	                dataArray.put(item);
	            }
	            input.put("data", dataArray);
	        } else if (value instanceof JSONArray) {
	            JSONArray array = (JSONArray) value;
	            input.put("shape", new int[]{array.length()});
	            input.put("datatype", "BYTES");
	            input.put("data", array);
	        }
	        
	        inputs.put(input);
	    }
	    
	    kserveRequest.put("inputs", inputs);
	    
	    adapterLogger.debug("Final KServe request: {}", kserveRequest.toString(2));
	    
	    return kserveRequest;
	}
    
	/**
	 * Extracts data from KServe response
	 */
	public static JSONObject formatKServeResponse(JSONObject kserveResponse) {
	    Logger adapterLogger = LogManager.getLogger(KServeAdapter.class);
	    JSONObject result = new JSONObject();
	    
	    adapterLogger.debug("Formatting KServe response: {}", kserveResponse.toString(2));
	    
	    if (kserveResponse.has("outputs")) {
	        JSONArray outputs = kserveResponse.getJSONArray("outputs");
	        
	        for (int i = 0; i < outputs.length(); i++) {
	            JSONObject output = outputs.getJSONObject(i);
	            String name = output.getString("name");
	            JSONArray data = output.getJSONArray("data");
	            
	            if (data.length() > 0) {
	                if (data.length() == 1) {
	                    Object dataValue = data.get(0);
	                    
	                    if (dataValue instanceof String) {
	                        String value = (String) dataValue;
	                        
	                        if ((value.startsWith("{") && value.endsWith("}")) || 
	                            (value.startsWith("[") && value.endsWith("]"))) {
	                            try {
	                                if (value.startsWith("{")) {
	                                    result.put(name, new JSONObject(value));
	                                } 
	                                else if (value.startsWith("[")) {
	                                    result.put(name, new JSONArray(value));
	                                }
	                            } catch (Exception e) {
	                                adapterLogger.debug("Could not parse JSON string for field {}: {}", name, e.getMessage());
	                                result.put(name, value);
	                            }
	                        } else {
	                            result.put(name, value);
	                        }
	                    } else {
	                        result.put(name, dataValue);
	                    }
	                } else {
	                    result.put(name, data);
	                }
	            } else {
	                result.put(name, new JSONArray());
	            }
	        }
	    }
	   
	    return result;
	}
}
