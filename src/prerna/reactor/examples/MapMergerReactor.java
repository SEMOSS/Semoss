package prerna.reactor.examples;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor2;
import prerna.reactor.annotations.ReactorKey;
import prerna.reactor.annotations.ReactorOutput;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Simple example demonstrating Map parameters and encoding in AbstractReactor2
 * 
 * This reactor merges two JSON objects and demonstrates:
 * - Map parameters with encoding
 * - Map output
 * - JSON manipulation
 * - Error handling for invalid JSON
 */
@ReactorOutput(
    description = "Merges two JSON objects into a single Map",
    dataType = PixelDataType.MAP
)
public class MapMergerReactor extends AbstractReactor2 {

    @ReactorKey(
        key = "sourceMap",
        description = "First JSON object to merge (will be the base)",
        required = true,
        dataType = PixelDataType.MAP,
        encoded = true
    )
    private Map<String, Object> sourceMap;

    @ReactorKey(
        key = "targetMap", 
        description = "Second JSON object to merge (will override conflicting keys)",
        dataType = PixelDataType.MAP,
        encoded = true,
        defaultValue = "{}"
    )
    private Map<String, Object> targetMap;

    @ReactorKey(
        key = "prefix",
        description = "Optional prefix to add to all keys in the result",
        defaultValue = "",
        dataType = PixelDataType.CONST_STRING
    )
    private String prefix;

    @ReactorKey(
        key = "conflictStrategy",
        description = "How to handle key conflicts: 'override', 'preserve', 'merge'",
        defaultValue = "override",
        dataType = PixelDataType.CONST_STRING
    )
    private String conflictStrategy;

    @Override
    public NounMetadata executeReactor() throws SemossPixelException {
        
        // Validate conflict strategy
        if (!conflictStrategy.matches("override|preserve|merge")) {
            throw new SemossPixelException("Invalid conflictStrategy. Must be 'override', 'preserve', or 'merge'");
        }
        
        // Create result map starting with source
        Map<String, Object> result = new HashMap<>();
        
        // Add all keys from source map with optional prefix
        if (sourceMap != null) {
            for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                String key = prefix.isEmpty() ? entry.getKey() : prefix + entry.getKey();
                result.put(key, entry.getValue());
            }
        }
        
        // Merge target map based on conflict strategy
        if (targetMap != null) {
            for (Map.Entry<String, Object> entry : targetMap.entrySet()) {
                String key = prefix.isEmpty() ? entry.getKey() : prefix + entry.getKey();
                Object newValue = entry.getValue();
                
                if (result.containsKey(key)) {
                    // Handle conflict
                    switch (conflictStrategy) {
                        case "override":
                            result.put(key, newValue);
                            break;
                        case "preserve":
                            // Keep existing value, don't overwrite
                            break;
                        case "merge":
                            // Try to merge if both are maps, otherwise override
                            Object existingValue = result.get(key);
                            if (existingValue instanceof Map && newValue instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> mergedMap = mergeMaps(
                                    (Map<String, Object>) existingValue,
                                    (Map<String, Object>) newValue
                                );
                                result.put(key, mergedMap);
                            } else {
                                result.put(key, newValue);
                            }
                            break;
                    }
                } else {
                    // No conflict, just add
                    result.put(key, newValue);
                }
            }
        }
        
        // Add metadata about the merge operation
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("sourceKeys", sourceMap != null ? sourceMap.keySet().size() : 0);
        metadata.put("targetKeys", targetMap != null ? targetMap.keySet().size() : 0);
        metadata.put("resultKeys", result.size());
        metadata.put("conflictStrategy", conflictStrategy);
        metadata.put("prefixUsed", !prefix.isEmpty());
        
        result.put("_mergeMetadata", metadata);
        
        return new NounMetadata(result, PixelDataType.MAP);
    }
    
    /**
     * Recursively merge two maps
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeMaps(Map<String, Object> map1, Map<String, Object> map2) {
        Map<String, Object> result = new HashMap<>(map1);
        
        for (Map.Entry<String, Object> entry : map2.entrySet()) {
            String key = entry.getKey();
            Object value2 = entry.getValue();
            
            if (result.containsKey(key)) {
                Object value1 = result.get(key);
                if (value1 instanceof Map && value2 instanceof Map) {
                    // Both are maps, merge recursively
                    result.put(key, mergeMaps((Map<String, Object>) value1, (Map<String, Object>) value2));
                } else {
                    // Not both maps, override with value2
                    result.put(key, value2);
                }
            } else {
                result.put(key, value2);
            }
        }
        
        return result;
    }
}

/*
 * USAGE EXAMPLES:
 * 
 * 1. Basic merge with encoded JSON:
 * MapMergerReactor(
 *   sourceMap=[encoded: "{\"name\":\"John\",\"age\":30}"],
 *   targetMap=[encoded: "{\"age\":31,\"city\":\"NYC\"}"]
 * );
 * Result: {"name":"John","age":31,"city":"NYC","_mergeMetadata":{...}}
 * 
 * 2. With conflict preservation:
 * MapMergerReactor(
 *   sourceMap=[encoded: "{\"id\":1,\"status\":\"active\"}"],
 *   targetMap=[encoded: "{\"id\":2,\"name\":\"Test\"}"],
 *   conflictStrategy=["preserve"]
 * );
 * Result: {"id":1,"status":"active","name":"Test","_mergeMetadata":{...}}
 * 
 * 3. With key prefix:
 * MapMergerReactor(
 *   sourceMap=[encoded: "{\"name\":\"John\"}"],
 *   targetMap=[encoded: "{\"name\":\"Jane\"}"],
 *   prefix=["user_"]
 * );
 * Result: {"user_name":"Jane","_mergeMetadata":{...}}
 * 
 * 4. Deep merge of nested objects:
 * MapMergerReactor(
 *   sourceMap=[encoded: "{\"user\":{\"name\":\"John\",\"age\":30}}"],
 *   targetMap=[encoded: "{\"user\":{\"age\":31,\"city\":\"NYC\"}}"],
 *   conflictStrategy=["merge"]
 * );
 * Result: {"user":{"name":"John","age":31,"city":"NYC"},"_mergeMetadata":{...}}
 * 
 * ENCODING WORKFLOW:
 * 
 * 1. Frontend has complex JSON: {"user": {"name": "John", "settings": {"theme": "dark"}}}
 * 2. Gets URL encoded: %7B%22user%22%3A%7B%22name%22%3A%22John%22%2C%22settings%22%3A%7B%22theme%22%3A%22dark%22%7D%7D%7D
 * 3. Sent to reactor with encoded=true flag
 * 4. AbstractReactor2 automatically decodes and parses to Map<String,Object>
 * 5. Your reactor code works with native Java Map objects
 * 
 * This eliminates the need to manually handle encoding/decoding and JSON parsing!
 */