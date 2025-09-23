package prerna.reactor.model;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;

public class SemanticDecisionLayerReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(SemanticDecisionLayerReactor.class);
	
	public SemanticDecisionLayerReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				"decisionType",
				"inputMaps",
				"referenceTopics"
		};
		this.keyRequired = new int[]{1, 1, 1, 1};
	}
		
		@Override 
		public NounMetadata execute() {
			organizeKeys();
			String modelId = this.keyValue.get(this.keysToGet[0]);
			String decisionType = this.keyValue.get(this.keysToGet[1]);
			List<Map<String, String>> inputMaps = getInputMaps();
			List<String> referenceTopics = getReferenceTopics();
			
			handleUserAccess(modelId);
			
	    	
			NounMetadata response = decisionHandler(modelId, decisionType, inputMaps, referenceTopics);
	    	
	    	return response;
		}
		
		protected NounMetadata decisionHandler(String modelId, String decisionType, List<Map<String, String>> inputMaps, List<String> referenceTopics) {
			switch(decisionType) {
				case("simple-string-compare"):
					return handleSimpleStringCompare(modelId, inputMaps, referenceTopics);
				default:
					throw new IllegalArgumentException("Expected decisionType to be one of: ['simple-string-compare']");
			}
		}
		
		protected NounMetadata handleSimpleStringCompare(String modelId, List<Map<String, String>> inputMaps, List<String> referenceTopics) {
		    classLogger.info("Handling Simple String Compare Decision");
		    IModelEngine engine = Utility.getModel(modelId);			
		    List<String> stringsToEmbed = new ArrayList<>();
		    List<Integer> textInputIndices = new ArrayList<>();

		    // Collect all text inputs that need embeddings
		    for (int i = 0; i < inputMaps.size(); i++) {
		        Map<String, String> map = inputMaps.get(i);
		        String type = map.get("type");
		        String input = map.get("input");
		        
		        if("text".equals(type.toLowerCase())) {
		            stringsToEmbed.add(input);
		            textInputIndices.add(i);
		        }
		    }
		    
		    // **NEW: Add reference topics to the embedding request**
		    List<String> allStringsToEmbed = new ArrayList<>(stringsToEmbed);
		    allStringsToEmbed.addAll(referenceTopics);
		    
		    classLogger.info("Generating embeddings for " + stringsToEmbed.size() + " inputs and " + referenceTopics.size() + " reference topics");
		    
		    // Generate embeddings for both inputs and reference topics in one call
		    HashMap<String, Object> paramMap = new HashMap<String, Object>();
		    Object embeddingsResponse = engine.embeddings(allStringsToEmbed, this.insight, paramMap);
		    EmbeddingsModelEngineResponse response = (EmbeddingsModelEngineResponse) embeddingsResponse;
		    
		    List<List<Double>> allEmbeddings = response.getResponse();
		    
		    // **NEW: Split embeddings into input embeddings and topic embeddings**
		    List<List<Double>> inputEmbeddings = allEmbeddings.subList(0, stringsToEmbed.size());
		    List<List<Double>> topicEmbeddings = allEmbeddings.subList(stringsToEmbed.size(), allEmbeddings.size());
		    
		    classLogger.info("Received " + inputEmbeddings.size() + " input embeddings and " + topicEmbeddings.size() + " topic embeddings");
		    
		    // Create enriched input maps (same as before)
		    List<Map<String, Object>> enrichedInputMaps = new ArrayList<>();
		    
		    int embeddingIndex = 0;
		    for (int i = 0; i < inputMaps.size(); i++) {
		        Map<String, String> originalMap = inputMaps.get(i);
		        Map<String, Object> enrichedMap = new HashMap<>();
		        
		        enrichedMap.put("type", originalMap.get("type"));
		        enrichedMap.put("input", originalMap.get("input"));
		        
		        if ("text".equals(originalMap.get("type").toLowerCase()) && embeddingIndex < inputEmbeddings.size()) {
		            enrichedMap.put("embeddings", inputEmbeddings.get(embeddingIndex));
		            embeddingIndex++;
		        }
		        
		        enrichedInputMaps.add(enrichedMap);
		    }
		    
		    // **NEW: Create reference topic embeddings map**
		    Map<String, List<Double>> referenceTopicEmbeddings = new HashMap<>();
		    for (int i = 0; i < referenceTopics.size() && i < topicEmbeddings.size(); i++) {
		        referenceTopicEmbeddings.put(referenceTopics.get(i), topicEmbeddings.get(i));
		    }
		    
		    classLogger.info("Created " + enrichedInputMaps.size() + " enriched input maps with embeddings");
		    classLogger.info("Created reference topic embeddings for " + referenceTopicEmbeddings.size() + " topics");
		    
		    // **UPDATED: Pass reference topic embeddings to Python**
		    String code = createPythonCall(enrichedInputMaps, referenceTopics, referenceTopicEmbeddings);
		    NounMetadata pythonOutput = makePythonCall(code);
		    
		    return pythonOutput;
		}

		// **UPDATED: New method signature to include reference topic embeddings**
		protected String createPythonCall(List<Map<String, Object>> inputMaps, List<String> referenceTopics, Map<String, List<Double>> referenceTopicEmbeddings) {
		    StringBuilder callMaker = new StringBuilder("semantic_router(input_maps=");
		    callMaker.append(PyUtils.determineStringType(inputMaps));
		    
		    callMaker.append(", reference_topics=");
		    callMaker.append(PyUtils.determineStringType(referenceTopics));
		    
		    // **NEW: Add reference topic embeddings parameter**
		    callMaker.append(", reference_topic_embeddings=");
		    callMaker.append(PyUtils.determineStringType(referenceTopicEmbeddings));
		    
		    callMaker.append(")");
		    
		    classLogger.info("Python Call >>> " + callMaker.toString());
		    
		    return callMaker.toString();
		}
		
		protected NounMetadata makePythonCall(String code) {
			PyTranslator pyTranslator = this.insight.getPyTranslator();
			String import_statement = pyTranslator.runScript("from semantic_routing import semantic_router") + "";
			
			Object output = pyTranslator.runScript(code);
			String outputString = output.toString();
			
			classLogger.info("Raw Python output: " + outputString);
			
			// Parse the string response back into proper data structure
			List<Map<String, Object>> parsedResponse = parseSemanticRouterResponse(outputString);
			
			classLogger.info("Parsed response size: " + parsedResponse.size());
			if (!parsedResponse.isEmpty()) {
				classLogger.info("First parsed item: " + parsedResponse.get(0));
			}
			
			NounMetadata execNoun = new NounMetadata(parsedResponse, PixelDataType.CUSTOM_DATA_STRUCTURE);
			return execNoun;
		}
		
		/**
		 * Parses the Python response string into a proper Java data structure
		 * Expected format: [{message=..., scores={...}, decision=...}, ...]
		 */
		protected List<Map<String, Object>> parseSemanticRouterResponse(String responseString) {
		    List<Map<String, Object>> result = new ArrayList<>();
		    
		    try {
		        classLogger.info("Starting to parse response: " + responseString);
		        
		        // Remove outer brackets and split by }, {
		        String cleaned = responseString.trim();
		        if (cleaned.startsWith("[") && cleaned.endsWith("]")) {
		            cleaned = cleaned.substring(1, cleaned.length() - 1);
		        }
		        
		        classLogger.info("Cleaned response (no outer brackets): " + cleaned);
		        
		        // Split the string into individual dictionary entries using Java Map format
		        List<String> dictStrings = splitJavaMapStrings(cleaned);
		        
		        classLogger.info("Found " + dictStrings.size() + " dictionary strings");
		        for (int i = 0; i < dictStrings.size(); i++) {
		            classLogger.info("Dict " + i + ": " + dictStrings.get(i));
		        }
		        
		        for (String dictString : dictStrings) {
		            Map<String, Object> parsedMap = parseJavaMapString(dictString);
		            if (parsedMap != null) {
		                classLogger.info("Successfully parsed dict: " + parsedMap);
		                result.add(parsedMap);
		            } else {
		                classLogger.warn("Failed to parse dict: " + dictString);
		            }
		        }
		        
		    } catch (Exception e) {
		        classLogger.error("Error parsing semantic router response: " + responseString, e);
		        // Return empty list as fallback
		        return new ArrayList<>();
		    }
		    
		    return result;
		}
		
		/**
		 * Splits Java Map strings while respecting nested structures
		 */
		protected List<String> splitJavaMapStrings(String content) {
		    List<String> maps = new ArrayList<>();
		    StringBuilder current = new StringBuilder();
		    int braceLevel = 0;
		    
		    for (int i = 0; i < content.length(); i++) {
		        char c = content.charAt(i);
		        
		        if (c == '{') {
		            braceLevel++;
		        } else if (c == '}') {
		            braceLevel--;
		            if (braceLevel == 0) {
		                current.append(c);
		                maps.add(current.toString().trim());
		                current = new StringBuilder();
		                
		                // Skip any following comma and whitespace
		                while (i + 1 < content.length() && 
		                       (content.charAt(i + 1) == ',' || Character.isWhitespace(content.charAt(i + 1)))) {
		                    i++;
		                }
		                continue;
		            }
		        }
		        
		        current.append(c);
		    }
		    
		    // Add any remaining content
		    if (current.length() > 0) {
		        String remaining = current.toString().trim();
		        if (!remaining.isEmpty()) {
		            maps.add(remaining);
		        }
		    }
		    
		    return maps;
		}
		
		/**
		 * Parses a single Java Map string into a Map<String, Object>
		 * Expected format: {message=..., scores={...}, decision=...}
		 */
		protected Map<String, Object> parseJavaMapString(String mapString) {
		    Map<String, Object> result = new HashMap<>();
		    
		    try {
		        classLogger.info("Parsing map string: " + mapString);
		        
		        // Remove outer braces
		        String content = mapString.trim();
		        if (content.startsWith("{") && content.endsWith("}")) {
		            content = content.substring(1, content.length() - 1);
		        }
		        
		        classLogger.info("Map content (no braces): " + content);
		        
		        // Parse key-value pairs for Java Map format (key=value)
		        String[] pairs = splitJavaKeyValuePairs(content);
		        
		        classLogger.info("Found " + pairs.length + " key-value pairs");
		        
		        for (int i = 0; i < pairs.length; i++) {
		            String pair = pairs[i];
		            classLogger.info("Processing pair " + i + ": " + pair);
		            
		            // Split on '=' (Java Map format)
		            int equalsIndex = findEqualsSeparator(pair);
		            if (equalsIndex > 0) {
		                String key = pair.substring(0, equalsIndex).trim();
		                String value = pair.substring(equalsIndex + 1).trim();
		                
		                classLogger.info("Key: '" + key + "', Value: '" + value + "'");
		                
		                // Parse the value based on its type
		                Object parsedValue = parseJavaValue(value);
		                result.put(key, parsedValue);
		                
		                classLogger.info("Added to result: " + key + " -> " + parsedValue);
		            } else {
		                classLogger.warn("Could not find equals separator in pair: " + pair);
		            }
		        }
		        
		    } catch (Exception e) {
		        classLogger.error("Error parsing Java map string: " + mapString, e);
		        return null;
		    }
		    
		    return result;
		}
		
		/**
		 * Finds the equals sign that separates key from value in Java Map format
		 */
		protected int findEqualsSeparator(String pair) {
		    int braceLevel = 0;
		    
		    for (int i = 0; i < pair.length(); i++) {
		        char c = pair.charAt(i);
		        
		        if (c == '{') {
		            braceLevel++;
		        } else if (c == '}') {
		            braceLevel--;
		        } else if (c == '=' && braceLevel == 0) {
		            return i;
		        }
		    }
		    
		    return -1;
		}
		
		/**
		 * Splits key-value pairs for Java Map format while respecting nested structures
		 */
		protected String[] splitJavaKeyValuePairs(String content) {
		    List<String> pairs = new ArrayList<>();
		    StringBuilder current = new StringBuilder();
		    int braceLevel = 0;
		    
		    for (int i = 0; i < content.length(); i++) {
		        char c = content.charAt(i);
		        
		        if (c == '{') {
		            braceLevel++;
		        } else if (c == '}') {
		            braceLevel--;
		        } else if (c == ',' && braceLevel == 0) {
		            pairs.add(current.toString().trim());
		            current = new StringBuilder();
		            continue;
		        }
		        
		        current.append(c);
		    }
		    
		    if (current.length() > 0) {
		        pairs.add(current.toString().trim());
		    }
		    
		    return pairs.toArray(new String[0]);
		}
		
		/**
		 * Parses a value string from Java Map format into the appropriate Java object
		 */
		protected Object parseJavaValue(String value) {
		    value = value.trim();
		    
		    // Handle nested maps (scores)
		    if (value.startsWith("{") && value.endsWith("}")) {
		        return parseJavaNestedMap(value);
		    }
		    
		    // Handle numbers
		    try {
		        if (value.contains(".")) {
		            return Double.parseDouble(value);
		        } else {
		            return Integer.parseInt(value);
		        }
		    } catch (NumberFormatException e) {
		        // Not a number, continue
		    }
		    
		    // Handle booleans
		    if ("true".equalsIgnoreCase(value)) return true;
		    if ("false".equalsIgnoreCase(value)) return false;
		    if ("null".equalsIgnoreCase(value)) return null;
		    
		    // Default to string (unquoted in Java Map format)
		    return value;
		}
		
		/**
		 * Parses a nested Java Map string like {key1=value1, key2=value2}
		 */
		protected Map<String, Object> parseJavaNestedMap(String mapString) {
		    Map<String, Object> result = new HashMap<>();
		    
		    String content = mapString.substring(1, mapString.length() - 1); // Remove braces
		    String[] pairs = splitJavaKeyValuePairs(content);
		    
		    for (String pair : pairs) {
		        int equalsIndex = findEqualsSeparator(pair);
		        if (equalsIndex > 0) {
		            String key = pair.substring(0, equalsIndex).trim();
		            String value = pair.substring(equalsIndex + 1).trim();
		            
		            result.put(key, parseJavaValue(value));
		        }
		    }
		    
		    return result;
		}
		
		/**
		 * Splits key-value pairs for Python dictionary format while respecting nested structures
		 */
		protected String[] splitPythonKeyValuePairs(String content) {
		    List<String> pairs = new ArrayList<>();
		    StringBuilder current = new StringBuilder();
		    int braceLevel = 0;
		    boolean inQuotes = false;
		    char quoteChar = 0;
		    
		    for (int i = 0; i < content.length(); i++) {
		        char c = content.charAt(i);
		        
		        if (!inQuotes && (c == '\'' || c == '"')) {
		            inQuotes = true;
		            quoteChar = c;
		        } else if (inQuotes && c == quoteChar) {
		            if (i == 0 || content.charAt(i - 1) != '\\') {
		                inQuotes = false;
		            }
		        } else if (!inQuotes) {
		            if (c == '{') {
		                braceLevel++;
		            } else if (c == '}') {
		                braceLevel--;
		            } else if (c == ',' && braceLevel == 0) {
		                pairs.add(current.toString().trim());
		                current = new StringBuilder();
		                continue;
		            }
		        }
		        
		        current.append(c);
		    }
		    
		    if (current.length() > 0) {
		        pairs.add(current.toString().trim());
		    }
		    
		    return pairs.toArray(new String[0]);
		}
		
		/**
		 * Removes quotes from a string (both single and double quotes)
		 */
		protected String removeQuotes(String str) {
		    if (str == null || str.length() < 2) {
		        return str;
		    }
		    
		    if ((str.startsWith("'") && str.endsWith("'")) || 
		        (str.startsWith("\"") && str.endsWith("\""))) {
		        return str.substring(1, str.length() - 1);
		    }
		    
		    return str;
		}
		
		/**
		 * Parses a value string from Python format into the appropriate Java object
		 */
		protected Object parsePythonValue(String value) {
		    value = value.trim();
		    
		    // Handle nested dictionaries (scores)
		    if (value.startsWith("{") && value.endsWith("}")) {
		        return parsePythonNestedDict(value);
		    }
		    
		    // Handle quoted strings
		    if ((value.startsWith("'") && value.endsWith("'")) || 
		        (value.startsWith("\"") && value.endsWith("\""))) {
		        return removeQuotes(value);
		    }
		    
		    // Handle numbers
		    try {
		        if (value.contains(".")) {
		            return Double.parseDouble(value);
		        } else {
		            return Integer.parseInt(value);
		        }
		    } catch (NumberFormatException e) {
		        // Not a number, continue
		    }
		    
		    // Handle booleans (Python format)
		    if ("True".equals(value)) return true;
		    if ("False".equals(value)) return false;
		    if ("None".equals(value)) return null;
		    
		    // Default to string (unquoted)
		    return value;
		}
		
		protected int findColonSeparator(String pair) {
		    int braceLevel = 0;
		    boolean inQuotes = false;
		    char quoteChar = 0;
		    
		    for (int i = 0; i < pair.length(); i++) {
		        char c = pair.charAt(i);
		        
		        // Handle quotes
		        if (!inQuotes && (c == '\'' || c == '"')) {
		            inQuotes = true;
		            quoteChar = c;
		        } else if (inQuotes && c == quoteChar) {
		            // Check if it's not escaped
		            if (i == 0 || pair.charAt(i - 1) != '\\') {
		                inQuotes = false;
		            }
		        } else if (!inQuotes) {
		            // Only count braces when not in quotes
		            if (c == '{') {
		                braceLevel++;
		            } else if (c == '}') {
		                braceLevel--;
		            } else if (c == ':' && braceLevel == 0) {
		                return i;
		            }
		        }
		    }
		    
		    return -1;
		}
		
		/**
		 * Parses a nested Python dictionary string like {'key1': value1, 'key2': value2}
		 */
		protected Map<String, Object> parsePythonNestedDict(String dictString) {
		    Map<String, Object> result = new HashMap<>();
		    
		    String content = dictString.substring(1, dictString.length() - 1); // Remove braces
		    String[] pairs = splitPythonKeyValuePairs(content);
		    
		    for (String pair : pairs) {
		        int colonIndex = findColonSeparator(pair);
		        if (colonIndex > 0) {
		            String key = pair.substring(0, colonIndex).trim();
		            String value = pair.substring(colonIndex + 1).trim();
		            
		            key = removeQuotes(key);
		            result.put(key, parsePythonValue(value));
		        }
		    }
		    
		    return result;
		}
		
		protected void handleUserAccess(String modelId) throws IllegalArgumentException {
			User user = this.insight.getUser();
			String userId = user.getPrimaryLoginToken().getId();
			
	    	if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
	    		throw new IllegalArgumentException(
	    				"Model " + modelId + " does not exist or user does not have access to this model");
	    	}
		}
		
		/*
		EX:	[
				{"type": "text", "input": "I was charged twice on my last invoice"},
				{"type": "text", "input": "The website crashes when I upload a PDF"},
				{"type": "text", "input": "I can't log in — password reset isn't working"},
			]
		 */
		private List<Map<String, String>> getInputMaps() {
		    List<Map<String, String>> inputMaps = new ArrayList<>();
		    
		    GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		    if (grs != null && !grs.isEmpty()) {
		        int size = grs.size();
		        for (int i = 0; i < size; i++) {
		            Object mapObj = grs.get(i);
		            if (mapObj instanceof Map) {
		                inputMaps.add((Map<String, String>) mapObj);
		            } else {
		                throw new IllegalArgumentException("Expected each element in " + this.keysToGet[2] + " to be a Map object");
		            }
		        }
		        return inputMaps;
		    }
		    
		    return inputMaps;
		}
		
		/*
		 ["billing and payment issues", "technical problems or bugs", "account login or security"]
		 */
		private List<String> getReferenceTopics() {
		    List<String> referenceTopics = new ArrayList<>();
		    
		    // Check if added as key
		    GenRowStruct grs = this.store.getNoun(this.keysToGet[3]);
		    if (grs != null && !grs.isEmpty()) {
		        int size = grs.size();
		        for (int i = 0; i < size; i++) {
		            referenceTopics.add(grs.get(i).toString());
		        }
		        return referenceTopics;
		    }
		    
		    // If no key is added, return empty list
		    return referenceTopics;
		}
}