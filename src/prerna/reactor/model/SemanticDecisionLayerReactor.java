package prerna.reactor.model;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;

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
			
	    	
	    	String response = decisionHandler(modelId, decisionType, inputMaps, referenceTopics);
	    	
	    	return new NounMetadata(response, PixelDataType.CONST_STRING);
		}
		
		protected String decisionHandler(String modelId, String decisionType, List<Map<String, String>> inputMaps, List<String> referenceTopics) {
			switch(decisionType) {
				case("simple-string-compare"):
					return handleSimpleStringCompare(modelId, inputMaps, referenceTopics);
				default:
					throw new IllegalArgumentException("Expected decisionType to be one of: ['simple-string-compare']");
			}
		}
		
		protected String handleSimpleStringCompare(String modelId, List<Map<String, String>> inputMaps, List<String> referenceTopics) {
		    classLogger.info("Handling Simple String Compare Decision");
		    IModelEngine engine = Utility.getModel(modelId);			
		    List<String> stringsToEmbed = new ArrayList<>();
		    List<Integer> textInputIndices = new ArrayList<>();

		    for (int i = 0; i < inputMaps.size(); i++) {
		        Map<String, String> map = inputMaps.get(i);
		        String type = map.get("type");
		        String input = map.get("input");
		        
		        if("text".equals(type.toLowerCase())) {
		            stringsToEmbed.add(input);
		            textInputIndices.add(i);
		        }
		    }
		    
		    HashMap<String, Object> paramMap = new HashMap<String, Object>();
		    Object embeddingsResponse = engine.embeddings(stringsToEmbed, this.insight, paramMap);
		    EmbeddingsModelEngineResponse response = (EmbeddingsModelEngineResponse) embeddingsResponse;
		    
		    List<List<Double>> embeddings = response.getResponse();
		    
		    List<Map<String, Object>> enrichedInputMaps = new ArrayList<>();
		    
		    int embeddingIndex = 0;
		    for (int i = 0; i < inputMaps.size(); i++) {
		        Map<String, String> originalMap = inputMaps.get(i);
		        Map<String, Object> enrichedMap = new HashMap<>();
		        
		        enrichedMap.put("type", originalMap.get("type"));
		        enrichedMap.put("input", originalMap.get("input"));
		        
		        if ("text".equals(originalMap.get("type").toLowerCase()) && embeddingIndex < embeddings.size()) {
		            enrichedMap.put("embeddings", embeddings.get(embeddingIndex));
		            embeddingIndex++;
		        }
		        
		        enrichedInputMaps.add(enrichedMap);
		    }
		    
		    classLogger.info("Created " + enrichedInputMaps.size() + " enriched input maps with embeddings");
		    
		    String code = createPythonCall(enrichedInputMaps, referenceTopics);
		    String pythonOutput = makePythonCall(code);
		    
		    return "Test";
		}
		
		protected String createPythonCall(List<Map<String, Object>> inputMaps, List<String> referenceTopics) {
			StringBuilder callMaker = new StringBuilder("semantic_router()");
			classLogger.info("Python Call >>> " + callMaker.toString());
			
			return callMaker.toString();
		}
		
		protected String makePythonCall(String code) {
			PyTranslator pyTranslator = this.insight.getPyTranslator();
			String import_statement = pyTranslator.runScript("from semantic_routing import semantic_router") + "";
			
			String output = pyTranslator.runScript(code) + "";
			NounMetadata execNoun = new NounMetadata(output, PixelDataType.CONST_STRING);
			List<NounMetadata> outputs = new ArrayList<>(2);
			outputs.add(execNoun);
			NounMetadata temp_output = new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
			return "TEST";
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
				{"type": "text", "input": "I can’t log in — password reset isn’t working"},
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
		 [“billing and payment issues”, “technical problems or bugs”, “account login or security”]
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
