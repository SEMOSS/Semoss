//package prerna.engine.impl.model;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.http.entity.ContentType;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import com.google.gson.Gson;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//
//import prerna.engine.api.ModelTypeEnum;
//import prerna.engine.impl.model.TextGenerationInferenceRestEngine.TextGenPayload.GeneratedTextItem;
//import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
//import prerna.engine.impl.model.responses.AskModelEngineResponse;
//import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
//import prerna.engine.impl.model.responses.IModelEngineResponseHandler;
//import prerna.engine.impl.model.responses.IModelEngineResponseStreamHandler;
//import prerna.om.Insight;
//import prerna.util.Constants;
//
//public class TextGenerationInferenceRestEngine extends RESTModelEngine {
//
//	//TODO decide what we want logged
//	private static final Logger classLogger = LogManager.getLogger(TextGenerationInferenceRestEngine.class);
//
//	private String endpoint;
//	private String modelName;
//	private Integer maxTokens;
//	private Integer maxInputTokens;
//	private Map<String, String> headersMap;
//	
//	@Override
//	public void open(Properties smssProp) throws Exception {
//		super.open(smssProp);
//		
//		this.endpoint = this.smssProp.getProperty(ENDPOINT);
//		if(this.endpoint == null || (this.endpoint=this.endpoint.trim()).isEmpty()) {
//			throw new IllegalArgumentException("This model requires a valid value for " + ENDPOINT);
//		}
//		
//		this.modelName = this.smssProp.getProperty(Constants.MODEL);
//		if(this.modelName == null || (this.modelName=this.modelName.trim()).isEmpty()) {
//			throw new IllegalArgumentException("This model requires a valid value for " + Constants.MODEL);
//		}
//		
//		this.headersMap = new HashMap<>();
//	}	
//
//	@Override
//	public AskModelEngineResponse askCall (
//			String question,
//			Object fullPrompt,
//			String context, 
//			Insight insight,
//			Map<String, Object> hyperParameters
//	) {
//		String insightId = insight.getInsightId();
//		
//		Map<String, Object> bodyMap = new HashMap<>();
//		
//		if (fullPrompt != null) {
//			bodyMap.put("inputs", fullPrompt);
//		} else {
//			
//			if(context != null) {
//				bodyMap.put("inputs", context + question);
//			} else {
//				bodyMap.put("inputs", question);
//			}			
//		}
//		
//		boolean stream = Boolean.parseBoolean(hyperParameters.remove("stream") + "");
//		bodyMap.put("stream", stream);
//		
//		bodyMap.put("parameters", this.adjustHyperParameters(hyperParameters));
//
//		IModelEngineResponseHandler modelResponse = postRequestStringBody(this.endpoint, this.headersMap, new Gson().toJson(bodyMap), ContentType.APPLICATION_JSON, 
//				null, null, null, 
//				stream, TextGenPayload.class, insightId);
//		Map<String, Object> modelEngineResponseMap = modelResponse.getModelEngineResponse();
//		
//		return AskModelEngineResponse.fromMap(modelEngineResponseMap);
//	}
//	
//	private Map<String, Object> adjustHyperParameters(Map<String, Object> hyperParameters) {
//		// Check the types of each parameter
//		if (hyperParameters.get("best_of") != null && !(hyperParameters.get("best_of") instanceof Integer)) {
//		    throw new IllegalArgumentException("The hyperparameter best_of is set but is not a integer.");
//		}
//		
//		if (hyperParameters.get("details") != null && !(hyperParameters.get("details") instanceof Boolean)) {
//		    throw new IllegalArgumentException("The hyperparameter details is set but is not a boolean.");
//		}
//		
//		if (hyperParameters.get("decoder_input_details") != null && !(hyperParameters.get("decoder_input_details") instanceof Boolean)) {
//		    throw new IllegalArgumentException("The hyperparameter decoder_input_details is set but is not a boolean.");
//		}
//		
//		if (hyperParameters.get("do_sample") != null && !(hyperParameters.get("do_sample") instanceof Boolean)) {
//		    throw new IllegalArgumentException("The hyperparameter do_sample is set but is not a boolean.");
//		}	
//		
//		if (hyperParameters.get("max_new_tokens") != null && !(hyperParameters.get("max_new_tokens") instanceof Integer)) {
//		    throw new IllegalArgumentException("The hyperparameter max_new_tokens is set but is not a integer.");
//		}
//
//		if (hyperParameters.get("return_full_text") != null && !(hyperParameters.get("return_full_text") instanceof Boolean)) {
//		    throw new IllegalArgumentException("The hyperparameter top_logprobs is set but is not an boolean.");
//		}
//
//		if (hyperParameters.get("seed") != null && !(hyperParameters.get("seed") instanceof Integer)) {
//		    throw new IllegalArgumentException("The hyperparameter do_sample is set but is not a integer.");
//		}
//
//		if (hyperParameters.get("stop") != null) {
//			Object stopListObject = hyperParameters.get("stop");
//			
//			if (stopListObject instanceof List<?>) {
//			    List<?> stopListRaw = (List<?>) stopListObject;
//			    
//			    boolean allStrings = true;
//			    for (Object element : stopListRaw) {
//			        if (!(element instanceof String)) {
//			            allStrings = false;
//			            break;
//			        }
//			    }
//			    
//			    if (!allStrings) {
//			    	throw new IllegalArgumentException("The hyperparameter 'stop' is set but is not a list of strings.");
//			    }
//			} else {
//				throw new IllegalArgumentException("The hyperparameter stop is set but is not a list.");
//			}
//		}
//		
//		if (hyperParameters.get("temperature") != null && !(hyperParameters.get("temperature") instanceof Number)) {
//		    throw new IllegalArgumentException("The hyperparameter temperature is set but is not a number.");
//		}
//		
//		if (hyperParameters.get("top_k") != null && !(hyperParameters.get("top_k") instanceof Integer)) {
//		    throw new IllegalArgumentException("The hyperparameter top_k is set but is not a integer.");
//		}
//
//		if (hyperParameters.get("top_p") != null && !(hyperParameters.get("top_p") instanceof Number)) {
//		    throw new IllegalArgumentException("The hyperparameter top_p is set but is not a number.");
//		}
//		
//		if (hyperParameters.get("typical_p") != null && !(hyperParameters.get("typical_p") instanceof Number)) {
//		    throw new IllegalArgumentException("The hyperparameter typical_p is set but is not a number.");
//		}
//		
//		if (hyperParameters.get("presence_penalty") != null && !(hyperParameters.get("presence_penalty") instanceof Number)) {
//		    throw new IllegalArgumentException("The hyperparameter presence_penalty is set but is not a number.");
//		}
//
//		if (hyperParameters.get("response_format") != null && !(hyperParameters.get("response_format") instanceof Map)) {
//		    throw new IllegalArgumentException("The hyperparameter response_format is set but is not a map.");
//		}
//
//		if (hyperParameters.get("seed") != null && !(hyperParameters.get("seed") instanceof Integer)) {
//		    throw new IllegalArgumentException("The hyperparameter seed is set but is not an integer.");
//		}
//
//		if (hyperParameters.get("stop") != null && !(hyperParameters.get("stop") instanceof String) && !(hyperParameters.get("stop") instanceof List)) {
//		    throw new IllegalArgumentException("The hyperparameter stop is set but is neither a string nor a list.");
//		}
//
//		if (hyperParameters.get("watermark") != null && !(hyperParameters.get("watermark") instanceof Boolean)) {
//		    throw new IllegalArgumentException("The hyperparameter watermark is set but is not an boolean.");
//		}
//
//		return hyperParameters;
//	}
//	
//	@Override
//	public ModelTypeEnum getModelType() {
//		return ModelTypeEnum.TEXT_GENERATION;
//	}
//	
//	/**
//	 * 
//	 * @param responseData
//	 * @param responseType
//	 * @return
//	 */
//	protected IModelEngineResponseHandler handleDeserialization(String responseData, Class<? extends IModelEngineResponseHandler> responseType) {
//        Gson gson = new Gson();
//		JsonArray jsonArray = gson.fromJson(responseData, JsonArray.class);
//        
//        List<GeneratedTextItem> generatedItems = new ArrayList<>();
//        for(JsonElement value : jsonArray) {
//        	generatedItems.add(gson.fromJson(value, GeneratedTextItem.class));
//        }
//        
//        TextGenPayload payload = new TextGenPayload();
//        payload.setGeneratedItems(generatedItems);
//        return payload;
//	}
//
//	public static class TextGenPayload implements IModelEngineResponseHandler {
//		
//		private String response;
//		private List<GeneratedTextItem> generatedItems;
//		
//		public TextGenPayload () {
//			
//		}
//
//		public List<GeneratedTextItem> getGeneratedItems() { return this.generatedItems; }
//		public void setGeneratedItems(List<GeneratedTextItem> generatedItems) { this.generatedItems =  generatedItems; }
//		
//		public class GeneratedTextItem {
//		    private String generated_text;
//		    private GeneratedTextDetails details;
//		    
//		    public String getGenerated_text() { return generated_text; }
//		    public void setGenerated_text(String generated_text) { this.generated_text = generated_text; }
//
//            public GeneratedTextDetails getDetails() { return this.details; }
//            public void setDetails(GeneratedTextDetails details) { this.details = details; }
//            
//		    @Override
//		    public String toString() {
//		        return "GeneratedTextItem{" +
//		                "generatedText='" + generated_text + '\'' +
//		                ", details=" + details +
//		                '}';
//		    }
//
//		    public class GeneratedTextDetails {
//		        private String finishReason;
//		        private int generatedTokens;
//		        private Object seed; // Use Object if the type of 'seed' is unknown or can vary
//		        private List<Object> prefill; // Use List<Object> if the type of list elements is unknown or can vary
//		        private List<Token> tokens;
//		        
//	            public String getFinishReason() { return this.finishReason; }
//	            public void setFinishReason(String finishReason) { this.finishReason = finishReason; }
//	            
//	            public int getGeneratedTokens() { return this.generatedTokens; }
//	            public void setGeneratedTokens(int generatedTokens) { this.generatedTokens = generatedTokens; }
//	            
//	            public Object getSeed() { return this.seed; }
//	            public void setSeed(Object seed) { this.seed = seed; }
//	            
//	            public List<Object> getPrefill() { return this.prefill; }
//	            public void setPrefill(List<Object> prefill) { this.prefill = prefill; }
//	            
//	            public List<Token> getTokens() { return this.tokens; }
//	            public void setTokens(List<Token> tokens) { this.tokens = tokens; }
//
//		        @Override
//		        public String toString() {
//		            return "GeneratedTextDetails{" +
//		                    "finishReason='" + finishReason + '\'' +
//		                    ", generatedTokens=" + generatedTokens +
//		                    ", seed=" + seed +
//		                    ", prefill=" + prefill +
//		                    ", tokens=" + tokens +
//		                    '}';
//		        }
//
//		        public class Token {
//		            private int id;
//		            private String text;
//		            private double logprob;
//		            private boolean special;
//
//		            public int getId() { return this.id; }
//		            public void setId(int id) { this.id = id; }
//		            
//		            public String getText() { return this.text; }
//		            public void setText(String text) { this.text = text; }
//		            
//		            public double getLogprob() { return this.logprob; }
//		            public void setLogprob(double logprob) { this.logprob = logprob; }
//		            
//		            public boolean getSpecial() { return this.special; }
//		            public void setSpecial(boolean special) { this.special = special; }
//
//		            @Override
//		            public String toString() {
//		                return "Token{" +
//		                        "id=" + id +
//		                        ", text='" + text + '\'' +
//		                        ", logprob=" + logprob +
//		                        ", special=" + special +
//		                        '}';
//		            }
//		        }
//		    }
//		}
//		
//		@Override
//		public void appendStream(IModelEngineResponseStreamHandler partial) {
//			// TODO Auto-generated method stub
//			
//		}
//
//		@Override
//		public void setResponse(Object response) {
//			// TODO Auto-generated method stub
//			
//		}
//
//		@Override
//		public List<IModelEngineResponseStreamHandler> getPartialResponses() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public Class<? extends IModelEngineResponseStreamHandler> getStreamHandlerClass() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public Map<String, Object> getModelEngineResponse() {
//
//			Map<String, Object> modelEngineResponse = new HashMap<String, Object>();
//			modelEngineResponse.put(AbstractModelEngineResponse.RESPONSE, this.getResponse());
//	        
//			List<IModelEngineResponseStreamHandler> partialResponses = this.getPartialResponses();
//			if (partialResponses != null && !partialResponses.isEmpty()) {
//				// need to build the responses here
//				modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_PROMPT, null);
//				modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_RESPONSE, partialResponses.size());
//		        
//			} else {
//				modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_PROMPT, this.getPromptTokens());
//		        modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_RESPONSE, this.getResponseTokens());
//			}
//
//			// copied from open ai chat completion rest engine
//			// need to do the travelsal and add null checks
////			if (this.getChoices() != null && this.getChoices().get(0).getLogprobs() != null) {
////				modelEngineResponse.put("logprobs", this.getChoices().get(0).getLogprobs());
////			}
//			
//			return modelEngineResponse;
//			
//		}
//
//		@Override
//		public Object getResponse() {
//			return this.response != null ? this.response : this.getGeneratedItems().get(0).getGenerated_text();
//		}
//
//		@Override
//		public Integer getPromptTokens() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public Integer getResponseTokens() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//		
//	}
//
//	@Override
//	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters) {
//		return new EmbeddingsModelEngineResponse(null, null, null);
//	}
//
//	@Override
//	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
//		return "This model does have an model method defined.";
//	}
//	
//	@Override
//	protected void resetAfterTimeout() {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	
//	////////////////////////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////////////////////////
//	////////////////////////////////////////////////////////////////////////////////
//	
//	
//	public static void main(String [] args) {
//		String responseData = "[{\"generated_text\":\"?\\n\\nA potato is a starchy, tuberous crop from the nightshade\",\"details\":{\"finish_reason\":\"length\",\"generated_tokens\":20,\"seed\":null,\"prefill\":[],\"tokens\":[{\"id\":28804,\"text\":\"?\",\"logprob\":-1.109375,\"special\":false},{\"id\":13,\"text\":\"\\n\",\"logprob\":-0.18688965,\"special\":false},{\"id\":13,\"text\":\"\\n\",\"logprob\":-0.20361328,\"special\":false},{\"id\":28741,\"text\":\"A\",\"logprob\":-0.66845703,\"special\":false},{\"id\":2513,\"text\":\" pot\",\"logprob\":-0.06793213,\"special\":false},{\"id\":1827,\"text\":\"ato\",\"logprob\":-0.0016536713,\"special\":false},{\"id\":349,\"text\":\" is\",\"logprob\":-0.5620117,\"special\":false},{\"id\":264,\"text\":\" a\",\"logprob\":-0.06652832,\"special\":false},{\"id\":341,\"text\":\" st\",\"logprob\":-0.049713135,\"special\":false},{\"id\":12940,\"text\":\"archy\",\"logprob\":-0.0025253296,\"special\":false},{\"id\":28725,\"text\":\",\",\"logprob\":-0.004283905,\"special\":false},{\"id\":11988,\"text\":\" tub\",\"logprob\":-0.01637268,\"special\":false},{\"id\":263,\"text\":\"er\",\"logprob\":-0.002298355,\"special\":false},{\"id\":607,\"text\":\"ous\",\"logprob\":-0.0007982254,\"special\":false},{\"id\":21948,\"text\":\" crop\",\"logprob\":-0.11639404,\"special\":false},{\"id\":477,\"text\":\" from\",\"logprob\":-0.006752014,\"special\":false},{\"id\":272,\"text\":\" the\",\"logprob\":-0.00016498566,\"special\":false},{\"id\":2125,\"text\":\" night\",\"logprob\":-0.6015625,\"special\":false},{\"id\":811,\"text\":\"sh\",\"logprob\":-0.00072956085,\"special\":false},{\"id\":770,\"text\":\"ade\",\"logprob\":-0.000056505203,\"special\":false}]}}]";
//		
//		TextGenerationInferenceRestEngine engine = new TextGenerationInferenceRestEngine();
//		engine.handleDeserialization(responseData, null);
//		
//		Gson gson = new Gson();
//		String value = "{\"generated_text\":\"tell me a joke that will make me laugh\\nWhy did the tomato turn red? Because it saw the salad dressing!\"}";
//		GeneratedTextItem test = gson.fromJson(value, GeneratedTextItem.class);
//		
//		System.out.println("MADE IT");
//	}
//}
