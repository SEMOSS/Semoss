package prerna.engine.impl.model;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.IModelEngineResponseHandler;
import prerna.engine.impl.model.responses.IModelEngineResponseStreamHandler;
import prerna.om.Insight;
import prerna.util.Constants;

public class OpenAiChatCompletionRestEngine extends RESTModelEngine {
	
	//TODO decide what we want logged
	private static final Logger classLogger = LogManager.getLogger(OpenAiChatCompletionRestEngine.class);

	private static final String ENDPOINT = "ENDPOINT";
	private static final String OPENAI_API_KEY = "OPENAI_API_KEY";
	
	private String endpoint;
	private String openAiApiKey;
	private String modelName;
	private Map<String, String> headersMap;
	
	private Map<String, ConversationChain> conversationHisotry = new Hashtable<>();
	
	Gson gson = new GsonBuilder()
		    .registerTypeAdapter(ConversationChain.class, new ConversationChainSerializer())
		    .create();
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.endpoint = this.smssProp.getProperty(ENDPOINT);
		if(this.endpoint == null || (this.endpoint=this.endpoint.trim()).isEmpty()) {
			throw new IllegalArgumentException("This model requires a valid value for " + ENDPOINT);
		}
		
		this.openAiApiKey = this.smssProp.getProperty(OPENAI_API_KEY);
		if(this.openAiApiKey == null || (this.openAiApiKey=this.openAiApiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("This model requires a valid value for " + OPENAI_API_KEY);
		}
		
		this.modelName = this.smssProp.getProperty(Constants.MODEL);
		if(this.modelName == null || (this.modelName=this.modelName.trim()).isEmpty()) {
			throw new IllegalArgumentException("This model requires a valid value for " + Constants.MODEL);
		}
		
		this.headersMap = new HashMap<>();
		this.headersMap.put("Authorization", "Bearer " + this.openAiApiKey);
	}
	
	@Override
	protected AskModelEngineResponse askCall(String question, String context, Insight insight, Map<String, Object> parameters) {
		
		String insightId = insight.getInsightId();
		
		Map<String, Object> bodyMap = new HashMap<>();
		bodyMap.put("model", this.modelName);
		
		Object streamResponse = parameters.remove("stream");
		boolean stream;
		if (streamResponse == null) {
			stream = true;
		} else {
			stream = Boolean.parseBoolean(streamResponse + "");
		}
		
		bodyMap.put("stream", stream);
		
		bodyMap.putAll(this.adjustHyperParameters(parameters));
		
		resetTimer();
		if (parameters.containsKey("full_prompt")) {
			// if you are using full_promot then the user is responsible for keeping history
			bodyMap.put("messages", parameters.remove("full_prompt"));
			IModelEngineResponseHandler modelResponse = postRequestStringBody(this.endpoint, this.headersMap, gson.toJson(bodyMap), ContentType.APPLICATION_JSON, null, null, null, stream, ChatCompletion.class, insightId);
			Map<String, Object> modelEngineResponseMap = modelResponse.getModelEngineResponse();
			
			
			return AskModelEngineResponse.fromMap(modelEngineResponseMap);
		} else {
			ConversationChain messages = createMessageList(question, context, insight.getUserId(), insightId);
			bodyMap.put("messages", messages);
			
			IModelEngineResponseHandler modelResponse = postRequestStringBody(this.endpoint, this.headersMap, gson.toJson(bodyMap), ContentType.APPLICATION_JSON, null, null, null, stream, ChatCompletion.class, insightId);
			Map<String, Object> modelEngineResponseMap = modelResponse.getModelEngineResponse();
			
			AskModelEngineResponse askResponse = AskModelEngineResponse.fromMap(modelEngineResponseMap);
			
			messages.addModelResponse(askResponse.getResponse());
			
			return askResponse;
		}
	}
	
	private ConversationChain createMessageList(String question, String context, String userId, String insightId) {
		ConversationChain conversationChain;
		if (this.keepsConversationHistory()){
			// are we keeping track of converation history
			if (this.conversationHisotry.containsKey(insightId)) {
				// does the chain already exist
				conversationChain = this.conversationHisotry.get(insightId);
			} else {
				// otherwise, create a new chain
				conversationChain = new ConversationChain();
				
				// look in the logs to determine if this is a past session we need to persist
				if (this.keepInputOutput()) {
					List<Map<String, Object>> convoHistoryFromDb = ModelInferenceLogsUtils.doRetrieveConversation(userId, insightId, "ASC");
					for (Map<String, Object> record : convoHistoryFromDb) {
						
						Object messageData = record.get("MESSAGE_DATA");
						if (record.get("MESSAGE_TYPE").equals("RESPONSE")) {
							conversationChain.addModelResponse(messageData+"");    
						} else {
							conversationChain.addUserInput(messageData+"");
						}
					}
				}
				
				this.conversationHisotry.put(insightId, conversationChain);
			}
		} else {
			conversationChain = new ConversationChain();
		}
				
		if (context != null) {
			conversationChain.setContext(context);
		}
		
		conversationChain.addUserInput(question);
		
		return conversationChain;
	}
	
	private Map<String, Object> adjustHyperParameters(Map<String, Object> hyperParameters) {

		// Check the types of each parameter
		if (hyperParameters.get("frequency_penalty") != null && !(hyperParameters.get("frequency_penalty") instanceof Number)) {
		    throw new IllegalArgumentException("The hyperparameter frequency_penalty is set but is not a number.");
		}

		if (hyperParameters.get("logit_bias") != null && !(hyperParameters.get("logit_bias") instanceof Map)) {
		    throw new IllegalArgumentException("The hyperparameter logit_bias is set but is not a map.");
		}

		if (hyperParameters.get("logprobs") != null && !(hyperParameters.get("logprobs") instanceof Boolean)) {
		    throw new IllegalArgumentException("The hyperparameter logprobs is set but is not a boolean.");
		}

		if (hyperParameters.get("top_logprobs") != null && !(hyperParameters.get("top_logprobs") instanceof Integer)) {
		    throw new IllegalArgumentException("The hyperparameter top_logprobs is set but is not an integer.");
		}

		if (hyperParameters.get("max_tokens") != null || hyperParameters.get("max_new_tokens") != null) {
			boolean hasMaxNewTokens = hyperParameters.containsKey("max_new_tokens");
			
			if (hasMaxNewTokens) {
				Object maxTokens = hyperParameters.remove("max_new_tokens");
				
				if (!(maxTokens instanceof Integer)) {
					throw new IllegalArgumentException("The hyperparameter max_new_tokens is set but is not an integer.");
				}
				
				hyperParameters.put("max_tokens", maxTokens);
			} else {
				if (!(hyperParameters.get("max_tokens") instanceof Integer)) {
				    throw new IllegalArgumentException("The hyperparameter max_tokens is set but is not an integer.");
				}
			}
		}

		if (hyperParameters.get("n") != null && !(hyperParameters.get("n") instanceof Integer)) {
		    throw new IllegalArgumentException("The hyperparameter n is set but is not an integer.");
		}

		if (hyperParameters.get("presence_penalty") != null && !(hyperParameters.get("presence_penalty") instanceof Number)) {
		    throw new IllegalArgumentException("The hyperparameter presence_penalty is set but is not a number.");
		}

		if (hyperParameters.get("response_format") != null && !(hyperParameters.get("response_format") instanceof Map)) {
		    throw new IllegalArgumentException("The hyperparameter response_format is set but is not a map.");
		}

		if (hyperParameters.get("seed") != null && !(hyperParameters.get("seed") instanceof Integer)) {
		    throw new IllegalArgumentException("The hyperparameter seed is set but is not an integer.");
		}

		if (hyperParameters.get("stop") != null && !(hyperParameters.get("stop") instanceof String) && !(hyperParameters.get("stop") instanceof List)) {
		    throw new IllegalArgumentException("The hyperparameter stop is set but is neither a string nor a list.");
		}

		if (hyperParameters.get("temperature") != null && !(hyperParameters.get("temperature") instanceof Number)) {
		    throw new IllegalArgumentException("The hyperparameter temperature is set but is not a number.");
		}

		if (hyperParameters.get("top_p") != null && !(hyperParameters.get("top_p") instanceof Number)) {
		    throw new IllegalArgumentException("The hyperparameter top_p is set but is not a number.");
		}

		if (hyperParameters.get("tools") != null && !(hyperParameters.get("tools") instanceof List)) {
		    throw new IllegalArgumentException("The hyperparameter tools is set but is not a list.");
		}

		if (hyperParameters.get("tool_choice") != null && !(hyperParameters.get("tool_choice") instanceof String) && !(hyperParameters.get("tool_choice") instanceof Map)) {
		    throw new IllegalArgumentException("The hyperparameter tool_choice is set but is neither a string nor a map.");
		}

		if (hyperParameters.get("user") != null && !(hyperParameters.get("user") instanceof String)) {
		    throw new IllegalArgumentException("The hyperparameter user is set but is not a string.");
		}

		return hyperParameters;
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.OPEN_AI;
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters) {
		return new EmbeddingsModelEngineResponse(null, null, null);
	}

	@Override
	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
		return "This model does have an model method defined.";
	}
	
	/**
	 * This class is used to process the responses from the Open AI Chat Completions API.
	 * This is a static class so that an object can be instantiated without the need to first instantiate {@code OpenAiChatCompletionRestEngine}.
	 * <p>
	 * https://platform.openai.com/docs/guides/text-generation/chat-completions-api
	 * <br>
	 * https://platform.openai.com/docs/api-reference/chat
	*/
	public static class ChatCompletion implements IModelEngineResponseHandler {
	    private String id;
	    private String object;
	    private long created;
	    private String model;
	    private String system_fingerprint;
	    private List<Choice> choices;
	    private Usage usage;
	    
	    private Object response = null;

	    private List<IModelEngineResponseStreamHandler> partials = new ArrayList<>();
	    
	    public ChatCompletion() {
	    	
	    }

	    // Getters and Setters for ChatCompletion fields
	    public String getId() { return id; }
	    public void setId(String id) { this.id = id; }
	    
	    public String getObject() { return object; }
	    public void setObject(String object) { this.object = object; }
	    
	    public long getCreated() { return created; }
	    public void setCreated(long created) { this.created = created; }
	    
	    public String getModel() { return model; }
	    public void setModel(String model) { this.model = model; }
	    
	    public String getSystem_fingerprint() { return system_fingerprint; }
	    public void setSystem_fingerprint(String system_fingerprint) { this.system_fingerprint = system_fingerprint; }
	    
	    public List<Choice> getChoices() { return choices; }
	    public void setChoices(List<Choice> choices) { this.choices = choices; }
	    
	    public Usage getUsage() { return usage; }
	    public void setUsage(Usage usage) { this.usage = usage; }

	    public class Choice {
	        private int index;
	        private Message message;
	        private LogProbsContent logprobs = null;
	        private String finish_reason;

	        // Getters and Setters for Choice fields
	        public int getIndex() { return index; }
	        public void setIndex(int index) { this.index = index; }
	        
	        public Message getMessage() { return message; }
	        public void setMessage(Message message) { this.message = message; }
	        
	        public LogProbsContent getLogprobs() { return logprobs; }
	        public void setLogprobs(LogProbsContent logprobs) { this.logprobs = logprobs; }
	        
	        public String getFinish_reason() { return finish_reason; }
	        public void setFinish_reason(String finish_reason) { this.finish_reason = finish_reason; }
	    }

	    public class Message {
	        private String role;
	        private String content;

	        // Getters and Setters for Message fields
	        public String getRole() { return role; }
	        public void setRole(String role) { this.role = role; }
	        
	        public String getContent() { return content; }
	        public void setContent(String content) { this.content = content; }
	    }

	    public class Usage {
	        private int prompt_tokens;
	        private int completion_tokens;
	        private int total_tokens;

	        // Getters and Setters for Usage fields
	        public int getPrompt_tokens() { return prompt_tokens; }
	        public void setPrompt_tokens(int prompt_tokens) { this.prompt_tokens = prompt_tokens; }
	        
	        public int getCompletion_tokens() { return completion_tokens; }
	        public void setCompletion_tokens(int completion_tokens) { this.completion_tokens = completion_tokens; }
	        
	        public int getTotal_tokens() { return total_tokens; }
	        public void setTotal_tokens(int total_tokens) { this.total_tokens = total_tokens; }
	    }
	    
	    public class LogProbsContent {
	        private List<TokenLogProb> content; // Represents the array of TokenLogProb

	        public List<TokenLogProb> getContent() { return content; }
	        public void setContent(List<TokenLogProb> content) { this.content = content; }
	    }
	    
	    public class TokenLogProb {
	        private String token;
	        private double logprob;
	        private List<Integer> bytes;
	        private List<TopLogProb> top_logprobs;

	        // Getters and Setters
	        public String getToken() { return token; }
	        public void setToken(String token) { this.token = token; }

	        public double getLogprob() { return logprob; }
	        public void setLogprob(double logprob) { this.logprob = logprob; }

	        public List<Integer> getBytes() { return bytes; }
	        public void setBytes(List<Integer> bytes) { this.bytes = bytes; }

	        public List<TopLogProb> getTop_logprobs() { return top_logprobs; }
	        public void setTop_logprobs(List<TopLogProb> top_logprobs) { this.top_logprobs = top_logprobs; }
	    }

	    public class TopLogProb {
	        private String token;
	        private double logprob;
	        private List<Integer> bytes;

	        // Getters and Setters
	        public String getToken() { return token; }
	        public void setToken(String token) { this.token = token; }

	        public double getLogprob() { return logprob; }
	        public void setLogprob(double logprob) { this.logprob = logprob; }

	        public List<Integer> getBytes() { return bytes; }
	        public void setBytes(List<Integer> bytes) { this.bytes = bytes; }
	    }

		@Override
		public Map<String, Object> getModelEngineResponse() {
			
			Map<String, Object> modelEngineResponse = new HashMap<String, Object>();
			modelEngineResponse.put(AbstractModelEngineResponse.RESPONSE, this.getResponse());
	        
			List<IModelEngineResponseStreamHandler> partialResponses = this.getPartialResponses();
			if (partialResponses != null && !partialResponses.isEmpty()) {
				// need to build the responses here
				modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_PROMPT, null);
				modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_RESPONSE, partialResponses.size());
		        
			} else {
				modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_PROMPT, this.getPromptTokens());
		        modelEngineResponse.put(AbstractModelEngineResponse.NUMBER_OF_TOKENS_IN_RESPONSE, this.getResponseTokens());
			}

			if (this.getChoices() != null && this.getChoices().get(0).getLogprobs() != null) {
				modelEngineResponse.put("logprobs", this.getChoices().get(0).getLogprobs());
			}
			
			return modelEngineResponse;
		}
		
		public void setResponse(Object response) {
			this.response = response;
		}
		
		@Override
		public Object getResponse() {
			return this.response != null ? this.response : this.getChoices().get(0).getMessage().getContent();
		}
		@Override
		public Integer getPromptTokens() {
			return this.getUsage().getPrompt_tokens();
		}
		@Override
		public Integer getResponseTokens() {
			return this.getUsage().getCompletion_tokens();
		}
		@Override
		public void appendStream(IModelEngineResponseStreamHandler partial) {
			this.partials.add(partial);
		}
		@Override
		public Class<? extends IModelEngineResponseStreamHandler> getStreamHandlerClass() {
			return ChatCompletionStream.class;
		}

		@Override
		public List<IModelEngineResponseStreamHandler> getPartialResponses() {
			return this.partials;
		}
	}
	
	/**
	 * This class is used to process the partial responses for the Open AI Chat Completions API.
	 * This is a static class so that an object can be instantiated without the need to first instantiate {@code OpenAiChatCompletionRestEngine}.
	 * <p>
	 * https://platform.openai.com/docs/api-reference/chat
	*/
	public static class ChatCompletionStream implements IModelEngineResponseStreamHandler {
	    private String id;
	    private String object;
	    private long created;
	    private String model;
	    private String systemFingerprint;
	    private List<Choice> choices;
	    
	    public ChatCompletionStream() {
	    	
	    }

	    // Getters and Setters
	    public String getId() { return id; }
	    public void setId(String id) { this.id = id; }
	    public String getObject() { return object; }
	    public void setObject(String object) { this.object = object; }
	    public long getCreated() { return created; }
	    public void setCreated(long created) { this.created = created; }
	    public String getModel() { return model; }
	    public void setModel(String model) { this.model = model; }
	    public String getSystemFingerprint() { return systemFingerprint; }
	    public void setSystemFingerprint(String systemFingerprint) { this.systemFingerprint = systemFingerprint; }
	    public List<Choice> getChoices() { return choices; }
	    public void setChoices(List<Choice> choices) { this.choices = choices; }

	    public class Choice {
	        private int index;
	        private Delta delta;
	        private Object logprobs;
	        private String finishReason;

	        // Getters and Setters
	        public int getIndex() { return index; }
	        public void setIndex(int index) { this.index = index; }
	        public Delta getDelta() { return delta; }
	        public void setDelta(Delta delta) { this.delta = delta; }
	        public Object getLogprobs() { return logprobs; }
	        public void setLogprobs(Object logprobs) { this.logprobs = logprobs; }
	        public String getFinishReason() { return finishReason; }
	        public void setFinishReason(String finishReason) { this.finishReason = finishReason; }
	    }

	    public class Delta {
	        private String content;
	        
	        // Getters and Setters
	        public String getContent() { return content; }
	        public void setContent(String content) { this.content = content; }
	    }

		@Override
		public Object getPartialResponse() {
			return this.getChoices().get(0).getDelta().getContent();
		}
	}
	
	private class ConversationChain {
		private Integer tokenCount = 0;
		private ChatCompletionMessage context = new ChatCompletionMessage("system", "You are a helpful assistant.");;
		private List<ChatCompletionMessage> messages = new ArrayList<>();
		
		public ConversationChain() {
			
		}
		
		public void setContext(String context) {
			this.context = new ChatCompletionMessage("system", context);
		}
		
		public ChatCompletionMessage getContext() {
			return this.context;
		}
		
		public void addTokens(Integer numberOfTokens) {
			this.tokenCount += numberOfTokens;
		}
		
		public void addUserInput(String content) {
			this.messages.add(new ChatCompletionMessage("user", content));
		}
		
		public void addModelResponse(String content) {
			this.messages.add(new ChatCompletionMessage("assistant", content));
		}
		
		public List<ChatCompletionMessage> getMessages(){
			return this.messages;
		}
		
		public List<ChatCompletionMessage> getSerializedForm() {
	        List<ChatCompletionMessage> serializedForm = new ArrayList<>();
	        serializedForm.add(this.context); // Add context as the first item
	        serializedForm.addAll(this.messages); // Add all other messages
	        return serializedForm;
	    }
	}
	
	public class ConversationChainSerializer implements JsonSerializer<ConversationChain> {
	    @Override
	    public JsonElement serialize(ConversationChain src, Type typeOfSrc, JsonSerializationContext context) {
	        // Use getSerializedForm to get the list including context and messages
	        List<ChatCompletionMessage> messages = src.getSerializedForm();

	        // Serialize this list to JSON
	        JsonArray jsonArray = new JsonArray();
	        for (ChatCompletionMessage message : messages) {
	            JsonElement jsonElement = context.serialize(message);
	            jsonArray.add(jsonElement);
	        }

	        return jsonArray;
	    }
	}
	
	private class ChatCompletionMessage implements Serializable {
		
		private String role;
		private String content;
		
		public ChatCompletionMessage() {
			
		}
		
		public ChatCompletionMessage(String role, String content) {
			this.role = role;
			this.content = content;
		}
	
		public void setRole(String role) {
			this.role = role;
		}
		
		public String getRole() {
			return this.role;
		}
		
		public void setContent(String content) {
			this.content = content;
		}
		
		public String getContent() {
			return this.content;
		}
	}
	
	@Override
	public void close() throws IOException {
		this.conversationHisotry.clear();
		
		super.close();
	}

	@Override
	protected void resetAfterTimeout() {
		this.conversationHisotry.clear();
	}
}
