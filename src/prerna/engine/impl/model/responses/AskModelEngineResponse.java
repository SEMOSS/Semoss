package prerna.engine.impl.model.responses;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.ToNumberPolicy;

import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessagePartAdapter;
import prerna.engine.impl.model.message.ThinkingMessagePart;
import prerna.engine.impl.model.message.ToolCallMessagePart;

public abstract class AskModelEngineResponse<T> extends AbstractModelEngineResponse<T> {

    private static final long serialVersionUID = 1L;

    public static final String MESSAGE_ID = "messageId";
    public static final String ROOM_ID = "roomId";
    public static final String MESSAGE_TYPE = "messageType";
    public static final String THINKING = "thinking";
    public static final String SCHEMA_VERSION = "schemaVersion";
    public static final String IO = "io";
    public static final String PARTS = "parts";
    public static final String CHAT = "CHAT";
    public static final String TOOL = "TOOL";
    public static final String IMAGE = "IMAGE";
    public static final String TTS = "TTS";

    private static final Gson PARTS_GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .registerTypeAdapter(MessagePart.class, new MessagePartAdapter())
            .disableHtmlEscaping()
            .create();

    protected String messageId;
    protected String roomId;
    protected String messageType = CHAT;
    protected String thinking;

    // New parts-based payload (preferred). Legacy callers may still use messageType/response.
    protected Integer schemaVersion;
    protected MessageIO io;
    protected List<MessagePart> parts;
    
    public AskModelEngineResponse(T response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageId() {
        return this.messageId;
    }

    public void setRoomId(String roomId) {
        this.roomId = roomId;
    }

    public String getRoomId() {
        return this.roomId;
    }
    
    public String getMessageType() {
    	return this.messageType;
    }

    public String getThinking() {
        return this.thinking;
    }

    public void setThinking(String thinking) {
        this.thinking = thinking;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public MessageIO getIo() {
        return io;
    }

    public void setIo(MessageIO io) {
        this.io = io;
    }

    public List<MessagePart> getParts() {
        return parts == null ? new ArrayList<>() : new ArrayList<>(parts);
    }

    public void setParts(List<MessagePart> parts) {
        this.parts = (parts == null) ? null : new ArrayList<>(parts);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> responseMap = super.toMap();
        responseMap.put(MESSAGE_ID, this.messageId);
        responseMap.put(ROOM_ID, this.roomId);
        responseMap.put(MESSAGE_TYPE, this.messageType);
        if (this.schemaVersion != null) {
            responseMap.put(SCHEMA_VERSION, this.schemaVersion);
        }
        if (this.io != null) {
            responseMap.put(IO, this.io.name());
        }
        if (this.parts != null && !this.parts.isEmpty()) {
            responseMap.put(PARTS, this.parts);
        }
        if (this.thinking != null) {
            responseMap.put(THINKING, this.thinking);
        }
        return responseMap;
    }
    
	public abstract String getStringResponse();

 // Factory method to create the appropriate response type
    @SuppressWarnings("unchecked")
    public static AskModelEngineResponse<?> fromMap(Object responseObject) {
        Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
        Object response = modelResponse.get(RESPONSE);

        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

        // Parse parts payload if present (new format).
        Integer schemaVersion = getTokens(modelResponse.get(SCHEMA_VERSION));
        MessageIO io = null;
        Object ioObj = modelResponse.get(IO);
        if (ioObj instanceof String) {
            try {
                io = MessageIO.valueOf((String) ioObj);
            } catch (IllegalArgumentException ignore) {
                io = null;
            }
        }
        List<MessagePart> parts = parseParts(modelResponse.get(PARTS));

        // Set default messageType
        String messageType = CHAT;

        // Check if MESSAGE_TYPE is present and valid
        Object messageTypeObject = modelResponse.get(MESSAGE_TYPE);
        if (messageTypeObject != null) {
            if (messageTypeObject instanceof String) {
                messageType = (String) messageTypeObject;
            } else {
                throw new IllegalArgumentException("MESSAGE_TYPE is not a String");
            }
        } else if (parts != null && !parts.isEmpty()) {
            messageType = deriveLegacyMessageType(parts);
        }

        AskModelEngineResponse<?> askResponse;

        // Adjust logic based on messageType
        if (TOOL.equals(messageType)) {
            if (response instanceof List) {
            	//TODO: why are we grabbing only 1 tool???
            	//TODO: why are we grabbing only 1 tool???
            	//TODO: why are we grabbing only 1 tool???
            	//TODO: why are we grabbing only 1 tool???
            	//TODO: why are we grabbing only 1 tool???
            	//TODO: why are we grabbing only 1 tool???
                List<?> responseList = (List<?>) response;
                if (!responseList.isEmpty()) {
                    askResponse = new AskToolModelEngineResponse((List<Map<String, Object>>) responseList, tokensInPrompt, tokensInResponse);
                } else {
                    throw new IllegalArgumentException("Tool list is empty or not valid");
                }
            } else {
                throw new IllegalArgumentException("Expected a List response for Tool messageType");
            }
        } else if (CHAT.equals(messageType)) {
            if (response instanceof String) {
                askResponse = new AskStringModelEngineResponse((String) response, tokensInPrompt, tokensInResponse);
            } else {
                throw new IllegalArgumentException("Expected a String response for Chat messageType");
            }
        }  else if (IMAGE.equals(messageType)) {
            if (response instanceof List) {
                List<?> responseList = (List<?>) response;
                
                // Validate that all items in the list are strings (base64 or URLs)
                for (Object item : responseList) {
                    if (!(item instanceof String)) {
                        throw new IllegalArgumentException("Expected List<String> for Image messageType, but found non-String item: " + item.getClass().getSimpleName());
                    }
                }
                
                // Cast to List<String> since we've validated all items are strings
                @SuppressWarnings("unchecked")
                List<String> imageList = (List<String>) responseList;
                
                // Use the OpenAI factory method
                askResponse = AskImageModelEngineResponse.getOpenAIImageResponse(imageList, tokensInPrompt, tokensInResponse);
                
            } else {
                throw new IllegalArgumentException("Expected a List<String> response for Image messageType, but received: " + response.getClass().getSimpleName());
            }
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + messageType);
        }

        // Extract thinking if present
        Object thinkingObj = modelResponse.get(THINKING);
        if (thinkingObj instanceof String) {
            askResponse.setThinking((String) thinkingObj);
        } else if (askResponse.getThinking() == null && parts != null) {
            for (MessagePart p : parts) {
                if (p instanceof ThinkingMessagePart) {
                    String t = ((ThinkingMessagePart) p).getThinking();
                    if (t != null && !t.isEmpty()) {
                        askResponse.setThinking(t);
                        break;
                    }
                }
            }
        }

        if (schemaVersion != null) {
            askResponse.setSchemaVersion(schemaVersion);
        }
        if (io != null) {
            askResponse.setIo(io);
        }
        if (parts != null && !parts.isEmpty()) {
            askResponse.setParts(parts);
        }

        return askResponse;
    }

    
	@SuppressWarnings("unchecked")
	public static AskModelEngineResponse fromObject(Object responseObject) {
		if(!(responseObject instanceof Map)) {
			throw new IllegalArgumentException("Expected map output. Instead received value: " + responseObject);
		}
		Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
		return fromMap(modelResponse);
    }

    @SuppressWarnings("unchecked")
    private static List<MessagePart> parseParts(Object partsObj) {
        if (!(partsObj instanceof List)) {
            return null;
        }
        List<?> list = (List<?>) partsObj;
        List<MessagePart> out = new ArrayList<>();
        for (Object elem : list) {
            if (elem == null) {
                continue;
            }
            JsonElement je = PARTS_GSON.toJsonTree(elem);
            try {
                MessagePart part = PARTS_GSON.fromJson(je, MessagePart.class);
                if (part != null) {
                    out.add(part);
                }
            } catch (Exception ignore) {
                // ignore individual part failures for forward compatibility
            }
        }
        return out;
    }

    private static String deriveLegacyMessageType(List<MessagePart> parts) {
        boolean hasTool = false;
        boolean hasMedia = false;
        for (MessagePart part : parts) {
            if (part instanceof ToolCallMessagePart) {
                hasTool = true;
                break;
            }
            if (part instanceof MediaMessagePart) {
                hasMedia = true;
            }
        }
        if (hasTool) {
            return TOOL;
        }
        if (hasMedia) {
            return IMAGE;
        }
        return CHAT;
    }
}
