package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;


public class ResponseMessage extends AbstractMessage {
    @SerializedName("content")
    private String content;
    @SerializedName("type")
    private MessageType type = MessageType.RESPONSE_TEXT;
    
 // Multiple tool responses as a list of maps
    @SerializedName("tool_responses")
    private List<Map<String, Object>> toolResponses = new ArrayList<>();


    private ResponseMessage() {
        super();
    }

    @Override
    public MessageType getMessageType() {
        return type;
    }

    public String getContent() {
        return content;
    }
    
    public List<Map<String, Object>> getToolResponses() {
        return new ArrayList<>(toolResponses);
    }
    
    public boolean hasToolResponses() {
        return toolResponses != null && !toolResponses.isEmpty();
    }

    public void setContent(String content) {
        this.content = content;
        this.formattedMessage = null;
    }

    public void setMessageType(MessageType type) {
        this.type = type;
        this.formattedMessage = null;
    }
    
    public void setToolResponses(List<Map<String, Object>> toolResponses) {
        if (toolResponses == null) {
            this.toolResponses = new ArrayList<>();
        } else {
            this.toolResponses = new ArrayList<>(toolResponses);
        }
         this.type = MessageType.RESPONSE_TOOL;
    }


    // Builder pattern for ResponseMessage
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ResponseMessage message = new ResponseMessage();

        public Builder withText(String content) {
            message.content = content;
            return this;
        }

        public Builder withType(MessageType type) {
            message.type = type;
            return this;
        }
        
        public Builder withToolResponses(List<Map<String, Object>> toolResponses) {
            message.setToolResponses(toolResponses);
            message.type = MessageType.RESPONSE_TOOL;
            return this;
        }
        
        public Builder addToolResponse(Map<String, Object> toolResponse) {
            if (message.toolResponses == null) {
                message.toolResponses = new ArrayList<>();
            }
            message.toolResponses.add(toolResponse);
            message.type = MessageType.RESPONSE_TOOL;
            return this;
        }
        
        
        public ResponseMessage build() {
            if (message.type == null) {
                message.type = MessageType.RESPONSE_TEXT;
            }
            return message;
        }
    }
    
    // Some factory/convenience methods
    public static ResponseMessage text(String content) {
        return builder().withText(content).withType(MessageType.RESPONSE_TEXT).build();
    }
    
    public static ResponseMessage toolResponses(List<Map<String, Object>> toolResponses) {
        return builder().withToolResponses(toolResponses).build();
    }
    
    public static ResponseMessage system(String content) {
        return builder().withText(content).withType(MessageType.SYSTEM).build();
    }
}