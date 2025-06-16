package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.om.Insight;

/**
 * Unified message class that can handle all types of messages (text, image, tool calls, etc.)
 * This class aims to replace the current AbstractMessage hierarchy while maintaining backward compatibility.
 */
public class InputMessage extends AbstractMessage {

    // Input message to be shown on UI
    @SerializedName("inputMessage")
    private String inputMessage;
    
    // inputPrompt to be sent to the Model
    private transient String inputPrompt;
    
    @SerializedName("type")
    private MessageType type = MessageType.INPUT_TEXT;
    
    // Tool-related fields
    @SerializedName("tool_calls")
    private List<Map<String, Object>> toolCalls = new ArrayList<>();
    	
	@SerializedName("tool_call_id")
    private String toolCallId;   // For tool result messages only

    @SerializedName("tool_name")
    private String toolName;     // For tool result messages only

    
    
    // Additional metadata
    @SerializedName("ornaments")
    private Map<String, Object> ornaments = new HashMap<>();

	private Map<String, Object> paramMap =  new HashMap<>();

    private transient List<ImageInfo> imageInfos = new ArrayList<>();

    // Private constructor - use Builder
    private InputMessage() {
        super();
    }

    @Override
    public MessageType getMessageType() {
        return type;
    }
    
    public void setMessageType(MessageType type) {
        this.type = type;
        this.formattedMessage = null;
    }
    
    /** Get the effective prompt to send to the LLM (RAG: includes user + chunks). */
    public String getInputPrompt() {
        return (inputPrompt == null || inputPrompt.trim().isEmpty())
                ? inputMessage
                : inputPrompt;
    }

    public void setInputPrompt(String inputPrompt) {
        this.inputPrompt = inputPrompt;
    }

    public String getInputMessage() {
        return inputMessage;
    }

    public void setInputMessage(String inputMessage) {
        this.inputMessage = inputMessage;
    }


    // ----------- Images -----------
    public List<ImageInfo> getImages() {
        return new ArrayList<>(imageInfos);
    }

    public boolean hasImages() {
        return imageInfos != null && !imageInfos.isEmpty();
    }

    public void addImage(String imagePath, String userId, String roomId, Insight insight) {
        if (imageInfos == null) {
        	imageInfos = new ArrayList<>();
        }
        ImageInfo imageData = ImageInfo.fromFile(imagePath, userId, roomId, messageId, insight.getInsightFolder());
        imageInfos.add(imageData);        
        
        // Clear cached formatted message when images change
        this.formattedMessage = null;
    }

    public void addImages(List<String> imagePaths, String userId, String roomId, Insight insight) {
        if (imagePaths != null) {
            for (String path : imagePaths) {
                addImage(path, userId, roomId, insight);
            }
        }
    }
    
	public void addImages(List<ImageInfo> imageInfos) {
		if (this.imageInfos == null) {
        	this.imageInfos = new ArrayList<>();
        }	
		for(ImageInfo image : imageInfos) {
		    this.imageInfos.add(image);        
		}

	}
	
    public List<ImageInfo> getImageInfos() {
        // Ensure insight folder is set
        if (insight != null) {
            for (ImageInfo info : imageInfos) {
                info.setInsightFolder(insight.getInsightFolder());
            }
        }
        return imageInfos;
    }

    public List<String> getImagesWithDataUrl() {
        List<String> urls = new ArrayList<>();
        if (imageInfos != null) {
            for (ImageInfo imageData : imageInfos) {
                urls.add(imageData.getFullDataUrl());
            }
        }
        return urls;
    }

    public List<String> getImagesBase64Only() {
        List<String> base64List = new ArrayList<>();
        if (imageInfos != null) {
            for (ImageInfo imageData : imageInfos) {
                base64List.add(imageData.getBase64Data());
            }
        }
        return base64List;
    }

    public List<String> getFormats() {
        List<String> formats = new ArrayList<>();
        if (imageInfos != null) {
            for (ImageInfo imageData : imageInfos) {
                formats.add(imageData.getFormat());
            }
        }
        return formats;
    }

    public List<String> getMimeTypes() {
        List<String> mimeTypes = new ArrayList<>();
        if (imageInfos != null) {
            for (ImageInfo imageData : imageInfos) {
                mimeTypes.add(imageData.getMimeType());
            }
        }
        return mimeTypes;
    }


    // ----------- Tools -----------
    public List<Map<String, Object>> getToolCalls() {
        return new ArrayList<>(toolCalls);
    }

    public boolean hasToolCalls() {
        return toolCalls != null && !toolCalls.isEmpty();
    }


    public void addToolCall(Map<String, Object> toolCallMap) {
        if (toolCalls == null) {
            toolCalls = new ArrayList<>();
        }
        toolCalls.add(toolCallMap);
        this.formattedMessage = null;
    }
    
    public String getToolCallId() {
		return toolCallId;
	}

	public void setToolCallId(String toolCallId) {
		this.toolCallId = toolCallId;
	}

	public String getToolName() {
		return toolName;
	}

	public void setToolName(String toolName) {
		this.toolName = toolName;
	}


    // ----------- Ornaments -----------
    public Map<String, Object> getOrnaments() {
        return new HashMap<>(ornaments);
    }
    
    public void setOrnament(String key, Object value) {
        if (ornaments == null) ornaments = new HashMap<>();
        ornaments.put(key, value);
    }
    public Object getOrnament(String key) {
        return ornaments != null ? ornaments.get(key) : null;
    }
    

    public Map<String, Object> getParamMap() {
		return paramMap;
	}

	public void setParamMap(Map<String, Object> paramMap) {
		this.paramMap = paramMap;
	}

	
    // Content type checks
    public boolean hasText() {
        return inputMessage != null && !inputMessage.isEmpty();
    }
    
    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    // Convenience factory methods for common cases
    public static InputMessage text(String content) {
        return builder().withInputMessage(content).withType(MessageType.INPUT_TEXT).build();
    }

    public static InputMessage response(String content) {
        return builder().withInputMessage(content).withType(MessageType.RESPONSE_TEXT).build();
    }

    public static InputMessage system(String content) {
        return builder().withInputMessage(content).withType(MessageType.SYSTEM).build();
    }
    
    public static InputMessage toolExecution(String toolCallId, String toolName, String content) {
         InputMessage toolExecution = builder()
            .withToolExecution(toolCallId, toolName, content)
            .withType(MessageType.INPUT_TOOL_EXEC)
            .build();
         toolExecution.setVisibile(false);
         return  toolExecution;
    }



	public static class Builder {
        private InputMessage message = new InputMessage();

        
        public Builder withInputMessage(String inputMessage) {
            message.setInputMessage(inputMessage);
            return this;
        }

        public Builder withInputPrompt(String inputPrompt) {
            message.setInputPrompt(inputPrompt);
            return this;
        }
        

        public Builder withType(MessageType type) {
            message.type = type;
            return this;
        }

        public Builder withTransactionId(String transactionId) {
            message.transactionId = transactionId;
            return this;
        }

        public Builder withModelType(ModelTypeEnum modelType) {
            message.modelType = modelType;
            return this;
        }

        public Builder withParamMap(Map<String, Object> paramMap) {
            if (paramMap != null) {
                message.setParamMap(new HashMap<>(paramMap));
            }
            return this;
        }

        public Builder withInsight(Insight insight) {
            message.insight = insight;
            return this;
        }

        public Builder withImage(String imagePath, String userId, String roomId, Insight insight) {
            message.addImage(imagePath, userId, roomId, insight);
            return this;
        }

        public Builder withImages(List<String> imagePaths, String userId, String roomId, Insight insight) {
            message.addImages(imagePaths, userId, roomId, insight);
            return this;
        }
        
		public Builder withImages(List<ImageInfo> imageInfos) {
			message.addImages(imageInfos);
			return this;
		}

		
        public Builder withToolCall(Map<String, Object> toolCallMap) {
            message.addToolCall(toolCallMap);
            return this;
        }
        public Builder withToolCalls(List<Map<String, Object>> toolCalls) {
            if (toolCalls != null) {
                for (Map<String, Object> tc : toolCalls) {
                    message.addToolCall(tc);
                }
            }
            return this;
        }
        
        public Builder withToolExecution(String toolCallId, String name, String content) {
            message.toolCallId = toolCallId;
            message.toolName = name;
            message.setInputMessage(content);
            message.setMessageType(MessageType.INPUT_TOOL_EXEC); // Ensure you have TOOL_INPUT or similar in your enum.
            return this;
        }


        public Builder withMetadata(String key, Object value) {
            message.setOrnament(key, value);
            return this;
        }

        public Builder withOrnaments(Map<String, Object> orn) {
            if (orn != null) {
                message.ornaments = new HashMap<>(orn);
            }
            return this;
        }

        public InputMessage build() {
            // Set default type if not specified
            if (message.type == null) {
                message.type = determineMessageType();
            }
            
            // If we have images but type isn't set to USER_IMAGE, update it
            if (message.hasImages() && message.type == MessageType.INPUT_TEXT) {
                message.type = MessageType.INPUT_MEDIA;
            }
            
            return message;
        }

        private MessageType determineMessageType() {
            if (message.hasImages()) {
                return MessageType.INPUT_MEDIA;
            }
            return MessageType.INPUT_TEXT;
        }

		public Builder withRAGChunks(List<Map<String, Object>> chunks) {
			message.setOrnament("chunks", chunks);
			return this;
		}


    }

}