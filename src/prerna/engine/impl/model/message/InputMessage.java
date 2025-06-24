package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.Room;

/**
 * Unified message class that can handle all types of messages (text, image, tool calls, etc.)
 * 
 * Builder requires Room; you cannot use it without.
 */
public class InputMessage extends AbstractMessage {

    @SerializedName("inputUIPrompt")
    private String inputUIPrompt;

    private String inputPrompt;

    @SerializedName("type")
    private MessageType type = MessageType.INPUT_TEXT;

    @SerializedName("tool_call_id")
    private String toolCallId;   // For tool result messages only

    @SerializedName("tool_name")
    private String toolName;     // For tool result messages only

    @SerializedName("ornaments")
    private Map<String, Object> ornaments = new HashMap<>();

    private Map<String, Object> paramMap = new HashMap<>();
    private List<ImageInfo> imageInfos = new ArrayList<>();
    // Make room package-private for builder, private for rest
    Room room;

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
    }

    /** Get the effective prompt to send to the LLM (RAG: includes user + chunks). */
    public String getInputPrompt() {
        return (inputPrompt == null || inputPrompt.trim().isEmpty())
                ? inputUIPrompt
                : inputPrompt;
    }

    public void setInputPrompt(String inputPrompt) {
        this.inputPrompt = inputPrompt;
    }

    public String getInputUIPrompt() {
        return inputUIPrompt;
    }

    public void setInputUIPrompt(String inputMessage) {
        this.inputUIPrompt = inputMessage;
    }

    // ----------- Images -----------
    public List<ImageInfo> getImages() {
        return new ArrayList<>(imageInfos);
    }

    public boolean hasImages() {
        return imageInfos != null && !imageInfos.isEmpty();
    }

    public void addImage(String imagePath, Room room) {
        if (imageInfos == null) {
            imageInfos = new ArrayList<>();
        }
        ImageInfo imageData = ImageInfo.fromFile(imagePath, room.getId(), messageId, room.getRoomFolderPath());
        imageInfos.add(imageData);

        ClusterUtil.pushRoom(room.getId());
        // this.formattedMessage = null;
    }

    public void addImages(List<String> imagePaths, Room room) {
        if (imagePaths != null) {
            for (String path : imagePaths) {
                addImage(path, room);
            }
        }
    }

    public void addImages(List<ImageInfo> imageInfos) {
        if (this.imageInfos == null) {
            this.imageInfos = new ArrayList<>();
        }
        if (imageInfos != null) {
            this.imageInfos.addAll(imageInfos);
        }
    }
    
    public void addImageUrl(String url) {
        if (imageInfos == null) imageInfos = new ArrayList<>();
        imageInfos.add(ImageInfo.fromUrl(url));
    }

    public List<ImageInfo> getImageInfos() {
        // Ensure insight folder is set
        if (room != null) {
            for (ImageInfo info : imageInfos) {
                info.setRoomFolder(room.getRoomFolderPath());
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
                formats.add(imageData.getFileFormat());
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
    public List<Map<String, Object>> getTools() {
        Object value = paramMap.get("tools");
        if (value instanceof List) {
            return new ArrayList<>((List<Map<String, Object>>) value);
        } else {
            return new ArrayList<>();
        }
    }

    public boolean hasToolCalls() {
        Object value = paramMap.get("tools");
        return value instanceof List && !((List<?>) value).isEmpty();
    }

    public void addTool(Map<String, Object> toolCallMap) {
        List<Map<String, Object>> toolCalls = (List<Map<String, Object>>) paramMap.get("tools");
        if (toolCalls == null) {
            toolCalls = new ArrayList<>();
            paramMap.put("tools", toolCalls);
        }
        toolCalls.add(toolCallMap);
        // this.formattedMessage = null;
    }

    public void setTools(List<Map<String, Object>> toolCalls) {
        paramMap.put("tools", toolCalls);
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
        return inputUIPrompt != null && !inputUIPrompt.isEmpty();
    }

    // ----------- Builder pattern (APPROACH 2) -----------
    public static Builder builder(Room room) {
        return new Builder(room);
    }

    /**
     * Convenience factory: must provide a Room.
     */
    public static InputMessage text(Room room, String content) {
        return builder(room).withInputUIPrompt(content).withType(MessageType.INPUT_TEXT).build();
    }
    public static InputMessage response(Room room, String content) {
        return builder(room).withInputUIPrompt(content).withType(MessageType.RESPONSE_TEXT).build();
    }
    public static InputMessage system(Room room, String content) {
        return builder(room).withInputUIPrompt(content).withType(MessageType.SYSTEM).build();
    }
    public static InputMessage toolExecution(Room room, String toolCallId, String toolName, String content) {
        InputMessage toolExecution = builder(room)
            .withToolExecution(toolCallId, toolName, content)
            .withType(MessageType.INPUT_TOOL_EXEC)
            .build();
        toolExecution.setVisibile(false);
        return toolExecution;
    }

    public static class Builder {
        private final InputMessage message;

        public Builder(Room room) {
            if (room == null) throw new IllegalArgumentException("Room cannot be null");
            this.message = new InputMessage();
            this.message.room = room;
        }

        public Builder withInputUIPrompt(String inputMessage) {
            message.setInputUIPrompt(inputMessage);
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

        public Builder withImage(String imagePath, Room room) {
            message.addImage(imagePath, room);
            return this;
        }

        public Builder withImages(List<String> imagePaths, Room room) {
            message.addImages(imagePaths, room);
            return this;
        }

        public Builder withImages(List<ImageInfo> imageInfos) {
            message.addImages(imageInfos);
            return this;
        }
        
        
        /** Accept list of image URLs (for direct image references) */
        public Builder withImageUrls(List<String> imageUrls) {
            if (imageUrls != null) {
                List<ImageInfo> byUrl = new ArrayList<>();
                for (String url : imageUrls) {
                    byUrl.add(ImageInfo.fromUrl(url));
                }
                message.addImages(byUrl);
            }
            return this;
        }
        /** Single URL convenience */
        public Builder withImageUrl(String url) {
            if (url != null) {
                message.addImages(Collections.singletonList(ImageInfo.fromUrl(url)));
            }
            return this;
        }
        

        public Builder withTool(Map<String, Object> toolCallMap) {
            message.addTool(toolCallMap);
            return this;
        }

        public Builder withTools(List<Map<String, Object>> toolCalls) {
            if (toolCalls != null) {
                for (Map<String, Object> tc : toolCalls) {
                    message.addTool(tc);
                }
            }
            return this;
        }

        public Builder withToolExecution(String toolCallId, String name, String content) {
            message.toolCallId = toolCallId;
            message.toolName = name;
            message.setInputUIPrompt(content);
            message.setMessageType(MessageType.INPUT_TOOL_EXEC);
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

        public Builder withRAGChunks(List<Map<String, Object>> chunks) {
            message.setOrnament("chunks", chunks);
            return this;
        }

        public InputMessage build() {
            if (message.room == null) {
                throw new IllegalStateException("Room must be set before building InputMessage");
            }

            // Set default type if not specified
            if (message.type == null) {
                message.type = determineMessageType();
            }

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
    }
}