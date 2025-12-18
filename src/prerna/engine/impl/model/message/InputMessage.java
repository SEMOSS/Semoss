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
 * Unified message class that can handle all types of messages (text, image,
 * tool calls, etc.)
 * 
 * Builder requires Room; you cannot use it without.
 */
public class InputMessage extends AbstractMessage {

	@SerializedName("inputUIPrompt")
	private String inputUIPrompt;

	private String inputPrompt;

	private String systemPrompt = null;

	@SerializedName("type")
	private MessageType type = MessageType.INPUT_TEXT;

	@SerializedName("tool_call_id")
	private String toolCallId; // For tool result messages only

	@SerializedName("tool_name")
	private String toolName; // For tool result messages only

	@SerializedName("tool_parameter_values")
	private Map<String, Object> toolParameterValues; // For tool parameter values that produced the output
	
	private Boolean cancelledTool;

	private Map<String, Object> paramMap = new HashMap<>();
	private List<MessageInputMedia> mediaInputs = new ArrayList<>();

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

	/**
	 * Get the effective prompt to send to the LLM (RAG: includes user + chunks).
	 */
	public String getInputPrompt() {
		return (inputPrompt == null || inputPrompt.trim().isEmpty()) ? inputUIPrompt : inputPrompt;
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
	public List<MessageInputMedia> getMediaInputs() {
		return new ArrayList<>(mediaInputs);
	}

	public boolean hasMediaInputs() {
		return mediaInputs != null && !mediaInputs.isEmpty();
	}

	public void addMediaInput(String imagePath, Room room) {
		if (mediaInputs == null) {
			mediaInputs = new ArrayList<>();
		}
		MessageInputMedia imageData = MessageInputMedia.fromFile(imagePath, room.getId(), messageId,
				room.getRoomFolderPath());
		mediaInputs.add(imageData);

		ClusterUtil.pushRoom(room.getId());
	}

	public void addMediaInput(List<String> inputPaths, Room room) {
		if (inputPaths != null) {
			for (String path : inputPaths) {
				addMediaInput(path, room);
			}
		}
	}

	public void addMediaInputs(List<MessageInputMedia> mediaInputs) {
	    if (this.mediaInputs == null) {
	        this.mediaInputs = new ArrayList<>();
	    }
	    if (mediaInputs != null && !mediaInputs.isEmpty()) {
	        for (MessageInputMedia m : mediaInputs) {
	            if (m != null) {
	                this.mediaInputs.add(m);
	            }
	        }
	    }
	}

	public void addMediaUrl(String url) {
		if (mediaInputs == null) {
			mediaInputs = new ArrayList<>();
		}
		mediaInputs.add(MessageInputMedia.fromUrl(url));
	}

	public List<MessageInputMedia> getMediaInfos() {
		// Ensure insight folder is set
		if (room != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				mediaInput.setRoomFolder(room.getRoomFolderPath());
			}
		}
		return mediaInputs;
	}

	public List<String> getMediaWithDataUrl() {
		List<String> urls = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				urls.add(mediaInput.getFullDataUrl());
			}
		}
		return urls;
	}

	public List<String> getMediaBase64Only() {
		List<String> base64List = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				base64List.add(mediaInput.getBase64Data());
			}
		}
		return base64List;
	}

	public List<String> getFormats() {
		List<String> formats = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				formats.add(mediaInput.getFileFormat());
			}
		}
		return formats;
	}

	public List<String> getMimeTypes() {
		List<String> mimeTypes = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				mimeTypes.add(mediaInput.getMimeType());
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

	public void setToolParameterValues(Map<String, Object> toolParameterValues) {
		this.toolParameterValues = toolParameterValues;
	}

	public Map<String, Object> getToolParameterValues() {
		return toolParameterValues;
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

	// ----------- Builder pattern -----------
	public static Builder builder(Room room) {
		return new Builder(room);
	}

	/**
	 * Convenience factory: must provide a Room.
	 */
	public static InputMessage text(Room room, String content) {
		return builder(room).withInputUIPrompt(content).withType(MessageType.INPUT_TEXT).build();
	}

	public static InputMessage toolExecution(Room room, String toolCallId, String toolName, String content,
			Map<String, Object> toolParameterValues, Boolean cancelledTool) {
		InputMessage toolExecution = builder(room).withToolExecution(toolCallId, toolName, content, toolParameterValues, cancelledTool)
				.withType(MessageType.INPUT_TOOL_EXEC).build();
		toolExecution.setVisibile(false);
		return toolExecution;
	}

	public static class Builder {
		private final InputMessage message;

		public Builder(Room room) {
			if (room == null) {
				throw new IllegalArgumentException("Room cannot be null");
			}
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

		public Builder withSystemPrompt(String prompt) {
			message.setSystemPrompt(prompt);
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

		public Builder withMediaInput(String mediaInputPath, Room room) {
			message.addMediaInput(mediaInputPath, room);
			return this;
		}

		public Builder withMediaInputs(List<String> mediaInputPaths, Room room) {
			message.addMediaInput(mediaInputPaths, room);
			return this;
		}

		public Builder withMediaInputs(List<MessageInputMedia> mediaInputs) {
			message.addMediaInputs(mediaInputs);
			return this;
		}

		/** Accept list of image URLs (for direct image references) */
		public Builder withMediaUrls(List<String> mediaUrls) {
			if (mediaUrls != null) {
				List<MessageInputMedia> byUrl = new ArrayList<>();
				for (String url : mediaUrls) {
					byUrl.add(MessageInputMedia.fromUrl(url));
				}
				message.addMediaInputs(byUrl);
			}
			return this;
		}

		/** Single URL convenience */
		public Builder withMediaUrl(String url) {
			if (url != null) {
				message.addMediaInputs(Collections.singletonList(MessageInputMedia.fromUrl(url)));
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

		public Builder withToolExecution(String toolCallId, String name, String content,
				Map<String, Object> toolParameterValues, Boolean cancelledTool) {
			message.toolCallId = toolCallId;
			message.toolName = name;
			message.toolParameterValues = toolParameterValues;
			message.setInputUIPrompt(content);
			message.setInputPrompt(content);
			message.setMessageType(MessageType.INPUT_TOOL_EXEC);
			message.cancelledTool = cancelledTool;
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

			if (message.hasMediaInputs() && message.type == MessageType.INPUT_TEXT) {
				message.type = MessageType.INPUT_MEDIA;
			}
			return message;
		}

		private MessageType determineMessageType() {
			if (message.hasMediaInputs()) {
				return MessageType.INPUT_MEDIA;
			}
			return MessageType.INPUT_TEXT;
		}
	}

	public String getSystemPrompt() {
		return systemPrompt;
	}

	public void setSystemPrompt(String prompt) {
		systemPrompt = prompt;
	}
}