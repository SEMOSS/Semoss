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
	@Deprecated
	private String inputUIPrompt;

	@Deprecated
	private String inputPrompt;

	@Deprecated
	private String systemPrompt = null;

	@SerializedName("type")
	@Deprecated
	private MessageType type = MessageType.INPUT_TEXT;

	@SerializedName("tool_call_id")
	@Deprecated
	private String toolCallId; // For tool result messages only

	@SerializedName("tool_name")
	@Deprecated
	private String toolName; // For tool result messages only
	
	@SerializedName("tool_status")
	@Deprecated
	private String toolStatus; // For tool result messages only

	@SerializedName("tool_parameter_values")
	@Deprecated
	private Map<String, Object> toolParameterValues; // For tool parameter values that produced the output

	private Map<String, Object> paramMap = new HashMap<>();
	
	@Deprecated
	private List<MessageInputMedia> mediaInputs = new ArrayList<>();

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
		ensureLegacyFromParts();
		return (inputPrompt == null || inputPrompt.trim().isEmpty()) ? inputUIPrompt : inputPrompt;
	}

	public void setInputPrompt(String inputPrompt) {
		this.inputPrompt = inputPrompt;
	}

	public String getInputUIPrompt() {
		ensureLegacyFromParts();
		return inputUIPrompt;
	}

	public void setInputUIPrompt(String inputMessage) {
		this.inputUIPrompt = inputMessage;
	}

	@Override
	public void normalizeAfterLoad(Room room) {
		super.normalizeAfterLoad(room);
		ensurePartsFromLegacy();
		ensureLegacyFromParts();

		if (hasMediaInputs() && room != null) {
			for (MessageInputMedia mediaInput : getMediaInfos()) {
				mediaInput.setRoomFolder(room.getRoomFolderPath());
				mediaInput.getBase64Data();
			}
		}
	}

	@Override
	public void normalizeForWrite() {
		if (io == null) {
			io = MessageIO.INPUT;
		}
		ensurePartsFromLegacy();
		ensureLegacyFromParts();
		super.normalizeForWrite();
	}

	private void ensurePartsFromLegacy() {
		if (hasParts()) {
			return;
		}

		// ---- text ----
		String effective = getInputPrompt();
		if (effective != null && !effective.trim().isEmpty()) {
			if (inputUIPrompt != null && !inputUIPrompt.equals(effective)) {
				addPart(new TextMessagePart(effective, inputUIPrompt));
			} else {
				addPart(new TextMessagePart(effective));
			}
		}

		// ---- system ----
		if (systemPrompt != null && !systemPrompt.trim().isEmpty()) {
			addPart(new SystemMessagePart(systemPrompt));
		}

		// ---- media ----
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				addPart(new MediaMessagePart(mediaInput));
			}
		}

		// ---- tool execution ----
		if (type == MessageType.INPUT_TOOL_EXEC || toolCallId != null || toolName != null) {
			addPart(new ToolResultMessagePart(
					new ToolResultPart(toolCallId, toolName, getInputPrompt(), toolParameterValues, toolStatus)));
		}
	}

	private void ensureLegacyFromParts() {
		if (!hasParts()) {
			return;
		}

		List<MessageInputMedia> derivedMedia = new ArrayList<>();
		String derivedText = null;
		String derivedUiText = null;
		String derivedSystem = null;
		ToolResultPart toolResultPart = null;

		for (MessagePart part : getParts()) {
			if (part == null || part.getType() == null) {
				continue;
			}
			if (part.getType() == MessagePartType.TEXT && derivedText == null) {
				if (part instanceof TextMessagePart) {
					derivedText = ((TextMessagePart) part).getText();
					derivedUiText = ((TextMessagePart) part).getUiText();
				}
			} else if (part.getType() == MessagePartType.SYSTEM && derivedSystem == null
					&& part instanceof SystemMessagePart) {
				derivedSystem = ((SystemMessagePart) part).getPrompt();
			} else if (part.getType() == MessagePartType.MEDIA) {
				if (part instanceof MediaMessagePart) {
					MessageInputMedia mediaInfo = ((MediaMessagePart) part).getMediaInfo();
					if (mediaInfo != null) {
						derivedMedia.add(mediaInfo);
					}
				}
			} else if (part.getType() == MessagePartType.TOOL_RESULT) {
				if (part instanceof ToolResultMessagePart) {
					toolResultPart = ((ToolResultMessagePart) part).getToolResult();
				}
			}
		}

		if (derivedText != null) {
			if (inputUIPrompt == null) {
				inputUIPrompt = (derivedUiText != null && !derivedUiText.isEmpty()) ? derivedUiText : derivedText;
			}
			if (inputPrompt == null) {
				inputPrompt = derivedText;
			}
		}

		if (derivedSystem != null && (systemPrompt == null || systemPrompt.isEmpty())) {
			systemPrompt = derivedSystem;
		}

		if (!derivedMedia.isEmpty() && (mediaInputs == null || mediaInputs.isEmpty())) {
			mediaInputs = derivedMedia;
		}

		if (toolResultPart != null) {
			type = MessageType.INPUT_TOOL_EXEC;
			if (toolCallId == null) {
				toolCallId = toolResultPart.getToolCallId();
			}
			if (toolName == null) {
				toolName = toolResultPart.getToolName();
			}
			if (toolStatus == null) {
				toolStatus = toolResultPart.getToolStatus();
			}
			if ((inputUIPrompt == null || inputUIPrompt.isEmpty())) {
				String output = toolResultPart.getOutput();
				if (output != null) {
					inputUIPrompt = output;
					inputPrompt = output;
				}
			}
		} else if (mediaInputs != null && !mediaInputs.isEmpty()) {
			if (type == null || type == MessageType.INPUT_TEXT) {
				type = MessageType.INPUT_MEDIA;
			}
		} else if (type == null) {
			type = MessageType.INPUT_TEXT;
		}
	}
	
	@Deprecated
	public String getToolStatus() {
		return toolStatus;
	}
	
	@Deprecated
	public void setToolStatus(String toolStatus) {
		this.toolStatus = toolStatus;
	}

	// ----------- Images -----------
	public List<MessageInputMedia> getMediaInputs() {
		ensureLegacyFromParts();
		return new ArrayList<>(mediaInputs);
	}

	public boolean hasMediaInputs() {
		return hasMediaPart() || (mediaInputs != null && !mediaInputs.isEmpty());
	}

	public void addMediaInput(String imagePath, Room room) {
		// Legacy API: now backed by parts
		if (room == null) {
			throw new IllegalArgumentException("Room cannot be null");
		}
		MessageInputMedia imageData = MessageInputMedia.fromFile(imagePath, room.getId(), messageId,
				room.getRoomFolderPath());
		addPart(new MediaMessagePart(imageData));
		ensureLegacyFromParts();
	}

	public void addMediaInput(List<String> inputPaths, Room room) {
		if (inputPaths != null) {
			for (String path : inputPaths) {
				addMediaInput(path, room);
			}
		}
	}

	public void addMediaInputs(List<MessageInputMedia> mediaInputs) {
	    if (mediaInputs != null && !mediaInputs.isEmpty()) {
	        for (MessageInputMedia m : mediaInputs) {
	            if (m != null) {
	                addPart(new MediaMessagePart(m));
	            }
	        }
	    }
	    ensureLegacyFromParts();
	}

	public void addMediaUrl(String url) {
		addPart(new MediaMessagePart(MessageInputMedia.fromUrl(url)));
		ensureLegacyFromParts();
	}

	public List<MessageInputMedia> getMediaInfos() {
		ensureLegacyFromParts();
		// Ensure insight folder is set
		if (room != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				mediaInput.setRoomFolder(room.getRoomFolderPath());
			}
		}
		return mediaInputs;
	}

	public List<String> getMediaWithDataUrl() {
		ensureLegacyFromParts();
		List<String> urls = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				urls.add(mediaInput.getFullDataUrl());
			}
		}
		return urls;
	}

	public List<String> getMediaBase64Only() {
		ensureLegacyFromParts();
		List<String> base64List = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				base64List.add(mediaInput.getBase64Data());
			}
		}
		return base64List;
	}

	public List<String> getFormats() {
		ensureLegacyFromParts();
		List<String> formats = new ArrayList<>();
		if (mediaInputs != null) {
			for (MessageInputMedia mediaInput : mediaInputs) {
				formats.add(mediaInput.getFileFormat());
			}
		}
		return formats;
	}

	public List<String> getMimeTypes() {
		ensureLegacyFromParts();
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
		if (hasToolCallPart()) {
			return true;
		}
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
		if (hasTextPart()) {
			return true;
		}
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
		return builder(room).withText(content).build();
	}

	public static InputMessage toolExecution(Room room, String toolCallId, String toolName, String content,
			Map<String, Object> toolParameterValues, String toolStatus) {
		InputMessage toolExecution = builder(room)
				.withToolResult(toolCallId, toolName, content, toolParameterValues, toolStatus).build();
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
			this.message.setRoom(room);
		}

		public Builder withText(String text) {
			if (text != null && !text.isEmpty()) {
				message.addPart(new TextMessagePart(text));
			}
			return this;
		}

		public Builder withText(String text, String uiText) {
			if (text != null && !text.isEmpty()) {
				message.addPart(new TextMessagePart(text, uiText));
			}
			return this;
		}

		public Builder withSystemPrompt(String prompt) {
			if (prompt != null && !prompt.isEmpty()) {
				message.addPart(new SystemMessagePart(prompt));
			}
			return this;
		}

		public Builder withType(MessageType type) {
			// Legacy-only: keep for compatibility, but behavior is driven by parts.
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
				for (String url : mediaUrls) {
					message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl(url)));
				}
			}
			return this;
		}

		/** Single URL convenience */
		public Builder withMediaUrl(String url) {
			if (url != null) {
				message.addPart(new MediaMessagePart(MessageInputMedia.fromUrl(url)));
			}
			return this;
		}

		public Builder withTool(Map<String, Object> toolCallMap) {
			message.addTool(toolCallMap);
			if (toolCallMap != null) {
				List<Map<String, Object>> one = new ArrayList<>();
				one.add(toolCallMap);
				message.addPart(new ToolCallMessagePart(one));
			}
			return this;
		}

		public Builder withTools(List<Map<String, Object>> toolCalls) {
			if (toolCalls != null) {
				for (Map<String, Object> tc : toolCalls) {
					message.addTool(tc);
				}
				message.addPart(new ToolCallMessagePart(toolCalls));
			}
			return this;
		}

		public Builder withToolResult(String toolCallId, String name, String content,
				Map<String, Object> toolParameterValues, String toolStatus) {
			message.addPart(new ToolResultMessagePart(
					new ToolResultPart(toolCallId, name, content, toolParameterValues, toolStatus)));
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
			if (message.getRoom() == null) {
				throw new IllegalStateException("Room must be set before building InputMessage");
			}

			message.normalizeForWrite();
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
		ensureLegacyFromParts();
		return systemPrompt;
	}

	@Deprecated
	public void setSystemPrompt(String prompt) {
		systemPrompt = prompt;
		if (prompt != null && !prompt.isEmpty()) {
			addPart(new SystemMessagePart(prompt));
		}
	}
}
