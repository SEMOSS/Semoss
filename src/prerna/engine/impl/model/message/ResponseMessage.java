package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import prerna.engine.impl.model.responses.AskImageModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.engine.impl.model.Room;

public class ResponseMessage extends AbstractMessage {

	@SerializedName("content")
	@Deprecated
	private String content;

	@SerializedName("thinking")
	@Deprecated
	private String thinking;

	@SerializedName("type")
	@Deprecated
	private MessageType type = MessageType.RESPONSE_TEXT;

	@SerializedName("tool_responses")
	@Deprecated
	private List<Map<String, Object>> toolResponses = new ArrayList<>();

	private transient AskModelEngineResponse<?> modelEngineResponse;

	private ResponseMessage() {
		super();
	}

	@Override
	public void normalizeAfterLoad(Room room) {
		super.normalizeAfterLoad(room);
		ensurePartsFromLegacy();
		ensureLegacyFromParts();
	}

	@Override
	public void normalizeForWrite() {
		if (io == null) {
			io = MessageIO.OUTPUT;
		}
		ensurePartsFromLegacy();
		ensureLegacyFromParts();
		super.normalizeForWrite();
	}

	private void ensurePartsFromLegacy() {
		if (hasParts()) {
			return;
		}

		if (content != null && !content.trim().isEmpty()) {
			addPart(new TextMessagePart(content));
		}
		if (thinking != null && !thinking.trim().isEmpty()) {
			addPart(new ThinkingMessagePart(thinking));
		}
		if (toolResponses != null && !toolResponses.isEmpty()) {
			addPart(new ToolCallMessagePart(toolResponses));
		}
	}

	@SuppressWarnings("unchecked")
	private void ensureLegacyFromParts() {
		if (!hasParts()) {
			return;
		}

		String derivedText = null;
		String derivedThinking = null;
		List<Map<String, Object>> derivedToolCalls = null;

		for (MessagePart part : getParts()) {
			if (part == null || part.getType() == null) {
				continue;
			}
			if (part.getType() == MessagePartType.TEXT && derivedText == null) {
				if (part instanceof TextMessagePart) {
					derivedText = ((TextMessagePart) part).getText();
				}
			} else if (part.getType() == MessagePartType.THINKING && derivedThinking == null) {
				if (part instanceof ThinkingMessagePart) {
					derivedThinking = ((ThinkingMessagePart) part).getThinking();
				}
			} else if (part.getType() == MessagePartType.TOOL_CALL && derivedToolCalls == null) {
				if (part instanceof ToolCallMessagePart) {
					derivedToolCalls = ((ToolCallMessagePart) part).getToolCalls();
				}
			}
		}

		if (derivedText != null && (content == null || content.isEmpty())) {
			content = derivedText;
		}
		if (derivedThinking != null && (thinking == null || thinking.isEmpty())) {
			thinking = derivedThinking;
		}
		if (derivedToolCalls != null && (toolResponses == null || toolResponses.isEmpty())) {
			toolResponses = new ArrayList<>(derivedToolCalls);
		}

		if (toolResponses != null && !toolResponses.isEmpty()) {
			type = MessageType.RESPONSE_TOOL;
		} else if (type == null) {
			type = MessageType.RESPONSE_TEXT;
		}
	}

	public AskModelEngineResponse<?> getModelEngineResponse() {
		return modelEngineResponse;
	}

	public void setModelEngineResponse(AskModelEngineResponse<?> resp) {
		this.modelEngineResponse = resp;
	}

	@Override
	public MessageType getMessageType() {
		return type;
	}

	public String getContent() {
		ensureLegacyFromParts();
		return content;
	}

	public String getThinking() {
		ensureLegacyFromParts();
		return thinking;
	}

	public List<Map<String, Object>> getToolResponses() {
		ensureLegacyFromParts();
		return new ArrayList<>(toolResponses);
	}

	public boolean hasToolResponses() {
		return hasToolCallPart() || (toolResponses != null && !toolResponses.isEmpty());
	}

	public void setContent(String content) {
		this.content = content;
	}

	public void setThinking(String thinking) {
		this.thinking = thinking;
	}

	public void setMessageType(MessageType type) {
		this.type = type;
	}

	public void setToolResponses(List<Map<String, Object>> toolResponses) {
		if (toolResponses == null) {
			this.toolResponses = new ArrayList<>();
		} else {
			this.toolResponses = new ArrayList<>(toolResponses);
		}
		this.type = MessageType.RESPONSE_TOOL;
	}

	// --- Builder pattern for ResponseMessage
	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private final ResponseMessage message = new ResponseMessage();

		public Builder withText(String content) {
			if (content != null && !content.isEmpty()) {
				message.addPart(new TextMessagePart(content));
			}
			return this;
		}

		public Builder withType(MessageType type) {
			// Legacy-only: keep for compatibility, but behavior is driven by parts.
			message.type = type;
			return this;
		}

		public Builder withToolResponses(List<Map<String, Object>> toolResponses) {
			if (toolResponses != null && !toolResponses.isEmpty()) {
				message.addPart(new ToolCallMessagePart(toolResponses));
			}
			return this;
		}

		public Builder addToolResponse(Map<String, Object> toolResponse) {
			if (toolResponse != null) {
				List<Map<String, Object>> one = new ArrayList<>();
				one.add(toolResponse);
				message.addPart(new ToolCallMessagePart(one));
			}
			return this;
		}

		public Builder withModelEngineResponse(AskModelEngineResponse<?> response) {
			message.modelEngineResponse = response;
			return this;
		}

		public Builder withRAGChunks(List<Map<String, Object>> chunks) {
			message.setOrnament("chunks", chunks);
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

		public Builder withThinking(String thinking) {
			if (thinking != null && !thinking.isEmpty()) {
				message.addPart(new ThinkingMessagePart(thinking));
			}
			return this;
		}

		public static Builder fromAskModelEngineResponse(AskModelEngineResponse<?> llmResponse) {

			if (llmResponse == null) {
				throw new IllegalArgumentException("AskModelEngineResponse is null.");
			}

			Builder builder = new Builder();
			builder.withModelEngineResponse(llmResponse);

			String messageType = llmResponse.getMessageType();
			if (AskModelEngineResponse.CHAT.equals(messageType)) {
				builder.withText(llmResponse.getStringResponse()).withType(MessageType.RESPONSE_TEXT);
			} else if (AskModelEngineResponse.TOOL.equals(messageType)) {
				// tool response handling
				if (llmResponse instanceof AskToolModelEngineResponse) {
					List<Map<String, Object>> toolReps = ((AskToolModelEngineResponse) llmResponse).getToolResponse();
					builder.withToolResponses(toolReps);
				} else {
					builder.withText("No tool response").withType(MessageType.RESPONSE_TOOL);
				}
			} else if (AskModelEngineResponse.IMAGE.equals(messageType)) {

				// TODO build this out
				if (llmResponse instanceof AskImageModelEngineResponse) {
					String[] imgs = ((AskImageModelEngineResponse) llmResponse).getImages();
					builder.withText(String.join(",", imgs)).withType(MessageType.RESPONSE_MEDIA);
				} else {
					builder.withText("No image response").withType(MessageType.RESPONSE_MEDIA);
				}
			} else {
				builder.withText("null").withType(MessageType.RESPONSE_TEXT);
			}
			
			if (llmResponse.getThinking() != null) {
				builder.withThinking(llmResponse.getThinking());
			}
			return builder;
		}

		public ResponseMessage build() {
			if (message.type == null) {
				message.type = MessageType.RESPONSE_TEXT;
			}
			// Prefer parts-based schema; keep legacy fields derived for compatibility.
			message.normalizeForWrite();
			return message;
		}
	}

	// Some factory/convenience methods
	public static ResponseMessage text(String content, AskModelEngineResponse<?> resp) {
		return builder().withText(content).withType(MessageType.RESPONSE_TEXT).withModelEngineResponse(resp).build();
	}

	public static ResponseMessage toolResponses(List<Map<String, Object>> toolResponses,
			AskModelEngineResponse<?> resp) {
		return builder().withToolResponses(toolResponses).withModelEngineResponse(resp).build();
	}


	// Or legacy factories if you want them (w/o model response)
	public static ResponseMessage text(String content) {
		return builder().withText(content).withType(MessageType.RESPONSE_TEXT).build();
	}

	public static ResponseMessage toolResponses(List<Map<String, Object>> toolResponses) {
		return builder().withToolResponses(toolResponses).build();
	}

	public static ResponseMessage toolResponse(Map<String, Object> toolResponse) {
		List<Map<String, Object>> toolResponses = new ArrayList<>();
		toolResponses.add(toolResponse);
		return builder().withToolResponses(toolResponses).build();
	}

}
