package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import prerna.engine.impl.model.message.InputMessage.Builder;
import prerna.engine.impl.model.responses.AskImageModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;

public class ResponseMessage extends AbstractMessage {
	@SerializedName("content")
	private String content;

	@SerializedName("type")
	private MessageType type = MessageType.RESPONSE_TEXT;

	@SerializedName("tool_responses")
	private List<Map<String, Object>> toolResponses = new ArrayList<>();

	private transient AskModelEngineResponse<?> modelEngineResponse;

	private ResponseMessage() {
		super();
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
			return builder;
		}

		public ResponseMessage build() {
			if (message.type == null) {
				message.type = MessageType.RESPONSE_TEXT;
			}
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

	public static ResponseMessage system(String content, AskModelEngineResponse<?> resp) {
		return builder().withText(content).withType(MessageType.SYSTEM).withModelEngineResponse(resp).build();
	}

	// Or legacy factories if you want them (w/o model response)
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