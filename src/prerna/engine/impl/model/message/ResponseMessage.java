/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.responses.AskImageModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;

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

		for (MessagePart part : getParts()) {
			if (MessagePartType.MEDIA == part.type) {
				MediaMessagePart mediaMessagePart = (MediaMessagePart) part;
				if (room != null) {
					mediaMessagePart.getMediaInfo().setRoomFolder(room.getRoomFolderPath());
				}
				mediaMessagePart.getMediaInfo().getBase64Data();
			}
		}
	}

	@Override
	public void normalizeForWrite() {
		if (io == null) {
			io = MessageIO.OUTPUT;
		}
		ensurePartsFromLegacy();
		ensureLegacyFromParts();
		super.normalizeForWrite();
		stripLegacyFieldsForWrite();
	}

	/**
	 * When persisting schema v2+ messages, avoid writing the legacy flat fields.
	 * They can always be re-derived from {@code parts} on load.
	 */
	private void stripLegacyFieldsForWrite() {
		if (schemaVersion == null || schemaVersion < LATEST_SCHEMA_VERSION) {
			return;
		}
		this.content = null;
		this.thinking = null;
		this.toolResponses = null;
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
			for (Map<String, Object> toolCall : toolResponses) {
				addPart(new ToolCallMessagePart(toolCall));
			}
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
		boolean hasMedia = false;

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
			} else if (part.getType() == MessagePartType.MEDIA) {
				hasMedia = true;
			}
		}

		derivedToolCalls = this.getToolResponses();

		if (derivedText != null && (content == null || content.isEmpty())) {
			content = derivedText;
		}
		if (derivedThinking != null && (thinking == null || thinking.isEmpty())) {
			thinking = derivedThinking;
		}
		if (derivedToolCalls != null && !derivedToolCalls.isEmpty()
				&& (toolResponses == null || toolResponses.isEmpty())) {
			toolResponses = new ArrayList<>(derivedToolCalls);
		}

		MessagePart lastPart = parts.getLast();
		if (lastPart.getType() == MessagePartType.TOOL_CALL) {
			type = MessageType.RESPONSE_TOOL;
		} else if (lastPart.getType() == MessagePartType.MEDIA) {
			type = MessageType.RESPONSE_MEDIA;
		} else {
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
		if (hasParts()) {
			for (MessagePart part : getParts()) {
				if (part instanceof TextMessagePart) {
					String text = ((TextMessagePart) part).getText();
					if (text != null) {
						return text;
					}
				}
			}
		}
		return content;
	}

	public String getThinking() {
		if (hasParts()) {
			for (MessagePart part : getParts()) {
				if (part instanceof ThinkingMessagePart) {
					String t = ((ThinkingMessagePart) part).getThinking();
					if (t != null) {
						return t;
					}
				}
			}
		}
		return thinking;
	}

	public List<Map<String, Object>> getToolResponses() {
		if (hasParts()) {
			List<Map<String, Object>> allTools = new ArrayList<>();
			for (MessagePart part : getParts()) {
				if (part instanceof ToolCallMessagePart) {
					allTools.add(((ToolCallMessagePart) part).getToolCall());
				}
			}
			if (!allTools.isEmpty()) {
				return allTools;
			}
		}
		return toolResponses != null ? new ArrayList<>(toolResponses) : new ArrayList<>();
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
				for (Map<String, Object> toolCall : toolResponses) {
					message.addPart(new ToolCallMessagePart(toolCall));
				}
			}
			return this;
		}

		public Builder withToolResponse(Map<String, Object> toolResponse) {
			if (toolResponse != null) {
				message.addPart(new ToolCallMessagePart(toolResponse));
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

			builder.message.setCacheCreationTokens(llmResponse.getNumberOfCacheCreationTokens());
			builder.message.setThinkingTokens(llmResponse.getNumberOfThinkingTokens());

			// Prefer parts-based responses, fall back to legacy messageType.
			if (llmResponse.getParts() != null && !llmResponse.getParts().isEmpty()) {
				for (MessagePart part : llmResponse.getParts()) {
					if (part != null) {
						builder.message.addPart(part);
					}
				}

				// Set a legacy type hint for older code paths based on the last part
				MessagePart lastPart = llmResponse.getParts().getLast();
				if (lastPart.getType() == MessagePartType.TOOL_CALL) {
					builder.withType(MessageType.RESPONSE_TOOL);
				} else if (lastPart.getType() == MessagePartType.MEDIA) {
					builder.withType(MessageType.RESPONSE_MEDIA);
				} else {
					builder.withType(MessageType.RESPONSE_TEXT);
				}
				return builder;
			}

			String messageType = llmResponse.getMessageType();
			if (AskModelEngineResponse.CHAT.equals(messageType)) {
				builder.withText(llmResponse.getStringResponse()).withType(MessageType.RESPONSE_TEXT);
				if (llmResponse.getThinking() != null) {
					builder.withThinking(llmResponse.getThinking());
				}
				return builder;
			}

			if (AskModelEngineResponse.TOOL.equals(messageType)) {
				if (llmResponse instanceof AskToolModelEngineResponse) {
					List<Map<String, Object>> toolReps = ((AskToolModelEngineResponse) llmResponse).getToolResponse();
					builder.withToolResponses(toolReps).withType(MessageType.RESPONSE_TOOL);
				} else {
					builder.withType(MessageType.RESPONSE_TOOL);
				}
				if (llmResponse.getThinking() != null) {
					builder.withThinking(llmResponse.getThinking());
				}
				return builder;
			}

			if (AskModelEngineResponse.IMAGE.equals(messageType)) {
				if (llmResponse instanceof AskImageModelEngineResponse) {
					String[] imgs = ((AskImageModelEngineResponse) llmResponse).getImages();
					builder.withText(String.join(",", imgs)).withType(MessageType.RESPONSE_MEDIA);
				} else {
					builder.withType(MessageType.RESPONSE_MEDIA);
				}
				if (llmResponse.getThinking() != null) {
					builder.withThinking(llmResponse.getThinking());
				}
				return builder;
			}

			builder.withText(llmResponse.getStringResponse()).withType(MessageType.RESPONSE_TEXT);
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
		return builder().withToolResponse(toolResponse).build();
	}

}
