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
package prerna.engine.impl.model.responses;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.TextMessagePart;
import prerna.engine.impl.model.message.ThinkingMessagePart;
import prerna.engine.impl.model.message.ToolCallMessagePart;

/**
 * A single per-request result within a completed batch, keyed by custom_id.
 * On success the provider content blocks are converted to a SEMOSS ResponseMessage
 * (same shape as AskPlayground's responseMessage field).
 */
public class BatchResultItem {

	private static final Gson GSON = new Gson();

	private String customId;
	private boolean ok;
	/** Raw content blocks from Python: {role, content:[{type,text|thinking|...}]} */
	private Object message;
	private Object error;
	private Integer inputTokens;
	private Integer outputTokens;
	/** Original prompt text, populated at results-fetch time from the submission log. */
	private String inputText;

	public String getCustomId() {
		return customId;
	}

	public boolean isOk() {
		return ok;
	}

	public Object getMessage() {
		return message;
	}

	/**
	 * Extract the first text block for logging/storage.
	 */
	@SuppressWarnings("unchecked")
	public String getFirstTextContent() {
		if (!(message instanceof Map)) {
			return null;
		}
		Object content = ((Map<String, Object>) message).get("content");
		if (!(content instanceof List)) {
			return null;
		}
		for (Object block : (List<Object>) content) {
			if (block instanceof Map) {
				Map<String, Object> b = (Map<String, Object>) block;
				if ("text".equals(b.get("type"))) {
					Object t = b.get("text");
					if (t != null) {
						return t.toString();
					}
				}
			}
		}
		return null;
	}

	public Object getError() {
		return error;
	}

	public Integer getInputTokens() {
		return inputTokens;
	}

	public Integer getOutputTokens() {
		return outputTokens;
	}

	public String getInputText() {
		return inputText;
	}

	public void setInputText(String inputText) {
		this.inputText = inputText;
	}

	/**
	 * Convert content blocks to a SEMOSS ResponseMessage (same as AskPlayground).
	 * Returns null if the message is null or malformed.
	 */
	@SuppressWarnings("unchecked")
	private ResponseMessage toResponseMessage() {
		if (!(message instanceof Map)) {
			return null;
		}
		Object contentObj = ((Map<String, Object>) message).get("content");
		if (!(contentObj instanceof List)) {
			return null;
		}
		ResponseMessage.Builder builder = ResponseMessage.builder();
		boolean hasToolCall = false;
		for (Object block : (List<Object>) contentObj) {
			if (!(block instanceof Map)) {
				continue;
			}
			Map<String, Object> b = (Map<String, Object>) block;
			String type = (String) b.get("type");
			if ("text".equals(type)) {
				String text = (String) b.get("text");
				if (text != null) {
					builder.withText(text);
				}
			} else if ("tool_use".equals(type)) {
				hasToolCall = true;
				Map<String, Object> toolCall = new HashMap<>();
				toolCall.put("id", b.get("id"));
				toolCall.put("name", b.get("name"));
				toolCall.put("input", b.get("input"));
				builder.withToolResponse(toolCall);
			} else if ("thinking".equals(type)) {
				String thinking = (String) b.get("thinking");
				if (thinking != null) {
					builder.withThinking(thinking);
				}
			}
		}
		builder.withType(hasToolCall ? MessageType.RESPONSE_TOOL : MessageType.RESPONSE_TEXT);
		return builder.build();
	}

	public Map<String, Object> toMap() {
		Map<String, Object> out = new HashMap<>();
		out.put("customId", customId);
		out.put("status", ok ? "succeeded" : "errored");
		if (inputText != null) {
			out.put("inputText", inputText);
		}
		out.put("numberOfTokensInPrompt", inputTokens);
		out.put("numberOfTokensInResponse", outputTokens);
		if (ok && message != null) {
			ResponseMessage responseMessage = toResponseMessage();
			if (responseMessage != null) {
				String json = MessageUtils.toJson(responseMessage);
				Map<String, Object> responseMap = GSON.fromJson(json,
						new TypeToken<Map<String, Object>>() {}.getType());
				out.put("responseMessage", responseMap);
			}
		}
		if (!ok && error != null) {
			out.put("error", error);
		}
		return out;
	}

	public static BatchResultItem fromMap(Map<String, Object> map) {
		BatchResultItem r = new BatchResultItem();
		r.customId = BatchModelEngineResponseUtil.getString(map, "custom_id");
		r.ok = BatchModelEngineResponseUtil.getBoolean(map.get("ok"));
		r.message = map.get("message");
		r.error = map.get("error");
		r.inputTokens = BatchModelEngineResponseUtil.getInteger(map.get("input_tokens"));
		r.outputTokens = BatchModelEngineResponseUtil.getInteger(map.get("output_tokens"));
		return r;
	}
}
