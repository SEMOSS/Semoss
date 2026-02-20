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

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class MessagePartAdapter implements JsonSerializer<MessagePart>, JsonDeserializer<MessagePart> {

	@Override
	public JsonElement serialize(MessagePart src, Type typeOfSrc, JsonSerializationContext context) {
		if (src == null) {
			return null;
		}
		return context.serialize(src, src.getClass());
	}

	@Override
	public MessagePart deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		if (json == null || !json.isJsonObject()) {
			return new UnknownMessagePart();
		}

		JsonObject obj = json.getAsJsonObject();
		String rawType = obj.has("type") && !obj.get("type").isJsonNull() ? obj.get("type").getAsString() : null;
		MessagePartType partType = MessagePartType.UNKNOWN;
		if (rawType != null) {
			try {
				partType = MessagePartType.valueOf(rawType);
			} catch (IllegalArgumentException ignore) {
				partType = MessagePartType.UNKNOWN;
			}
		}

		switch (partType) {
		case TEXT:
			return context.deserialize(obj, TextMessagePart.class);
		case MEDIA:
			return context.deserialize(obj, MediaMessagePart.class);
		case TOOL_CALL:
			return context.deserialize(obj, ToolCallMessagePart.class);
		case TOOL_RESULT:
			return context.deserialize(obj, ToolResultMessagePart.class);
		case THINKING:
			return context.deserialize(obj, ThinkingMessagePart.class);
		case SYSTEM:
			return context.deserialize(obj, SystemMessagePart.class);
		default:
			Map<String, Object> data = context.deserialize(obj, LinkedHashMap.class);
			return new UnknownMessagePart(partType, data);
		}
	}
}
