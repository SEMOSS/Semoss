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
package prerna.playground;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.message.ResponseMessage;

public class PlaygroundUtils {

	public static final String PLAYGROUND_PROJECT_ID = "SYSTEM__PLAYGROUND";

	/**
	 * Canned assistant-side ack persisted after a hidden user note (e.g. the
	 * "your prior response was cut short" note that follows a cancelled turn).
	 * Kept so provider payloads stay role-alternating.
	 */
	public static final String HIDDEN_MESSAGE_ACK = "Understood — I'll wait for your next instruction.";

	public static final String FOLLOW_UP_SUGGESTIONS_PROMPT = """
			You generate follow-up suggestions only.
			Based on the most recent conversation, propose short, user-style follow-up questions that advance the same topic.
			Do not answer the user. Do not include preamble or numbering.
			Each suggestion must be a single sentence (<= 12 words).
			Avoid repeating the last user message verbatim.
			""";

	public static final String FOLLOW_UP_SUGGESTIONS_SCHEMA = """
			{
			  "title": "FollowUpSuggestions",
			  "type": "object",
			  "additionalProperties": false,
			  "required": ["suggestions"],
			  "properties": {
			    "suggestions": {
			      "type": "array",
			      "minItems": %s,
			      "maxItems": %s,
			      "items": { "type": "string" }
			    }
			  }
			}
			""";

	/**
	 * Build a ResponseMessage from a caller-supplied parts list, in order. Each
	 * element is expected to be a Map with {@code type} = "THINKING" or "TEXT"
	 * and the matching payload field. Always returns a message — empty when no
	 * usable parts came through — so the caller's input/response pair stays
	 * balanced. Other part types (TOOL_CALL/TOOL_RESULT/MEDIA) are intentionally
	 * ignored: a cancelled stream shouldn't have them, and any half-formed
	 * TOOL_CALL from a chained tool-use turn would be an orphan.
	 */
	public static ResponseMessage buildResponseMessageFromParts(List<Map<String, Object>> responseParts) {
		ResponseMessage.Builder builder = ResponseMessage.builder();
		if (responseParts != null) {
			for (Map<String, Object> part : responseParts) {
				if (part == null) {
					continue;
				}
				Object typeObj = part.get("type");
				String type = typeObj != null ? typeObj.toString() : null;
				if ("THINKING".equals(type)) {
					Object thinkingObj = part.get("thinking");
					String thinking = thinkingObj != null ? thinkingObj.toString() : null;
					if (thinking != null && !thinking.isEmpty()) {
						builder.withThinking(thinking);
					}
				} else if ("TEXT".equals(type)) {
					Object textObj = part.get("text");
					String text = textObj != null ? textObj.toString() : null;
					if (text != null && !text.isEmpty()) {
						builder.withText(text);
					}
				}
			}
		}
		return builder.build();
	}
}
