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
package prerna.reactor.playwright;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Produces one editable browser-automation goal from recent Playground room
 * conversation. This is intentionally separate from action planning so the user
 * can review and change the goal before any browser interaction runs.
 */
public class GeneratePlaywrightAutomationGoalReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GeneratePlaywrightAutomationGoalReactor.class);

	private static final int DEFAULT_MESSAGE_LIMIT = 20;
	private static final int MAX_MESSAGE_LIMIT = 20;
	private static final int MAX_GOAL_LENGTH = 4_000;
	private static final int MAX_CONTEXT_LENGTH = 12_000;

	public GeneratePlaywrightAutomationGoalReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> result = new LinkedHashMap<>();
		try {
			String requestedEngine = clean(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
			String roomId = clean(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
			int limit = parseLimit(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
			Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
			String engineId = firstNonBlank(requestedEngine, activeRoomModel(room));
			if (engineId.isBlank()) {
				throw new IllegalArgumentException("No model is available to generate the browser automation goal");
			}
			if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException(
						"Model " + engineId + " does not exist or user does not have access");
			}

			String conversation = RecordingMetadataPrivacy.sanitizeText(
					GeneratePlaywrightFieldActionsReactor.buildRoomContext(room, limit), MAX_CONTEXT_LENGTH);
			if (conversation.isBlank()) {
				throw new IllegalArgumentException("No visible Playground conversation was found");
			}

			IModelEngine model = Utility.getModel(engineId);
			Room inferenceRoom = RoomUtils.createRoomForStatelessAsk(UUID.randomUUID().toString(), this.insight, model,
					null);
			ResponseMessage response = inferenceRoom
					.ask(InputMessage.builder(inferenceRoom).withText(buildPrompt(conversation)).build(), model);
			String goal = parseGoal(responseText(response));

			result.put("success", true);
			result.put("goal", goal);
			result.put("engineId", engineId);
			result.put("messageLimit", limit);
			return new NounMetadata(result, PixelDataType.MAP);
		} catch (Exception e) {
			classLogger.warn("GeneratePlaywrightAutomationGoal failed: {}", e.getMessage());
			result.put("success", false);
			result.put("error", e.getMessage() == null ? "Goal generation failed" : e.getMessage());
			return new NounMetadata(result, PixelDataType.MAP);
		}
	}

	static String buildPrompt(String conversation) {
		return """
				Summarize the user's current browser-automation goal from the recent Playground conversation below.
				Use the latest user request as the primary objective and retain relevant constraints, values, and expected outcome \
				from earlier messages. Ignore completed or superseded requests. Do not invent steps and do not include passwords, \
				authentication secrets, tokens, or other credentials. Write one concise, actionable goal that can be reviewed and \
				edited before automation starts.
				Return ONLY JSON in this form: {"goal":"..."}.

				RECENT CONVERSATION (oldest to newest):
				"""
				+ conversation + "\n\nJSON:";
	}

	static String parseGoal(String modelOutput) throws Exception {
		String output = modelOutput == null ? "" : modelOutput.trim();
		int start = output.indexOf('{');
		int end = output.lastIndexOf('}');
		if (start < 0 || end <= start) {
			throw new IllegalArgumentException("Model did not return a JSON goal object");
		}
		Map<String, Object> parsed = GSON.fromJson(output.substring(start, end + 1),
				new TypeToken<Map<String, Object>>() {
				}.getType());
		if (parsed == null) {
			throw new IllegalArgumentException("Model did not return a JSON goal object");
		}
		String goal = clean(parsed.get("goal"));
		if (goal.isBlank()) {
			throw new IllegalArgumentException("Model returned an empty automation goal");
		}
		return goal.length() > MAX_GOAL_LENGTH ? goal.substring(0, MAX_GOAL_LENGTH) : goal;
	}

	private static String responseText(ResponseMessage response) {
		String text = firstNonBlank(response.getContent(), response.getThinking());
		if (text.isBlank() && response.hasToolResponses()) {
			return response.getToolResponses().toString();
		}
		return text;
	}

	private static String activeRoomModel(Room room) {
		Object optionModel = room.getOptionsMap() == null ? null : room.getOptionsMap().get("modelId");
		return firstNonBlank(clean(optionModel), clean(room.getModelId()));
	}

	private static int parseLimit(Object value) {
		try {
			return Math.min(Math.max(1, Integer.parseInt(clean(value))), MAX_MESSAGE_LIMIT);
		} catch (Exception e) {
			return DEFAULT_MESSAGE_LIMIT;
		}
	}

	private static String clean(Object value) {
		if (value == null) {
			return "";
		}
		String text = String.valueOf(value).trim();
		if (text.length() >= 2
				&& ((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("'") && text.endsWith("'")))) {
			return text.substring(1, text.length() - 1).trim();
		}
		return text;
	}

	private static String firstNonBlank(String... values) {
		for (String value : values) {
			if (value != null && !value.isBlank()) {
				return value.trim();
			}
		}
		return "";
	}

	@Override
	public String getReactorDescription() {
		return "Generates one editable browser-automation goal from up to 20 recent visible Playground messages before "
				+ "any browser actions are executed.";
	}
}
