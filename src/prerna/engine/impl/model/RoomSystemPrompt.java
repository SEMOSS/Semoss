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
package prerna.engine.impl.model;

import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Shared room-instruction composition for chat and agent harnesses. */
public final class RoomSystemPrompt {

	private RoomSystemPrompt() {

	}

	/**
	 * Resolve room instructions against a lazily loaded agent prompt. An explicit
	 * {@code overrideSystemPrompt=false} appends instructions; omitted flags retain
	 * legacy replacement behavior. Blank instructions always retain the agent
	 * prompt. The caller owns workspace lookup and its access checks.
	 */
	public static String resolve(String roomOptions, Supplier<String> agentPrompt) {
		JsonObject options = new JsonObject();
		if (StringUtils.isNotBlank(roomOptions)) {
			try {
				options = JsonParser.parseString(roomOptions).getAsJsonObject();
			} catch (RuntimeException ignored) {
				// Malformed options behave like an unconfigured room.
			}
		}

		JsonElement instructionsValue = options.get("instructions");
		String instructions = instructionsValue != null && instructionsValue.isJsonPrimitive()
				? StringUtils.trimToNull(instructionsValue.getAsString())
				: null;
		JsonElement overrideValue = options.get("overrideSystemPrompt");
		boolean append = overrideValue != null && overrideValue.isJsonPrimitive()
				&& overrideValue.getAsJsonPrimitive().isBoolean() && !overrideValue.getAsBoolean();
		if (instructions != null && !append) {
			return instructions;
		}

		String base = StringUtils.trimToNull(agentPrompt.get());
		if (instructions == null) {
			return base;
		}
		return base == null ? instructions : base + "\n\n" + instructions;
	}
}
