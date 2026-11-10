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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Preserves native Decisions answers and exposes usage for platform accounting. */
public class OpenAIDecisionsModelEngineResponse extends AbstractModelEngineResponse<Map<String, Object>> {

	private static final long serialVersionUID = 1L;

	private OpenAIDecisionsModelEngineResponse(Map<String, Object> response, int inputTokens, int outputTokens) {
		super(response, inputTokens, outputTokens);
	}

	@SuppressWarnings("unchecked")
	public static OpenAIDecisionsModelEngineResponse fromObject(Object output) {
		if (!(output instanceof Map<?, ?> map) || !(map.get("model") instanceof String)
				|| !(map.get("answers") instanceof List<?> answers)
				|| answers.stream().anyMatch(answer -> !(answer instanceof Map))
				|| !(map.get("usage") instanceof Map<?, ?> usage)) {
			throw new IllegalArgumentException("Invalid OpenAI Decisions response: expected model, answers array, and usage");
		}
		OpenAIDecisionsModelEngineResponse response = new OpenAIDecisionsModelEngineResponse(
				new LinkedHashMap<>((Map<String, Object>) map), tokenCount(usage.get("input_tokens")),
				tokenCount(usage.get("output_tokens")));
		response.setNumberOfCacheReadTokens(detailCount(usage.get("input_tokens_details"), "cached_tokens"));
		response.setNumberOfCacheCreationTokens(detailCount(usage.get("input_tokens_details"), "cache_write_tokens"));
		response.setNumberOfThinkingTokens(detailCount(usage.get("output_tokens_details"), "reasoning_tokens"));
		return response;
	}

	private static Integer detailCount(Object details, String key) {
		return details instanceof Map<?, ?> map && map.get(key) != null ? tokenCount(map.get(key)) : null;
	}

	private static int tokenCount(Object value) {
		if (!(value instanceof Number number) || !Double.isFinite(number.doubleValue())
				|| number.doubleValue() < 0 || number.doubleValue() > Integer.MAX_VALUE
				|| number.doubleValue() != number.intValue()) {
			throw new IllegalArgumentException("Invalid OpenAI Decisions token count");
		}
		return number.intValue();
	}
}
