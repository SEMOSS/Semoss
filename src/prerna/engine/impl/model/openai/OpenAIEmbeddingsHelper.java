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
package prerna.engine.impl.model.openai;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;

/**
 * Helper class for formatting embeddings responses into the OpenAI
 * {@code /v1/embeddings} wire shape.
 */
public final class OpenAIEmbeddingsHelper {

	/**
	 * Convert an engine embeddings response into the OpenAI embeddings payload: a
	 * {@code data} list of {@code {embedding, index, object}} entries plus the
	 * model and token usage.
	 *
	 * @param engineId           the model engine id, echoed as "model"
	 * @param embeddingsResponse the response returned by the model engine
	 * @return the OpenAI-shaped embeddings payload
	 */
	public static Map<String, Object> processEmbeddingsResponse(String engineId,
			EmbeddingsModelEngineResponse embeddingsResponse) {
		List<List<Double>> embeddings = embeddingsResponse.getResponse();
		Integer promptTokens = embeddingsResponse.getNumberOfTokensInPrompt();
		Integer responseTokens = embeddingsResponse.getNumberOfTokensInResponse();

		Map<String, Object> embeddingsResponseMap = new HashMap<>();

		List<Map<String, Object>> dataList = new ArrayList<>();
		for (int i = 0; i < embeddings.size(); i++) {
			Map<String, Object> embeddingMap = new HashMap<>();
			embeddingMap.put("embedding", embeddings.get(i));
			embeddingMap.put("index", i);
			embeddingMap.put("object", "embedding");

			dataList.add(embeddingMap);
		}

		embeddingsResponseMap.put("data", dataList);
		embeddingsResponseMap.put("model", engineId);
		embeddingsResponseMap.put("object", "list");

		// "usage" object
		Map<String, Object> usage = new HashMap<>();

		if (promptTokens != null && responseTokens != null) {
			usage.put("prompt_tokens", promptTokens);
			usage.put("total_tokens", promptTokens + responseTokens);
		} else {
			usage.put("prompt_tokens", promptTokens);
		}

		embeddingsResponseMap.put("usage", usage);
		return embeddingsResponseMap;
	}

	private OpenAIEmbeddingsHelper() {
	}
}
