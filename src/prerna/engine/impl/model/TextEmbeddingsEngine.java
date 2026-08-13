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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public class TextEmbeddingsEngine extends AbstractRESTModelEngine {

	private static final Logger classLogger = LogManager.getLogger(TextEmbeddingsEngine.class);

	private static final String ENDPOINT = "ENDPOINT";
	private static final String BATCH_SIZE = "BATCH_SIZE";

	private int batchSize;
	private String endpoint;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.endpoint = this.smssProp.getProperty(ENDPOINT);
		if (this.endpoint == null || (this.endpoint = this.endpoint.trim()).isEmpty()) {
			throw new IllegalArgumentException("This model requires a valid value for " + ENDPOINT);
		}
//		Utility.checkIfValidDomain(this.endpoint);

		this.batchSize = 32;
		String batchSizeStr = null;
		try {
			batchSizeStr = this.smssProp.getProperty(BATCH_SIZE);
			if (batchSizeStr != null && !(batchSizeStr = batchSizeStr.trim()).isEmpty()) {
				this.batchSize = Integer.valueOf(batchSizeStr);
			}
		} catch (NumberFormatException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("The SMSS has an invalid value for " + BATCH_SIZE
					+ ". Must be an integer but found " + batchSizeStr);
		}
	}

	@Override
	public EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEncode, Insight insight,
			Map<String, Object> parameters) {
		List<List<Double>> embeddings = new ArrayList<>();

		List<List<String>> sentenceSublists = new ArrayList<>();

		for (int i = 0; i < stringsToEncode.size(); i += batchSize) {
			int endIndex = Math.min(i + batchSize, stringsToEncode.size());
			List<String> sublist = stringsToEncode.subList(i, endIndex);
			sentenceSublists.add(sublist);
		}

		for (List<String> sublist : sentenceSublists) {
			Map<String, Object> bodyMap = new HashMap<>();
			bodyMap.put("inputs", sublist);
			bodyMap.put("truncate", true);
			String output = HttpHelperUtility.postRequestStringBody(this.endpoint, null, new Gson().toJson(bodyMap),
					ContentType.APPLICATION_JSON, null, null, null);

			List<List<Double>> outputParsed = new Gson().fromJson(output, new TypeToken<List<List<Double>>>() {
			}.getType());
			embeddings.addAll(outputParsed);
		}

		EmbeddingsModelEngineResponse embeddingsResponse = new EmbeddingsModelEngineResponse(embeddings, 0, 0);

		return embeddingsResponse;
	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> parameters) {
		return new AskStringModelEngineResponse("This model does not support text generation.", 0, 0);
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_EMBEDDINGS;
	}

	@Override
	protected void resetAfterTimeout() {
		// nothing to reset currently
	}
}