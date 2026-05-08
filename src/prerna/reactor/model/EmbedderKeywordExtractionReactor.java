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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class EmbedderKeywordExtractionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EmbedderKeywordExtractionReactor.class);

	private static final String PERCENTILE = "percentile";

	public EmbedderKeywordExtractionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MODEL.getKey(), ReactorKeysEnum.INPUT.getKey(), PERCENTILE,
				ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelE = Utility.getModel(engineId);
		if (!(modelE instanceof EmbeddedModelEngine)) {
			throw new IllegalArgumentException("This method only works for Local EmbeddedModelEngines");
		}

		String percentile = this.keyValue.get(PERCENTILE);
		String limit = this.keyValue.get(this.keysToGet[3]);

		EmbeddedModelEngine eme = (EmbeddedModelEngine) modelE;
		Map<String, Object> parameters = new HashMap<>();
		if (percentile != null && !(percentile = percentile.trim()).isEmpty()) {
			parameters.put("percentile", ((Number) Double.parseDouble(percentile)).intValue());
		}
		if (limit != null && !(limit = limit.trim()).isEmpty()) {
			parameters.put("max_keywords", ((Number) Double.parseDouble(limit)).intValue());
		}

		List<String> input = getInput();
		if (input.isEmpty()) {
			throw new IllegalArgumentException("Must pass in list of inputs");
		}
		List<String> keywords = eme.keywordExtraction(input, insight, parameters);
		return new NounMetadata(keywords, PixelDataType.VECTOR);
	}

	private List<String> getInput() {
		List<String> columns = new ArrayList<>();

		GenRowStruct colGrs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		} else {
			GenRowStruct inputsGRS = this.getCurRow();
			// keep track of selectors to change to upper case
			if (inputsGRS != null && !inputsGRS.isEmpty()) {
				for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
					String column = inputsGRS.get(selectIndex) + "";
					columns.add(column);
				}
			}
		}

		return columns;
	}

	@Override
	public String getReactorDescription() {
		return "Utilizes a keyBERT model to extract the keywords from the text input";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.INPUT.getKey())) {
			return """
					The input array of string values to extract keywords from. Each string input will result in a space delimited list of keywords. \
					For convenience, instead of escaping quotes or backslashes you can wrap \
					each element within "<encode>your_text</encode>" and the system will encode it for you.
					""";
		} else if (key.equals(PERCENTILE)) {
			return "The percentile (integer) cutoff for the words within the text to be considered a keyword. Values must be between 0 and 100 inclusive.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The limit to be applied after the percentile for the maximum number of keywords to be returned for each string input";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.INPUT.getKey()).put("description",
				"The input array of string values to extract keywords from. Each string input will result in a space delimited list of keywords.");
		return properties;
	}

}
