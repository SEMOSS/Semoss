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

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class MultiModalEmbeddingsReactor extends AbstractReactor {

	public MultiModalEmbeddingsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.TEXT.getKey(),
				ReactorKeysEnum.IMAGE.getKey(), ReactorKeysEnum.VIDEO.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		List<String> text = getInputStrings(this.keysToGet[1]);
		List<String> image = getInputStrings(this.keysToGet[2]);
		List<String> video = getInputStrings(this.keysToGet[3]);
		Map<String, Object> paramMap = getMap();
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		IModelEngine engine = Utility.getModel(engineId);
		Map<String, Object> output = engine.multiModalEmbeddings(text, image, video, this.insight, paramMap).toMap();
		return new NounMetadata(output, PixelDataType.MAP);
	}

	/**
	 * Get the input values provided under a given key. Unlike the single-input
	 * embedding reactors there is no fallback to the anonymous inputs, because the
	 * three modalities need to stay distinct.
	 *
	 * @param key the reactor key whose values should be collected
	 * @return the values for that key, or an empty list if none were provided
	 */
	public List<String> getInputStrings(String key) {
		List<String> inputStrings = new ArrayList<>();

		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				inputStrings.add(grs.get(i).toString());
			}
		}

		return inputStrings;
	}

	/**
	 *
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(this.keysToGet[4]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to interact with multi modal Embedding Model Engines, passing any combination of "
				+ "text, image, and video inputs to be embedded. The results are broken out by modality. If the model "
				+ "does not support multi modal embeddings it will return \"This model does not support multi modal embeddings.\"";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TEXT.getKey())) {
			return "Specify the text string value(s) to generate embeddings vector(s) from.";
		} else if (key.equals(ReactorKeysEnum.IMAGE.getKey())) {
			return "Specify the image input(s) - base64, data URL, or remote URL - to generate embeddings vector(s) from.";
		} else if (key.equals(ReactorKeysEnum.VIDEO.getKey())) {
			return "Specify the video input(s) - base64, data URL, or remote URL - to generate embeddings vector(s) from.";
		}
		return super.getDescriptionForKey(key);
	}
}
