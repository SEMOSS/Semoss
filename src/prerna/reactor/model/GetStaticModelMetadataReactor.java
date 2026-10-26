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

import java.nio.file.Path;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.StaticModelMetadataCatalog;

public class GetStaticModelMetadataReactor extends AbstractReactor {

	static final String MODEL_ID_KEY = "modelId";

	public GetStaticModelMetadataReactor() {
		this.keysToGet = new String[] { MODEL_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String modelId = requireValue(this.keyValue.get(MODEL_ID_KEY), MODEL_ID_KEY);

		Map<String, Object> metadata = getModelMetadata(getMetadataFile(), modelId);
		return new NounMetadata(metadata, PixelDataType.MAP);
	}

	static Map<String, Object> getModelMetadata(Path metadataFile, String modelId) {
		return StaticModelMetadataCatalog.getFlattenedMetadata(metadataFile, modelId);
	}

	Path getMetadataFile() {
		return StaticModelMetadataCatalog.getMetadataFile();
	}

	private static String requireValue(String value, String key) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException("Must input a " + key);
		}
		return value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Returns static metadata for a model from meta/model.json";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MODEL_ID_KEY)) {
			return "The catalog model key or fully qualified provider model ID in meta/model.json";
		}
		return super.getDescriptionForKey(key);
	}
}
