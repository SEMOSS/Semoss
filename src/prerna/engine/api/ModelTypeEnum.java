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
package prerna.engine.api;

import prerna.engine.impl.model.AnthropicEngine;
import prerna.engine.impl.model.AzureOpenAiEngine;
import prerna.engine.impl.model.BedrockEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.KServeImageEmbedEngine;
import prerna.engine.impl.model.KServeImageEngine;
import prerna.engine.impl.model.KServeTTSEngine;
import prerna.engine.impl.model.KServeVisionEngine;
import prerna.engine.impl.model.NEREngine;
import prerna.engine.impl.model.OpenAiEngine;
import prerna.engine.impl.model.RouterEngine;
import prerna.engine.impl.model.TextEmbeddingsEngine;
import prerna.engine.impl.model.TextGenerationEngine;
import prerna.engine.impl.model.VertexEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;

public enum ModelTypeEnum {

	// @formatter:off
	
	// these are the main ones
	ANTHROPIC("ANTHROPIC", AnthropicEngine.class.getName()),
	AZURE_OPEN_AI("AZURE_OPEN_AI", AzureOpenAiEngine.class.getName()),
	BEDROCK("BEDROCK", BedrockEngine.class.getName()),
	OPEN_AI("OPEN_AI", OpenAiEngine.class.getName()),
	ROUTER("ROUTER", RouterEngine.class.getName()),
	VERTEX("VERTEX", VertexEngine.class.getName()),

	// these are secondary
	EMBEDDED("EMBEDDED", EmbeddedModelEngine.class.getName()),
	NER("NER", NEREngine.class.getName()),
	KSERVE_VISION("KSERVE_VISION", KServeVisionEngine.class.getName()),
	KSERVE_IMAGE_EMBED("KSERVE_IMAGE_EMBED", KServeImageEmbedEngine.class.getName()),
	KSERVE_IMAGE("KSERVE_IMAGE", KServeImageEngine.class.getName()),
	KSERVE_TTS("KSERVE_TTS", KServeTTSEngine.class.getName()),
	REMOTE("REMOTE", RemoteModelEngine.class.getName()),
	TEXT_EMBEDDINGS("TEXT_EMBEDDINGS", TextEmbeddingsEngine.class.getName()),
	TEXT_GENERATION("TEXT_GENERATION", TextGenerationEngine.class.getName()),
	;
	// @formatter:on

	private String modelName;
	private String modelClass;

	ModelTypeEnum(String modelName, String modelClass) {
		this.modelName = modelName;
		this.modelClass = modelClass;
	}

	/**
	 * 
	 * @return
	 */
	public String getModelClass() {
		return this.modelClass;
	}

	/**
	 * 
	 * @return
	 */
	public String getModelName() {
		return this.modelName;
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public static ModelTypeEnum getEnumFromName(String name) {
		ModelTypeEnum[] allValues = values();
		for (ModelTypeEnum v : allValues) {
			if (v.getModelName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
