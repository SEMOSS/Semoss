/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.api;

import prerna.engine.impl.model.BedrockEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.KServeImageEmbedEngine;
import prerna.engine.impl.model.KServeImageEngine;
import prerna.engine.impl.model.KServeTTSEngine;
import prerna.engine.impl.model.KServeVisionEngine;
import prerna.engine.impl.model.NEREngine;
import prerna.engine.impl.model.OpenAiEngine;
import prerna.engine.impl.model.TextEmbeddingsEngine;
import prerna.engine.impl.model.TextGenerationEngine;
import prerna.engine.impl.model.VertexEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;

public enum ModelTypeEnum {
	BEDROCK("BEDROCK", BedrockEngine.class.getName()), EMBEDDED("EMBEDDED", EmbeddedModelEngine.class.getName()),
	// FAST_CHAT("FAST_CHAT", FastChatProcessModel.class.getName()),
	NER("NER", NEREngine.class.getName()), KSERVE_VISION("KSERVE_VISION",
			KServeVisionEngine.class.getName()), KSERVE_IMAGE_EMBED("KSERVE_IMAGE_EMBED",
					KServeImageEmbedEngine.class.getName()), KSERVE_IMAGE("KSERVE_IMAGE",
							KServeImageEngine.class.getName()), KSERVE_TTS("KSERVE_TTS", KServeTTSEngine.class
									.getName()), OPEN_AI("OPEN_AI", OpenAiEngine.class.getName()), REMOTE("REMOTE",
											RemoteModelEngine.class.getName()), TEXT_EMBEDDINGS("TEXT_EMBEDDINGS",
													TextEmbeddingsEngine.class.getName()), TEXT_GENERATION(
															"TEXT_GENERATION",
															TextGenerationEngine.class.getName()), VERTEX("VERTEX",
																	VertexEngine.class.getName()),;

	private String modelName;
	private String modelClass;

	ModelTypeEnum(String modelName, String modelClass) {
		this.modelName = modelName;
		this.modelClass = modelClass;
	}

	/**
	 * @return
	 */
	public String getModelClass() {
		return this.modelClass;
	}

	/**
	 * @return
	 */
	public String getModelName() {
		return this.modelName;
	}

	/**
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
