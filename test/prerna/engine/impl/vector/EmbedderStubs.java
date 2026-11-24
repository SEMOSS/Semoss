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
package prerna.engine.impl.vector;

import static org.mockito.Mockito.lenient;

import java.util.Properties;

import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.util.Constants;

/**
 * Stubs an embedding model mock from its settings, the way a vector database
 * reads them: its model name, type and max tokens, each through its own getter.
 */
final class EmbedderStubs {

	private EmbedderStubs() {
	}

	/**
	 * Makes the embedder answer with the model name, model type and max tokens its
	 * settings hold, and with the settings themselves.
	 *
	 * @param embedder the embedding model mock
	 * @param smss     its settings
	 */
	static void answerFrom(IModelEngine embedder, Properties smss) {
		lenient().when(embedder.getSmssProp()).thenReturn(smss);
		lenient().when(embedder.getModelName()).thenReturn(smss.getProperty(Constants.MODEL));
		String modelType = smss.getProperty(IModelEngine.MODEL_TYPE);
		lenient().when(embedder.getModelType()).thenReturn(modelType == null ? null : ModelTypeEnum.valueOf(modelType));
		String maxTokens = smss.getProperty(Constants.MAX_TOKENS);
		lenient().when(embedder.getMaxTokens()).thenReturn(maxTokens == null ? 0L : Long.parseLong(maxTokens.trim()));
	}
}
