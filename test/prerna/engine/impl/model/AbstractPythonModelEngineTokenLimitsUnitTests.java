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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.util.Constants;
import prerna.util.Settings;

class AbstractPythonModelEngineTokenLimitsUnitTests {

	@Test
	void oldInitPlaceholdersUseSavedLimitsAndPythonNoneWhenClearedOnReload() throws Exception {
		Map<String, Object> row = new HashMap<>(Map.of("contextWindow", 1_000_000L, "maxOutputTokens", 64_000L));
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
			metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id")).thenReturn(row);
			Properties properties = new Properties();
			properties.setProperty(Constants.ENGINE, "model-id");
			properties.setProperty(Constants.ENGINE_ALIAS, "test-model");
			properties.setProperty(Settings.VAR_NAME, "test_model");
			properties.setProperty(Constants.CONTEXT_WINDOW, "128000");
			properties.setProperty(Constants.MAX_TOKENS, "4096");
			TestPythonModelEngine engine = new TestPythonModelEngine();
			engine.setBasic(true);
			engine.open(properties);

			assertEquals("build_model(context_window=1000000, max_tokens=64000)", engine.initScript());
			row.put("contextWindow", null);
			row.put("maxOutputTokens", null);
			engine.close();
			engine.open(engine.getOrigSmssProp());

			assertEquals("build_model(context_window=None, max_tokens=None)", engine.initScript());
			assertEquals(0, engine.getContextWindow());
			assertNull(engine.maxTokens);
			assertNull(engine.getSmssProp().getProperty(Constants.MAX_TOKENS));
		}
	}

	private static class TestPythonModelEngine extends AbstractPythonModelEngine {
		String initScript() {
			return new StringSubstitutor(vars)
					.replace("build_model(context_window=${CONTEXT_WINDOW}, max_tokens=${MAX_TOKENS})");
		}

		@Override
		public ModelTypeEnum getModelType() {
			return ModelTypeEnum.OPEN_AI;
		}
	}
}
