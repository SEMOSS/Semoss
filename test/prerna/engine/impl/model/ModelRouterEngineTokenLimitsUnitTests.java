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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IModelEngine;
import prerna.util.Constants;
import prerna.util.Utility;

class ModelRouterEngineTokenLimitsUnitTests {

	@TempDir
	Path directory;

	@Test
	void derivedWindowTracksServingTargetChangesWithoutReloadingTheRouter() throws Exception {
		IModelEngine route = mock(IModelEngine.class);
		IModelEngine defaultRoute = mock(IModelEngine.class);
		IModelEngine fallback = mock(IModelEngine.class);
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(() -> Utility.getEngine("route")).thenReturn(route);
			utility.when(() -> Utility.getEngine("default")).thenReturn(defaultRoute);
			utility.when(() -> Utility.getEngine("fallback")).thenReturn(fallback);
			when(route.getContextWindow()).thenReturn(512_000);
			when(defaultRoute.getContextWindow()).thenReturn(256_000);
			when(fallback.getContextWindow()).thenReturn(128_000);
			ModelRouterEngine router = openRouter(new Properties());
			assertEquals(128_000, router.getContextWindow());

			when(fallback.getContextWindow()).thenReturn(1_000_000);
			assertEquals(256_000, router.getContextWindow());
			when(defaultRoute.getContextWindow()).thenReturn(1_000_000);
			assertEquals(512_000, router.getContextWindow());
		}
	}

	@Test
	void explicitSavedRouterWindowOverridesItsLegacySmssAndDerivedWindow() throws Exception {
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
			metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("router"))
					.thenReturn(Map.of("contextWindow", 256_000L));
			Properties properties = new Properties();
			properties.setProperty(Constants.CONTEXT_WINDOW, "128000");
			assertEquals(256_000, openRouter(properties).getContextWindow());
		}
	}

	private ModelRouterEngine openRouter(Properties properties) throws Exception {
		Path config = directory.resolve("router.json");
		Files.writeString(config, """
				{"routes": [{"engine_id": "route", "keywords": ["code"]}],
				 "default_route": "default", "fallbacks": ["fallback"]}
				""");
		ModelRouterEngine router = new ModelRouterEngine() {
			@Override
			public File resolveConfigFile() {
				return config.toFile();
			}
		};
		router.setBasic(true);
		properties.setProperty(Constants.ENGINE, "router");
		properties.setProperty(Constants.ENGINE_ALIAS, "test-router");
		router.open(properties);
		return router;
	}
}
