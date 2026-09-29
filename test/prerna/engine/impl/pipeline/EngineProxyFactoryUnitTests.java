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
package prerna.engine.impl.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

class EngineProxyFactoryUnitTests {
	@BeforeAll
	static void initializeEngineUtilityBaseFolder() {
		Properties properties = new Properties();
		properties.setProperty(Constants.BASE_FOLDER, System.getProperty("java.io.tmpdir"));
		DIHelper.getInstance().setCoreProp(properties);
	}

	@TempDir
	Path root;

	@Test
	void resolvesPipelineAsDirectChildOfCanonicalEngineAssets() throws Exception {
		Layout layout = layout(root.resolve("catalog"));
		try (MockedStatic<EngineUtility> utility = enginePaths(layout)) {
			File resolved = EngineProxyFactory.getJsonFile(engine(), " pipeline.json ");

			assertEquals(layout.assets().resolve("pipeline.json").toFile().getCanonicalFile(), resolved);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "../pipeline.json", "nested/pipeline.json", "..\\pipeline.json",
			"C:\\pipeline.json", "/tmp/pipeline.json" })
	void rejectsPipelinePathsThatAreNotSingleFilesInAssets(String pipeline) throws Exception {
		Layout layout = layout(root.resolve("catalog"));
		try (MockedStatic<EngineUtility> utility = enginePaths(layout)) {
			assertThrows(IllegalArgumentException.class, () -> EngineProxyFactory.getJsonFile(engine(), pipeline));
		}
	}

	@Test
	void rejectsPipelineSymlinkThatEscapesAssets() throws Exception {
		Layout layout = layout(root.resolve("catalog"));
		Path outside = Files.writeString(root.resolve("outside.json"), "{}");
		Files.createSymbolicLink(layout.assets().resolve("pipeline.json"), outside);
		try (MockedStatic<EngineUtility> utility = enginePaths(layout)) {
			assertThrows(IllegalArgumentException.class,
					() -> EngineProxyFactory.getJsonFile(engine(), "pipeline.json"));
		}
	}

	@Test
	void rejectsAssetsDirectoryOutsideTheEngineCatalog() throws Exception {
		Layout catalogLayout = layout(root.resolve("catalog"));
		Layout outsideLayout = layout(root.resolve("outside-catalog"));
		try (MockedStatic<EngineUtility> utility = mockStatic(EngineUtility.class)) {
			utility.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.MODEL))
					.thenReturn(catalogLayout.catalog().toString());
			utility.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.MODEL, "engine-id",
					"Engine Name")).thenReturn(outsideLayout.assets().toString());

			assertThrows(IllegalArgumentException.class,
					() -> EngineProxyFactory.getJsonFile(engine(), "pipeline.json"));
		}
	}

	@Test
	void readsPipelineOnlyAfterSinkLevelContainmentValidation() throws Exception {
		Layout layout = layout(root.resolve("catalog"));
		Path pipeline = Files.writeString(layout.assets().resolve("pipeline.json"), "{\"pipelines\":{}}");
		try (MockedStatic<EngineUtility> utility = enginePaths(layout)) {
			assertEquals("{\"pipelines\":{}}",
					PipelineInvocationHandler.getJsonData(engine(), pipeline.toFile()));
		}
	}

	@Test
	void sinkLevelValidationRejectsPipelineOutsideAssets() throws Exception {
		Layout layout = layout(root.resolve("catalog"));
		Path outside = Files.writeString(root.resolve("outside.json"), "{}");
		try (MockedStatic<EngineUtility> utility = enginePaths(layout)) {
			assertThrows(IllegalArgumentException.class,
					() -> PipelineInvocationHandler.getJsonData(engine(), outside.toFile()));
		}
	}

	@Test
	void sinkLevelValidationRejectsPipelineSymlinkThatEscapesAssets() throws Exception {
		Layout layout = layout(root.resolve("catalog"));
		Path outside = Files.writeString(root.resolve("outside.json"), "{}");
		Path pipeline = Files.createSymbolicLink(layout.assets().resolve("pipeline.json"), outside);
		try (MockedStatic<EngineUtility> utility = enginePaths(layout)) {
			assertThrows(IllegalArgumentException.class,
					() -> PipelineInvocationHandler.getJsonData(engine(), pipeline.toFile()));
		}
	}

	private IEngine engine() {
		IEngine engine = mock(IEngine.class);
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
		when(engine.getEngineId()).thenReturn("engine-id");
		when(engine.getEngineName()).thenReturn("Engine Name");
		return engine;
	}

	private Layout layout(Path catalog) throws Exception {
		Path assets = Files.createDirectories(catalog.resolve("Engine Name__engine-id")
				.resolve(Constants.APP_ROOT_FOLDER).resolve(Constants.VERSION_FOLDER).resolve(Constants.ASSETS_FOLDER));
		return new Layout(catalog, assets);
	}

	private MockedStatic<EngineUtility> enginePaths(Layout layout) {
		MockedStatic<EngineUtility> utility = mockStatic(EngineUtility.class);
		utility.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.MODEL))
				.thenReturn(layout.catalog().toString());
		utility.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.MODEL, "engine-id",
				"Engine Name")).thenReturn(layout.assets().toString());
		return utility;
	}

	private record Layout(Path catalog, Path assets) {
	}
}
