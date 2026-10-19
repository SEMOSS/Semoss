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
package prerna.reactor.model.upload;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.StaticModelMetadataCatalog;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

class CreateModelEngineTokenLimitsUnitTests {

	@TempDir
	Path directory;

	@Test
	void savesLimitsBeforeInitializingAModelAndExcludesThemFromItsSmss() throws Exception {
		exerciseCreation(false);
	}

	@Test
	void failedInitializationRemovesTheSavedMetadataAndReportsFailure() throws Exception {
		exerciseCreation(true);
	}

	private void exerciseCreation(boolean failInitialization) throws Exception {
		User user = mock(User.class);
		when(user.getLogins()).thenReturn(List.of());
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		CreateModelEngineReactor reactor = new CreateModelEngineReactor();
		reactor.setInsight(insight);
		reactor.getNounStore().makeNoun(ReactorKeysEnum.MODEL.getKey())
				.add(new NounMetadata("Test Model", PixelDataType.CONST_STRING));
		reactor.getNounStore().makeNoun(ReactorKeysEnum.MODEL_DETAILS.getKey())
				.add(new NounMetadata(Map.of(IModelEngine.MODEL_TYPE, "REMOTE", Constants.MODEL, "test-model",
						Constants.CONTEXT_WINDOW, 1_000_000L, Constants.MAX_TOKENS, 64_000L), PixelDataType.MAP));
		AtomicReference<String> engineId = new AtomicReference<>();
		AtomicReference<Map<String, Object>> saved = new AtomicReference<>();
		IOException initializationFailure = new IOException("Initialization failed");

		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<AbstractSecurityUtils> security = mockStatic(AbstractSecurityUtils.class);
				MockedStatic<SecurityEngineUtils> engines = mockStatic(SecurityEngineUtils.class);
				MockedStatic<StaticModelMetadataCatalog> catalog = mockStatic(StaticModelMetadataCatalog.class);
				MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<UploadUtilities> uploads = mockStatic(UploadUtilities.class);
				MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class);
				MockedConstruction<RemoteModelEngine> models = mockConstruction(RemoteModelEngine.class,
						(model, context) -> doAnswer(invocation -> {
							assertNotNull(saved.get(), "Model settings must be saved before engine.open");
							assertEquals(1_000_000L, saved.get().get(Constants.CONTEXT_WINDOW));
							assertEquals(64_000L, saved.get().get(Constants.MAX_TOKENS));
							if (failInitialization) throw initializationFailure;
							return null;
						}).when(model).open(anyString()))) {
			utility.when(() -> Utility.validateName("Test Model")).thenReturn(true);
			metadata.when(() -> SecurityModelMetadataUtils.normalizeModelDetails(anyMap())).thenCallRealMethod();
			metadata.when(() -> SecurityModelMetadataUtils.getModelEngineProperties(anyMap())).thenCallRealMethod();
			metadata.when(() -> SecurityModelMetadataUtils.upsertModelMetadata(anyString(), anyMap()))
					.thenAnswer(invocation -> {
						assertEquals(engineId.get(), invocation.getArgument(0));
						saved.set(invocation.getArgument(1));
						return null;
					});
			uploads.when(() -> UploadUtilities.generateSpecificEngineFolder(any(), anyString(), anyString()))
					.thenReturn(directory.toFile());
			uploads.when(() -> UploadUtilities.createTemporaryModelSmss(anyString(), anyString(), anyString(), anyMap()))
					.thenAnswer(invocation -> {
						engineId.set(invocation.getArgument(0));
						Map<String, Object> properties = invocation.getArgument(3);
						assertFalse(properties.containsKey(Constants.CONTEXT_WINDOW));
						assertFalse(properties.containsKey(Constants.MAX_TOKENS));
						Properties smss = new Properties();
						properties.forEach((key, value) -> smss.setProperty(key, value.toString()));
						Path file = directory.resolve(engineId.get() + ".temp");
						try (Writer writer = Files.newBufferedWriter(file)) {
							smss.store(writer, "Test model");
						}
						return file.toFile();
					});
			uploads.when(() -> UploadUtilities.getEngineReturnData(eq(user), anyString()))
					.thenAnswer(invocation -> Map.of("engine_id", invocation.getArgument(1, String.class)));

			if (failInitialization) {
				IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, reactor::execute);
				assertSame(initializationFailure, failure.getCause());
				engines.verify(() -> SecurityEngineUtils.deleteEngine(engineId.get()));
				uploads.verify(() -> UploadUtilities.cleanUpCreateNewError(any(), eq(engineId.get()), any(), isNull(), any()));
				uploads.verify(() -> UploadUtilities.getEngineReturnData(any(), anyString()), never());
			} else {
				NounMetadata result = reactor.execute();
				assertEquals(Map.of("engine_id", engineId.get()), result.getValue());
				assertTrue(Files.isRegularFile(directory.resolve(engineId.get() + ".smss")));
				engines.verify(() -> SecurityEngineUtils.addEngine(engineId.get(), false, user));
				engines.verify(() -> SecurityEngineUtils.deleteEngine(anyString()), never());
			}
			assertEquals(1, models.constructed().size());
			verify(models.constructed().get(0)).open(anyString());
			metadata.verify(() -> SecurityModelMetadataUtils.upsertModelMetadata(eq(engineId.get()), anyMap()));
		}
	}
}
