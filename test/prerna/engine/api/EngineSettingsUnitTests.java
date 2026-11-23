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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.openshell.OpenShellSecrets;

class EngineSettingsUnitTests {

	@TempDir
	Path folder;

	@Test
	void aSettingEditorsChangeIsSavedToTheSmssFileAndTheOpenSettings() throws IOException {
		Path smss = Files.writeString(this.folder.resolve("engine.smss"), "ENGINE\tid\n");
		Properties open = new Properties();
		IEngine engine = engineAt(smss.toString(), open);

		engine.updateSetting(IEngine.PIPELINE, "pipeline.json");

		assertEquals("pipeline.json", Utility.loadProperties(smss.toString()).getProperty(IEngine.PIPELINE));
		assertEquals("pipeline.json", open.getProperty(IEngine.PIPELINE));
	}

	@Test
	void anyOtherSettingIsLeftToTheOwners() throws IOException {
		Path smss = Files.writeString(this.folder.resolve("engine.smss"), "PASSWORD\tsecret\n");
		Properties open = new Properties();
		IEngine engine = engineAt(smss.toString(), open);

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> engine.updateSetting("PASSWORD", "changed"));

		assertTrue(refusal.getMessage().contains("owners"), refusal.getMessage());
		assertEquals("secret", Utility.loadProperties(smss.toString()).getProperty("PASSWORD"));
		assertNull(open.getProperty("PASSWORD"));
	}

	@Test
	void reloadingClosesTheEngineAndOpensItFromItsOwnFile() throws Exception {
		IEngine engine = engineAt("db/engine.smss", new Properties());

		engine.reload();

		InOrder order = inOrder(engine);
		order.verify(engine).close();
		order.verify(engine).open("db/engine.smss");
	}

	@Test
	void eachGetterReadsItsOneSetting() {
		Properties smss = new Properties();
		smss.setProperty(Constants.MODEL, "gpt-4o");
		smss.setProperty(IEngine.PIPELINE, "pipeline.json");
		smss.setProperty(Constants.BIG_DATA_ENGINE, "true");
		smss.setProperty(IFunctionEngine.FUNCTION_TYPE, "REST");
		smss.setProperty(IGuardrailReactorFunctionEngine.MODEL_ENGINE_ID_KEY, "judge");

		assertEquals("gpt-4o", engineHolding(IModelEngine.class, smss).getModelName());
		assertEquals("pipeline.json", engineHolding(IEngine.class, smss).getPipelineFile());
		assertTrue(engineHolding(IDatabaseEngine.class, smss).isBigData());
		assertEquals("REST", engineHolding(IFunctionEngine.class, smss).getFunctionType());
		assertEquals("judge", engineHolding(IGuardrailReactorFunctionEngine.class, smss).getJudgeModelEngineId());
	}

	@Test
	void anEngineWithoutTheSettingReturnsNothing() {
		Properties smss = new Properties();

		assertNull(engineHolding(IModelEngine.class, smss).getModelName());
		assertNull(engineHolding(IEngine.class, smss).getPipelineFile());
		assertFalse(engineHolding(IDatabaseEngine.class, smss).isBigData());
		assertNull(engineHolding(IGuardrailReactorFunctionEngine.class, smss).getJudgeModelEngineId());
		assertNull(engineHolding(IVectorDatabaseEngine.class, smss).getEmbedderEngineId());
	}

	@Test
	void aVectorDatabaseNamesItsEmbedderByEitherKey() {
		Properties current = new Properties();
		current.setProperty(Constants.EMBEDDER_ENGINE_ID, " embedder ");
		Properties older = new Properties();
		older.setProperty("ENCODER_ID", "encoder");

		assertEquals("embedder", engineHolding(IVectorDatabaseEngine.class, current).getEmbedderEngineId());
		assertEquals("encoder", engineHolding(IVectorDatabaseEngine.class, older).getEmbedderEngineId());
	}

	@Test
	void aProjectHandsOverOnlyItsSandboxSecrets() {
		Properties smss = new Properties();
		smss.setProperty(OpenShellSecrets.SECRETS_PROP, "[{\"name\":\"KEY\"}]");
		smss.setProperty(OpenShellSecrets.VALUE_PROP_PREFIX + "KEY", "value");
		smss.setProperty("RDBMS_INSIGHTS_PASSWORD", "secret");

		Properties secrets = engineHolding(IProject.class, smss).getSandboxSecretSettings();

		assertEquals(2, secrets.size());
		assertEquals("value", secrets.getProperty(OpenShellSecrets.VALUE_PROP_PREFIX + "KEY"));
		assertNull(secrets.getProperty("RDBMS_INSIGHTS_PASSWORD"));
	}

	@Test
	void s3AndGoogleCloudStorageNameTheirBucket() {
		Properties s3Smss = new Properties();
		s3Smss.setProperty(S3StorageEngine.S3_BUCKET_KEY, "s3-files");
		S3StorageEngine s3 = new S3StorageEngine();
		s3.setSmssProp(s3Smss);
		Properties gcsSmss = new Properties();
		gcsSmss.setProperty(GoogleCloudStorageEngine.GCS_BUCKET_KEY, "gcs-files");
		GoogleCloudStorageEngine gcs = new GoogleCloudStorageEngine();
		gcs.setSmssProp(gcsSmss);

		assertEquals("s3-files", s3.getBucketName());
		assertEquals("gcs-files", gcs.getBucketName());
		assertNull(engineHolding(IStorageEngine.class, new Properties()).getBucketName());
	}

	private static IEngine engineAt(String smssFilePath, Properties openSettings) {
		IEngine engine = engineHolding(IEngine.class, openSettings);
		doReturn(smssFilePath).when(engine).getSmssFilePath();
		return engine;
	}

	private static <T extends IEngine> T engineHolding(Class<T> type, Properties smss) {
		T engine = mock(type, CALLS_REAL_METHODS);
		doReturn(smss).when(engine).getSmssProp();
		return engine;
	}
}
