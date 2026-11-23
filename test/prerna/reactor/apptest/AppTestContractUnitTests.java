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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.parallel.Resources.SYSTEM_PROPERTIES;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import prerna.reactor.AbstractReactor;
import prerna.util.InternalUtility;

class AppTestContractUnitTests {
	@TempDir Path temporary;

	@Test
	@ResourceLock(SYSTEM_PROPERTIES)
	void readsH2SettingsFromRdfMapWithJvmOverrides() throws Exception {
		String rootKey = "semoss.appRegressionRoot";
		String urlKey = "semoss.appRegressionMonolithUrl";
		String recordingId = "94e82b66-61ce-449c-b6c2-b7e1dcbcfb71";
		Path project = Files.createDirectory(temporary.resolve("app"));
		Files.writeString(project.resolve("approval.json"), "{\"recordingId\":\"" + recordingId + "\"}");
		Properties prior = InternalUtility.getCoreProp();
		Properties configured = new Properties(prior);
		configured.setProperty(rootKey, temporary.toString());
		configured.setProperty(urlKey, "http://127.0.0.1:8080/Monolith");
		String priorRoot = System.clearProperty(rootKey);
		String priorUrl = System.clearProperty(urlKey);
		try {
			InternalUtility.setCoreProp(configured);
			assertEquals(recordingId, RegressionPreview.approvedRecordingId("app"));
			assertEquals(URI.create("http://127.0.0.1:8080/Monolith/"), RegressionPreview.loopbackMonolith());

			System.setProperty(rootKey, "");
			System.setProperty(urlKey, "http://127.0.0.1:9090/Monolith");
			assertNull(RegressionPreview.approvedRecordingId("app"));
			assertEquals(URI.create("http://127.0.0.1:9090/Monolith/"), RegressionPreview.loopbackMonolith());

			System.clearProperty(urlKey);
			configured.setProperty(urlKey, "http://example.org:8080/Monolith");
			assertThrows(IOException.class, RegressionPreview::loopbackMonolith);
		} finally {
			InternalUtility.setCoreProp(prior);
			if (priorRoot == null) System.clearProperty(rootKey); else System.setProperty(rootKey, priorRoot);
			if (priorUrl == null) System.clearProperty(urlKey); else System.setProperty(urlKey, priorUrl);
		}
	}

	@Test
	void allAppScopedToolsRequireAnExplicitTargetAndDeclareMapOutputs() {
		for (AbstractReactor reactor : List.of(new RunAppTestsReactor(), new SaveAppTestRecordingReactor(), new ListAppTestRecordingsReactor(), new GetAppTestRecordingReactor(), new DeleteAppTestRecordingReactor(), new ListAppTestRunsReactor(), new GetAppTestRunHistoryReactor())) {
			assertTrue(reactor.getReactorDefinition().getKey("project").getRequired());
			assertEquals(Map.class, reactor.getReactorDefinition().getOutput().getType());
		}
		assertEquals(List.class, new RunAppTestsReactor().getReactorDefinition().getKey("pages").getType());
		assertEquals(List.class, new RunAppTestsReactor().getReactorDefinition().getKey("recordingIds").getType());
		assertEquals(Boolean.class, new RunAppTestsReactor().getReactorDefinition().getKey("all").getType());
		assertTrue(new GetAppTestRecordingReactor().getReactorDefinition().getKey("recordingId").getRequired());
		assertEquals(Map.class, new GetAppTestRunReactor().getReactorDefinition().getOutput().getType());
	}

	@Test
	void fixturesRequireExplicitDevelopmentConfigurationAndNeverClaimBrowserVerification() throws Exception {
		Properties properties = InternalUtility.getCoreProp();
		Object priorFlag = properties.get(AppTestRuns.ALLOW_MOCKS_PROP);
		Object priorDir = properties.get(AppTestRuns.STUB_DIR_PROP);
		try {
			properties.setProperty(AppTestRuns.ALLOW_MOCKS_PROP, "false");
			assertThrows(IllegalArgumentException.class, () -> AppTestRuns.start("NATIVE:editor", "app", "all_pass", null, List.of(), null, null, "JSESSIONID"));
			Files.writeString(temporary.resolve("all_pass.json"), "{\"status\":\"COMPLETED\",\"tests\":[{\"name\":\"Fixture\",\"status\":\"PASSED\"}]}");
			properties.setProperty(AppTestRuns.ALLOW_MOCKS_PROP, "true");
			properties.setProperty(AppTestRuns.STUB_DIR_PROP, temporary.toString());
			String runId = AppTestRuns.start("NATIVE:editor", "app", "all_pass", null, List.of(), null, null, "JSESSIONID");
			assertEquals("MOCK", AppTestRuns.get("NATIVE:editor", runId).get("mode"));
			assertThrows(IllegalArgumentException.class, () -> AppTestRuns.get("MICROSOFT:editor", runId));
			assertThrows(IllegalArgumentException.class, () -> AppTestRuns.get("NATIVE:other", runId));
			assertNotEquals(AppTestRuns.ownerDirectory("NATIVE:editor"), AppTestRuns.ownerDirectory("MICROSOFT:editor"));
		} finally {
			if (priorFlag == null) properties.remove(AppTestRuns.ALLOW_MOCKS_PROP); else properties.put(AppTestRuns.ALLOW_MOCKS_PROP, priorFlag);
			if (priorDir == null) properties.remove(AppTestRuns.STUB_DIR_PROP); else properties.put(AppTestRuns.STUB_DIR_PROP, priorDir);
		}
	}
	@Test
	void realRunsCannotDefaultToAllEvenWhenThereAreNoRecordings() {
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.start(
				"NATIVE:editor", "app", null, null, List.of(), null, null, "JSESSIONID"));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.start(
				"NATIVE:editor", "app", null, null, List.of("orders"), null, null, "JSESSIONID"));
	}

	@Test
	void shippedToolboxExposesIndividualReadsAndExplicitSelections() throws Exception {
		Path manifest = Path.of(System.getProperty("semoss.test.sourceRoot", "."),
				"project/platform__app-tests/app_root/version/assets/mcp/pixel_mcp.json");
		com.google.gson.JsonArray tools = com.google.gson.JsonParser.parseString(Files.readString(manifest)).getAsJsonObject().getAsJsonArray("tools");
		boolean foundGet = false, foundRun = false, foundHistoryList = false, foundHistoryRead = false;
		for (var value : tools) {
			var tool = value.getAsJsonObject();
			if (tool.get("name").getAsString().equals("GetAppTestRecording")) {
				foundGet = true;
				assertEquals(2, tool.getAsJsonObject("inputSchema").getAsJsonArray("required").size());
			}
			if (tool.get("name").getAsString().equals("ListAppTestRuns")) {
                foundHistoryList = true;
                assertTrue(tool.getAsJsonObject("inputSchema").getAsJsonObject("properties").has("recordingId"));
            }
            if (tool.get("name").getAsString().equals("GetAppTestRunHistory")) foundHistoryRead = true;
            if (tool.get("name").getAsString().equals("RunAppTests")) {
				foundRun = true;
				assertTrue(tool.getAsJsonObject("inputSchema").getAsJsonObject("properties").has("recordingIds"));
				assertTrue(tool.getAsJsonObject("inputSchema").getAsJsonObject("properties").has("all"));
			}
		}
		assertTrue(foundGet && foundRun && foundHistoryList && foundHistoryRead);
	}

    @Test void generatedRecordingSchemaEqualsTheShippedToolSchema() throws Exception {
        var generated=new SaveAppTestRecordingReactor().asMcpTool().getJSONObject("inputSchema").getJSONObject("properties").getJSONObject("recording");
        assertEquals(0, generated.getJSONObject("properties").getJSONObject("cleanupSteps").getInt("minItems"));
        var manifest=com.google.gson.JsonParser.parseString(Files.readString(Path.of(System.getProperty("semoss.test.sourceRoot","."),"project/platform__app-tests/app_root/version/assets/mcp/pixel_mcp.json"))).getAsJsonObject();
        for(var item:manifest.getAsJsonArray("tools"))if(item.getAsJsonObject().get("name").getAsString().equals("SaveAppTestRecording"))
            assertEquals(com.google.gson.JsonParser.parseString(generated.toString()),item.getAsJsonObject().getAsJsonObject("inputSchema").getAsJsonObject("properties").get("recording"));
    }

}
