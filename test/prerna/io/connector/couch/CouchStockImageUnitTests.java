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
package prerna.io.connector.couch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import jakarta.ws.rs.core.Response;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.util.DefaultImageGeneratorUtil;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

class CouchStockImageUnitTests {

	@TempDir
	Path temp;
	private final List<String> methods = new ArrayList<>();
	private String findBody;
	private MockedStatic<Utility> utility;
	private MockedStatic<ClusterUtil> cluster;
	private MockedStatic<EngineUtility> engines;
	private MockedStatic<InsightUtility> images;
	private MockedStatic<DefaultImageGeneratorUtil> stock;
	private MockedStatic<MasterDatabaseUtility> databases;
	private MockedStatic<SecurityProjectUtils> projects;
	private MockedStatic<HttpClientBuilder> http;
	private MockedStatic<Response> responses;

	@BeforeEach
	void setUp() throws Exception {
		utility = mockStatic(Utility.class);
		utility.when(Utility::getBaseFolder).thenReturn(temp.toString());
		cluster = mockStatic(ClusterUtil.class);
		engines = mockStatic(EngineUtility.class);
		images = mockStatic(InsightUtility.class);
		stock = mockStatic(DefaultImageGeneratorUtil.class);
		databases = mockStatic(MasterDatabaseUtility.class);
		projects = mockStatic(SecurityProjectUtils.class);
		databases.when(() -> MasterDatabaseUtility.getDatabaseAliasForId("resource-id")).thenReturn("Example");
		projects.when(() -> SecurityProjectUtils.getProjectAliasForId("resource-id")).thenReturn("Example");
		http = mockStatic(HttpClientBuilder.class);
		HttpClientBuilder builder = mock(HttpClientBuilder.class);
		CloseableHttpClient client = mock(CloseableHttpClient.class);
		http.when(HttpClientBuilder::create).thenReturn(builder);
		when(builder.build()).thenReturn(client);
		findBody = "{\"docs\":[]}";
		when(client.execute(any(HttpUriRequest.class))).thenAnswer(invocation -> {
			HttpUriRequest request = invocation.getArgument(0);
			methods.add(request.getMethod());
			String body = switch (request.getMethod()) {
			case "POST" -> findBody;
			case "GET" -> "{\"_attachments\":{\"image.png\":{\"data\":\""
					+ Base64.getEncoder().encodeToString(new byte[] { 7, 8, 9 }) + "\"}}}";
			case "PUT" -> "{\"ok\":true}";
			default -> throw new AssertionError("Unexpected Couch request: " + request.getMethod());
			};
			CloseableHttpResponse response = mock(CloseableHttpResponse.class);
			when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
			when(response.getEntity()).thenReturn(new StringEntity(body));
			return response;
		});
		// Core includes the JAX-RS API; the runtime response provider lives in
		// Monolith.
		responses = mockStatic(Response.class);
		Response.ResponseBuilder responseBuilder = mock(Response.ResponseBuilder.class, RETURNS_SELF);
		Response downloaded = mock(Response.class);
		when(downloaded.getStatus()).thenReturn(200);
		when(responseBuilder.build()).thenReturn(downloaded);
		responses.when(() -> Response.ok(any(byte[].class))).thenAnswer(invocation -> {
			when(downloaded.getEntity()).thenReturn(invocation.getArgument(0));
			return responseBuilder;
		});
	}

	@AfterEach
	void tearDown() {
		if (responses != null) {
			responses.close();
		}
		if (http != null) {
			http.close();
		}
		if (projects != null) {
			projects.close();
		}
		if (databases != null) {
			databases.close();
		}
		if (stock != null) {
			stock.close();
		}
		if (images != null) {
			images.close();
		}
		if (engines != null) {
			engines.close();
		}
		if (cluster != null) {
			cluster.close();
		}
		if (utility != null) {
			utility.close();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "project", "database" })
	void missingImageReturnsStockWithoutWritingCouchDocument(String partition) throws Exception {
		byte[] bytes = { 1, 2, 3 };
		stock.when(() -> DefaultImageGeneratorUtil.pickRandomImageBytes("resource-id", null)).thenReturn(bytes);
		Response response = CouchUtil.download(partition, Map.of(partition, "resource-id"));
		assertEquals(200, response.getStatus());
		assertArrayEquals(bytes, (byte[]) response.getEntity());
		assertEquals(List.of("POST"), methods);
	}

	@ParameterizedTest
	@ValueSource(strings = { "project", "database" })
	void themeChangesUseMatchingStockWithoutPersistingAnAttachment(String partition) throws Exception {
		byte[] light = { 1, 2, 3 };
		byte[] dark = { 4, 5, 6 };
		stock.when(() -> DefaultImageGeneratorUtil.pickRandomImageBytes("resource-id", "light")).thenReturn(light);
		stock.when(() -> DefaultImageGeneratorUtil.pickRandomImageBytes("resource-id", "dark")).thenReturn(dark);
		Response lightResponse = CouchUtil.download(partition, Map.of(partition, "resource-id"), "light");
		assertArrayEquals(light, (byte[]) lightResponse.getEntity());
		Response darkResponse = CouchUtil.download(partition, Map.of(partition, "resource-id"), "dark");
		assertArrayEquals(dark, (byte[]) darkResponse.getEntity());
		assertEquals(List.of("POST", "POST"), methods);
	}

	@ParameterizedTest
	@ValueSource(strings = { "light", "dark" })
	void stockAssignmentMatchesLocalAndClusterPathsAcrossRenames(String theme) throws Exception {
		stock.close();
		stock = mockStatic(DefaultImageGeneratorUtil.class, CALLS_REAL_METHODS);
		Path directory = Files.createDirectories(temp.resolve("images/stock-engines-" + theme));
		for (int i = 0; i < 12; i++) {
			Files.writeString(directory.resolve(i + ".png"), theme + "-image-" + i);
		}
		String localPath = temp.resolve("project/platform__resource-id/app_root/version/image.png").toString();
		String clusterPath = temp.resolve("images/projects/resource-id.png").toString();
		File local = DefaultImageGeneratorUtil.getStockImageForPath(localPath, theme);
		assertNotNull(local);
		assertEquals(local, DefaultImageGeneratorUtil.getStockImageForPath(clusterPath, theme));
		for (String alias : new String[] { "MCP A", "MCP B", "platform" }) {
			projects.when(() -> SecurityProjectUtils.getProjectAliasForId("resource-id")).thenReturn(alias);
			Response response = CouchUtil.download("project", Map.of("project", "resource-id"), theme);
			assertArrayEquals(Files.readAllBytes(local.toPath()), (byte[]) response.getEntity());
		}
		assertEquals(List.of("POST", "POST", "POST"), methods);
	}

	@ParameterizedTest
	@ValueSource(strings = { "project", "database" })
	void localImageStillMigratesToCouch(String partition) throws Exception {
		File uploaded = Files.write(temp.resolve("image.png"), new byte[] { 4, 5, 6 }).toFile();
		images.when(() -> InsightUtility.findImageFile(nullable(String.class))).thenReturn(new File[] { uploaded });
		images.when(() -> InsightUtility.findImageFile(nullable(String.class), eq("resource-id")))
				.thenReturn(new File[] { uploaded });
		Response response = CouchUtil.download(partition, Map.of(partition, "resource-id"), "dark");
		assertArrayEquals(new byte[] { 4, 5, 6 }, (byte[]) response.getEntity());
		assertEquals(List.of("POST", "PUT"), methods);
		stock.verifyNoInteractions();
	}

	@ParameterizedTest
	@ValueSource(strings = { "project", "database" })
	void existingAttachmentTakesPrecedenceOverStock(String partition) throws Exception {
		findBody = "{\"docs\":[{\"_id\":\"existing\",\"_attachments\":{\"image.png\":{}}}]}";
		Response response = CouchUtil.download(partition, Map.of(partition, "resource-id"), "dark");
		assertArrayEquals(new byte[] { 7, 8, 9 }, (byte[]) response.getEntity());
		assertEquals(List.of("POST", "GET"), methods);
		stock.verifyNoInteractions();
	}

	/**
	 * Path traversal hardening: if the resolved local image directory for a
	 * database/project ever escapes the engine's own trusted base directory (for
	 * example because a crafted id produced a "../.." in the path that
	 * EngineUtility.getSpecificEngineVersionFolder built), download() must refuse
	 * to read from it rather than silently serving whatever file happens to live
	 * there.
	 */
	@Test
	void downloadRejectsADatabaseImagePathThatEscapesTheExpectedBaseDirectory() throws Exception {
		Path trustedBase = Files.createDirectories(temp.resolve("db-trusted-base"));
		// a sibling directory outside of trustedBase - standing in for wherever a
		// crafted id's "../.." could have redirected the resolved path to
		Path escaped = Files.createDirectories(temp.resolve("db-escaped-elsewhere"));
		Files.write(escaped.resolve("image.png"), new byte[] { 1, 1, 1 });
		engines.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
				.thenReturn(trustedBase.toString());
		engines.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.DATABASE, "resource-id",
				"Example")).thenReturn(escaped.toString());

		// download() must fail closed before ever reading a file from "escaped"
		CouchException ex = assertThrows(CouchException.class,
				() -> CouchUtil.download("database", Map.of("database", "resource-id")));
		assertTrue(ex.getMessage().toLowerCase().contains("outside"));
		images.verifyNoInteractions();
		stock.verifyNoInteractions();
	}

	/**
	 * Non-regression companion to the above: a resolved path that legitimately
	 * stays within the engine's trusted base directory must still be served
	 * normally.
	 */
	@Test
	void downloadAcceptsADatabaseImagePathWithinTheExpectedBaseDirectory() throws Exception {
		Path trustedBase = Files.createDirectories(temp.resolve("db-trusted-base"));
		Path within = Files.createDirectories(trustedBase.resolve("resource-id-version"));
		Files.write(within.resolve("image.png"), new byte[] { 2, 2, 2 });
		engines.when(() -> EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.DATABASE))
				.thenReturn(trustedBase.toString());
		engines.when(() -> EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.DATABASE, "resource-id",
				"Example")).thenReturn(within.toString());
		images.when(() -> InsightUtility.findImageFile(within.toString()))
				.thenReturn(new File[] { within.resolve("image.png").toFile() });

		Response response = CouchUtil.download("database", Map.of("database", "resource-id"));

		assertArrayEquals(new byte[] { 2, 2, 2 }, (byte[]) response.getEntity());
		stock.verifyNoInteractions();
	}
}
