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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import jakarta.ws.rs.core.Response;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.util.DefaultImageGeneratorUtil;
import prerna.util.EngineUtility;
import prerna.util.insight.InsightUtility;
import prerna.util.Utility;
import prerna.masterdatabase.utility.MasterDatabaseUtility;

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
						+ Base64.getEncoder().encodeToString(new byte[] {7, 8, 9}) + "\"}}}";
				case "PUT" -> "{\"ok\":true}";
				default -> throw new AssertionError("Unexpected Couch request: " + request.getMethod());
			};
			CloseableHttpResponse response = mock(CloseableHttpResponse.class);
			when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
			when(response.getEntity()).thenReturn(new StringEntity(body));
			return response;
		});
		// Core includes the JAX-RS API; the runtime response provider lives in Monolith.
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
		if (responses != null) responses.close();
		if (http != null) http.close();
		if (projects != null) projects.close();
		if (databases != null) databases.close();
		if (stock != null) stock.close();
		if (images != null) images.close();
		if (engines != null) engines.close();
		if (cluster != null) cluster.close();
		if (utility != null) utility.close();
	}

	@ParameterizedTest
	@ValueSource(strings = {"project", "database"})
	void missingImageReturnsStockWithoutWritingCouchDocument(String partition) throws Exception {
		byte[] bytes = {1, 2, 3};
		stock.when(() -> DefaultImageGeneratorUtil.pickRandomImageBytes(partition + "|Example__resource-id"))
				.thenReturn(bytes);
		Response response = CouchUtil.download(partition, Map.of(partition, "resource-id"));
		assertEquals(200, response.getStatus());
		assertArrayEquals(bytes, (byte[]) response.getEntity());
		assertEquals(List.of("POST"), methods);
	}

	@ParameterizedTest
	@ValueSource(strings = {"project", "database"})
	void localImageStillMigratesToCouch(String partition) throws Exception {
		File uploaded = Files.write(temp.resolve("image.png"), new byte[] {4, 5, 6}).toFile();
		images.when(() -> InsightUtility.findImageFile(nullable(String.class))).thenReturn(new File[] {uploaded});
		images.when(() -> InsightUtility.findImageFile(nullable(String.class), eq("resource-id")))
				.thenReturn(new File[] {uploaded});
		Response response = CouchUtil.download(partition, Map.of(partition, "resource-id"));
		assertArrayEquals(new byte[] {4, 5, 6}, (byte[]) response.getEntity());
		assertEquals(List.of("POST", "PUT"), methods);
		stock.verifyNoInteractions();
	}

	@ParameterizedTest
	@ValueSource(strings = {"project", "database"})
	void existingAttachmentTakesPrecedenceOverStock(String partition) throws Exception {
		findBody = "{\"docs\":[{\"_id\":\"existing\",\"_attachments\":{\"image.png\":{}}}]}";
		Response response = CouchUtil.download(partition, Map.of(partition, "resource-id"));
		assertArrayEquals(new byte[] {7, 8, 9}, (byte[]) response.getEntity());
		assertEquals(List.of("POST", "GET"), methods);
		stock.verifyNoInteractions();
	}
}
