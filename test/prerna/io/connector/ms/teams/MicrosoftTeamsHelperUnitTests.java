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
package prerna.io.connector.ms.teams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.security.HttpHelperUtility;

/** Channel files are listed across Graph's pages, and checked against the size cap before one is fetched. */
class MicrosoftTeamsHelperUnitTests {

	@TempDir
	Path folder;

	private MockedStatic<HttpHelperUtility> http;
	// the size Graph lists for the file
	private long listedSize;

	@BeforeEach
	void setup() {
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> metadata());
		// a read into memory
		http.when(() -> HttpHelperUtility.getRequestBytes(anyString(), any(), isNull(), isNull(), isNull()))
				.thenReturn("hello".getBytes(StandardCharsets.UTF_8));
		// a read onto disk
		http.when(() -> HttpHelperUtility.getRequestFileDownload(anyString(), any(), isNull(), isNull(), isNull(),
				anyString(), anyString())).thenAnswer(call -> Files
						.writeString(Path.of(call.<String>getArgument(5), call.<String>getArgument(6)), "hello").toFile());
	}

	@AfterEach
	void cleanup() {
		http.close();
	}

	@Test
	void aChannelFileOverTheCapIsRefusedBeforeAnythingIsFetched() throws Exception {
		listedSize = 100L * 1024L * 1024L + 1;

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, this::download);
		assertEquals("Minutes.docx is larger than the 100 MB limit for a download.", refused.getMessage());
		http.verify(() -> HttpHelperUtility.getRequestBytes(any(), any(), any(), any(), any()), never());
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(any(), any(), any(), any(), any(), any(), any()),
				never());
		assertEquals(List.of(), files());
	}

	@Test
	void aChannelFileIsStreamedToItsNameAndAnswersAsBefore() throws Exception {
		listedSize = 5;

		assertEquals(Map.of("id", "f1", "name", "Minutes.docx", "filePath", "Minutes.docx", "success", true),
				download());
		assertEquals("hello", Files.readString(folder.resolve("Minutes.docx")));
		assertEquals(List.of("Minutes.docx"), files());
		http.verify(() -> HttpHelperUtility.getRequestBytes(any(), any(), any(), any(), any()), never());
	}

	@Test
	void aChannelsFilesAreListedAcrossEveryPageGraphAnswers() throws Exception {
		List<String> read = new ArrayList<>();
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					String url = call.getArgument(0);
					read.add(url);
					if (url.endsWith("/filesFolder")) {
						return new Gson().toJson(
								Map.of("id", "root", "name", "General", "parentReference", Map.of("driveId", "d1")));
					}
					if (url.endsWith("/children")) {
						return new Gson().toJson(Map.of("value", List.of(Map.of("id", "f1", "name", "a.docx")),
								"@odata.nextLink", "https://graph.microsoft.com/v1.0/drives/d1/items/root/children?page=2"));
					}
					return new Gson().toJson(Map.of("value", List.of(Map.of("id", "f2", "name", "b.docx"))));
				});

		assertEquals(List.of("f1", "f2"), ids(MicrosoftTeamsHelper.listChannelFiles("token", "t1", "c1", null, 0)));
		read.clear();
		assertEquals(List.of("f1"), ids(MicrosoftTeamsHelper.listChannelFiles("token", "t1", "c1", null, 1)));
		// a limit met on the first page reads no further
		assertEquals(2, read.size());
	}

	private static List<Object> ids(List<Map<String, Object>> items) {
		return items.stream().map(item -> item.get("id")).toList();
	}

	private Map<String, Object> download() throws Exception {
		return MicrosoftTeamsHelper.downloadFile("token", null, null, "d1", "f1", folder.toString(), null);
	}

	private String metadata() {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", "f1");
		item.put("name", "Minutes.docx");
		item.put("size", listedSize);
		item.put("@microsoft.graph.downloadUrl", "https://download.example/f1");
		return new Gson().toJson(item);
	}

	private List<String> files() throws Exception {
		try (Stream<Path> listed = Files.list(folder)) {
			return listed.map(path -> path.getFileName().toString()).sorted().toList();
		}
	}
}
