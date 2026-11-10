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
package prerna.io.connector.ms.onedrive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

/**
 * A downloaded file keeps to its destination folder whatever name it arrives
 * with, and to the size cap however large it is.
 */
class MicrosoftOneDriveHelperUnitTests {

	private static final long CAP = 100L * 1024L * 1024L;
	private static final byte[] HELLO = "hello".getBytes(StandardCharsets.UTF_8);

	@TempDir
	Path folder;

	private MockedStatic<HttpHelperUtility> http;
	// the size Graph lists for the file, or null for none
	private Long listedSize;
	// whether the bytes served run past the cap, or break off part way
	private boolean oversized;
	private boolean broken;

	@BeforeEach
	void setup() {
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> item());
		// a read into memory
		http.when(() -> HttpHelperUtility.getRequestBytes(anyString(), any(), isNull(), isNull(), isNull()))
				.thenReturn(HELLO);
		// a read onto disk
		http.when(() -> HttpHelperUtility.getRequestFileDownload(anyString(), any(), isNull(), isNull(), isNull(),
				anyString(), anyString())).thenAnswer(call -> {
					File file = new File(call.<String>getArgument(5), call.<String>getArgument(6));
					try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
						out.write(HELLO);
						if (oversized) {
							// sparse, so nothing that size is written
							out.setLength(CAP + 1);
						}
					}
					if (broken) {
						throw new IllegalArgumentException("the connection dropped");
					}
					return file;
				});
	}

	@AfterEach
	void cleanup() {
		http.close();
	}

	@Test
	void aNameCarryingAPathKeepsOnlyItsLastPart() {
		assertEquals("report.pdf", MicrosoftOneDriveHelper.baseName("report.pdf"));
		assertEquals("AGENTS.md", MicrosoftOneDriveHelper.baseName("../../../../AGENTS.md"));
		assertEquals("evil.sh", MicrosoftOneDriveHelper.baseName("..\\..\\bin\\evil.sh"));
		assertEquals("x.txt", MicrosoftOneDriveHelper.baseName("/etc/cron.d/x.txt"));
	}

	@Test
	void aNameWithNoFilePartIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> MicrosoftOneDriveHelper.baseName(".."));
		assertThrows(IllegalArgumentException.class, () -> MicrosoftOneDriveHelper.baseName("folder/"));
		assertThrows(IllegalArgumentException.class, () -> MicrosoftOneDriveHelper.baseName("../."));
	}

	@Test
	void aFileGraphListsOverTheCapIsRefusedBeforeAnythingIsFetched() throws Exception {
		listedSize = CAP + 1;

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, this::download);
		assertEquals("Recording.mp4 is larger than the 100 MB limit for a download.", refused.getMessage());
		http.verify(() -> HttpHelperUtility.getRequestBytes(any(), any(), any(), any(), any()), never());
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(any(), any(), any(), any(), any(), any(), any()),
				never());
		assertEquals(List.of(), files());
	}

	@Test
	void aDownloadThatRunsPastTheCapIsDeletedAndRefused() throws Exception {
		oversized = true;

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, this::download);
		assertEquals("Recording.mp4 is larger than the 100 MB limit for a download.", refused.getMessage());
		assertEquals(List.of(), files());
	}

	@Test
	void aDownloadThatBreaksPartWayLeavesNothingBehind() throws Exception {
		broken = true;

		IllegalArgumentException failed = assertThrows(IllegalArgumentException.class, this::download);
		assertEquals("the connection dropped", failed.getMessage());
		assertEquals(List.of(), files());
	}

	@Test
	void aFileIsStreamedOverTheOneThereAndAnswersAsBefore() throws Exception {
		listedSize = 5L;
		Files.writeString(folder.resolve("Recording.mp4"), "an older, longer copy");

		assertEquals(Map.of("id", "i1", "name", "Recording.mp4", "driveId", "d1", "filePath", "Recording.mp4", "size",
				5, "success", true), download());
		assertEquals("hello", Files.readString(folder.resolve("Recording.mp4")));
		assertEquals(List.of("Recording.mp4"), files());
		http.verify(() -> HttpHelperUtility.getRequestBytes(any(), any(), any(), any(), any()), never());
		// the pre-authenticated url is called without the bearer header
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(eq("https://download.example/i1"), isNull(),
				isNull(), isNull(), isNull(), anyString(), anyString()));
	}

	@Test
	void aNewFileIsAnEmptyUploadThatNeverReplacesOne() throws Exception {
		http.when(() -> HttpHelperUtility.putRequestBytesBody(anyString(), anyMap(), any(), any(), isNull(), isNull(),
				isNull())).thenAnswer(call -> item());

		assertEquals("i1", MicrosoftOneDriveHelper.createFile("token", null, null, "Reports/Q3 2026", " notes.txt ")
				.get("id"));
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(eq(
				"https://graph.microsoft.com/v1.0/me/drive/root:/Reports/Q3%202026/notes.txt:/content?@microsoft.graph.conflictBehavior=fail"),
				eq(Map.of("Authorization", "Bearer token")), aryEq(new byte[0]), any(), isNull(), isNull(),
				isNull()));
	}

	@Test
	void aNewFileNameThatIsTakenOrHoldsAFolderIsRefusedInPlainWords() {
		// a separator would put the file in another folder, so nothing is sent
		assertThrows(IllegalArgumentException.class,
				() -> MicrosoftOneDriveHelper.createFile("token", null, null, null, "Reports/notes.txt"));
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(any(), any(), any(), any(), any(), any(), any()),
				never());

		http.when(() -> HttpHelperUtility.putRequestBytesBody(anyString(), anyMap(), any(), any(), isNull(), isNull(),
				isNull())).thenThrow(new IllegalArgumentException(
						"PUT request to https://graph.microsoft.com/v1.0/me/drive returned HTTP 409. Response body: {}"));
		IllegalArgumentException taken = assertThrows(IllegalArgumentException.class,
				() -> MicrosoftOneDriveHelper.createFile("token", null, null, null, "notes.txt"));
		assertEquals("The name notes.txt is already taken in that folder.", taken.getMessage());
	}

	private Map<String, Object> download() throws Exception {
		return MicrosoftOneDriveHelper.downloadFile("token", null, "i1", null, null, folder.toString(), null);
	}

	private String item() {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", "i1");
		item.put("name", "Recording.mp4");
		item.put("file", Map.of("mimeType", "video/mp4"));
		item.put("parentReference", Map.of("driveId", "d1"));
		item.put("@microsoft.graph.downloadUrl", "https://download.example/i1");
		if (listedSize != null) {
			item.put("size", listedSize);
		}
		return new Gson().toJson(item);
	}

	private List<String> files() throws Exception {
		try (Stream<Path> listed = Files.list(folder)) {
			return listed.map(path -> path.getFileName().toString()).sorted().toList();
		}
	}
}
