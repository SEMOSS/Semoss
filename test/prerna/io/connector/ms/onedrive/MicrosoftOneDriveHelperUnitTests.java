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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hc.core5.util.Timeout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.security.HttpHelperUtility;

/**
 * A downloaded file keeps to its destination folder whatever name it arrives
 * with, and to the size cap however large it is. It remembers the file it was
 * copied from, so a later download brings the same copy up to date, a copy
 * changed here is never written over, and a copy goes back over its original
 * only while the original is unchanged.
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
	// the version Graph names for the file's contents, or null for none
	private String version;
	// the bytes a download serves
	private byte[] served = HELLO;

	@BeforeEach
	void setup() {
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> item());
		// the read of a file's state, which waits only so long
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull(),
				any(Timeout.class))).thenAnswer(call -> item());
		// a read into memory
		http.when(() -> HttpHelperUtility.getRequestBytes(anyString(), any(), isNull(), isNull(), isNull()))
				.thenReturn(HELLO);
		// a read onto disk
		http.when(() -> HttpHelperUtility.getRequestFileDownload(anyString(), any(), isNull(), isNull(), isNull(),
				anyString(), anyString())).thenAnswer(call -> {
					File file = new File(call.<String>getArgument(5), call.<String>getArgument(6));
					try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
						out.write(served);
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

	@Test
	void aDownloadSaysWhenTheFileLastChangedAndRecordsWhereItCameFrom() throws Exception {
		version = "c1";

		Map<String, Object> result = download();
		assertEquals("2026-11-10T14:02:00Z", result.get("lastModifiedDateTime"));
		assertEquals("Jane Doe", result.get("lastModifiedBy"));
		MicrosoftOneDriveSources.Source source = MicrosoftOneDriveSources.read(copy());
		assertEquals("d1", source.driveId());
		assertEquals("i1", source.itemId());
		assertEquals("c1", source.version());
		assertEquals("Jane Doe", source.lastModifiedBy());
		assertTrue(MicrosoftOneDriveSources.isUntouched(copy(), source));
	}

	@Test
	void downloadingAgainBringsTheSameCopyUpToDateRatherThanWritingASecond() throws Exception {
		version = "c1";
		download();

		// the same version: nothing is fetched, and the name asked for is not used
		assertEquals("Recording.mp4", downloadAs("Recording (2).mp4").get("filePath"));
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(any(), any(), any(), any(), any(), any(), any()),
				times(1));

		// a newer version is fetched over the copy
		version = "c2";
		served = "newer".getBytes(StandardCharsets.UTF_8);
		assertEquals("Recording.mp4", downloadAs("Recording (2).mp4").get("filePath"));
		assertEquals("newer", Files.readString(copy().toPath()));
		assertEquals("c2", MicrosoftOneDriveSources.read(copy()).version());
		assertEquals(List.of(".semoss", "Recording.mp4"), files());
	}

	@Test
	void aCopyChangedHereIsNeverWrittenOverByALaterDownload() throws Exception {
		version = "c1";
		download();
		Files.writeString(copy().toPath(), "edited here");

		version = "c2";
		assertEquals("Recording (2).mp4", downloadAs("Recording (2).mp4").get("filePath"));
		assertEquals("edited here", Files.readString(copy().toPath()));
		assertEquals("hello", Files.readString(folder.resolve("Recording (2).mp4")));
		// each keeps its own source: the edited one the version it was taken from
		assertEquals("c1", MicrosoftOneDriveSources.read(copy()).version());
		assertEquals("c2", MicrosoftOneDriveSources.read(folder.resolve("Recording (2).mp4").toFile()).version());
	}

	@Test
	void aCopyGoesBackOverItsOriginalWhileTheOriginalIsUnchanged() throws Exception {
		version = "c1";
		download();
		Files.writeString(copy().toPath(), "edited here");
		// what OneDrive answers the upload with
		http.when(() -> HttpHelperUtility.putRequestBytesBody(anyString(), anyMap(), any(), any(), isNull(), isNull(),
				isNull())).thenAnswer(call -> {
					version = "c2";
					return item();
				});

		Map<String, Object> saved = MicrosoftOneDriveHelper.saveBack("token", copy(), false);
		assertEquals("i1", saved.get("id"));
		assertEquals(true, saved.get("success"));
		// the file itself, by its id, with no name that could rename it
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(
				eq("https://graph.microsoft.com/v1.0/drives/d1/items/i1/content"),
				eq(Map.of("Authorization", "Bearer token")),
				aryEq("edited here".getBytes(StandardCharsets.UTF_8)), any(), isNull(), isNull(), isNull()));
		// the copy is the original again, so the next change on either side shows
		MicrosoftOneDriveSources.Source source = MicrosoftOneDriveSources.read(copy());
		assertEquals("c2", source.version());
		assertTrue(MicrosoftOneDriveSources.isUntouched(copy(), source));
	}

	@Test
	void aLargeCopyGoesBackInPiecesToTheFileItselfAndKeepsItsName() throws Exception {
		version = "c1";
		download();
		// a deck past the 4 MB a single request takes; sparse, so nothing that size is written
		try (RandomAccessFile deck = new RandomAccessFile(copy(), "rw")) {
			deck.setLength(5L * 1024L * 1024L);
		}
		http.when(() -> HttpHelperUtility.postRequestStringBody(anyString(), anyMap(), anyString(), any(), isNull(),
				isNull(), isNull())).thenReturn("{\"uploadUrl\":\"https://upload.example/session\"}");
		http.when(() -> HttpHelperUtility.putRequestBytesBody(eq("https://upload.example/session"), anyMap(), any(),
				any(), isNull(), isNull(), isNull())).thenAnswer(call -> {
					// only the last piece answers with the file
					String range = call.<Map<String, String>>getArgument(1).get("Content-Range");
					version = "c2";
					return range.endsWith("-5242879/5242880") ? item() : "";
				});

		assertEquals(true, MicrosoftOneDriveHelper.saveBack("token", copy(), false).get("success"));
		// by its id, to be replaced, and with no name that could rename it
		http.verify(() -> HttpHelperUtility.postRequestStringBody(
				eq("https://graph.microsoft.com/v1.0/drives/d1/items/i1/createUploadSession"), anyMap(),
				eq("{\"item\":{\"@microsoft.graph.conflictBehavior\":\"replace\"}}"), any(), isNull(), isNull(),
				isNull()));
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(eq("https://upload.example/session"), anyMap(), any(),
				any(), isNull(), isNull(), isNull()), times(2));
		assertEquals("c2", MicrosoftOneDriveSources.read(copy()).version());
	}

	@Test
	void aLargeCopyOneDriveRefusesHasItsUploadSessionCancelledAndTheRefusalIsWhatIsThrown() throws Exception {
		version = "c1";
		download();
		try (RandomAccessFile deck = new RandomAccessFile(copy(), "rw")) {
			deck.setLength(5L * 1024L * 1024L);
		}
		http.when(() -> HttpHelperUtility.postRequestStringBody(anyString(), anyMap(), anyString(), any(), isNull(),
				isNull(), isNull())).thenReturn("{\"uploadUrl\":\"https://upload.example/session\"}");
		// the deck is open in PowerPoint
		http.when(() -> HttpHelperUtility.putRequestBytesBody(eq("https://upload.example/session"), anyMap(), any(),
				any(), isNull(), isNull(), isNull())).thenThrow(
						new IllegalArgumentException("PUT request to https://upload.example/session returned HTTP 423."));
		// and a cancel that fails too does not hide why the upload did
		http.when(() -> HttpHelperUtility.deleteRequestStringBody(anyString(), isNull(), isNull(), isNull(), isNull()))
				.thenThrow(new IllegalArgumentException("DELETE request returned HTTP 500."));

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> MicrosoftOneDriveHelper.saveBack("token", copy(), false));
		// the status is still there to be read, and the url, which signs the request, is not
		assertEquals("PUT request to the upload session returned HTTP 423.", refused.getMessage());
		// left open, the session has OneDrive refuse every later save of the deck with a 409
		http.verify(() -> HttpHelperUtility.deleteRequestStringBody(eq("https://upload.example/session"), isNull(),
				isNull(), isNull(), isNull()));
		// the copy still waits to go back
		assertEquals("c1", MicrosoftOneDriveSources.read(copy()).version());
	}

	@Test
	void aFailedFetchOfALinkThatSignsItselfDoesNotQuoteTheLink() {
		String link = "https://files.example/download?tempauth=secret";
		http.when(() -> HttpHelperUtility.getRequestFileDownload(eq(link), any(), isNull(), isNull(), isNull(),
				anyString(), anyString()))
				.thenThrow(new IllegalArgumentException("GET request to " + link + " returned HTTP 503"));
		File deck = folder.resolve("Deck.pptx").toFile();

		// fetched with no headers, the link is all it takes to read the file
		assertEquals("GET request to the download link returned HTTP 503",
				assertThrows(IllegalArgumentException.class,
						() -> MicrosoftOneDriveHelper.download(link, null, deck, "Deck.pptx")).getMessage());
		// fetched with the owner's token, it is an address like any other
		assertEquals("GET request to " + link + " returned HTTP 503",
				assertThrows(IllegalArgumentException.class, () -> MicrosoftOneDriveHelper.download(link,
						Map.of("Authorization", "Bearer token"), deck, "Deck.pptx")).getMessage());
	}

	@Test
	void aCopyIsNotSavedOverAnOriginalThatChangedUntilTheOwnerWhoWasToldAsksAgain() throws Exception {
		version = "c1";
		download();
		Files.writeString(copy().toPath(), "edited here");
		http.when(() -> HttpHelperUtility.putRequestBytesBody(anyString(), anyMap(), any(), any(), isNull(), isNull(),
				isNull())).thenAnswer(call -> item());

		// the first time it is found changed it is refused, whoever asks
		version = "c2";
		MicrosoftOneDriveHelper.SaveBackRefused changed = assertThrows(
				MicrosoftOneDriveHelper.SaveBackRefused.class,
				() -> MicrosoftOneDriveHelper.saveBack("token", copy(), true));
		assertEquals("Recording.mp4 was changed in OneDrive after this copy was taken (last changed "
				+ "2026-11-10T14:02:00Z by Jane Doe), so it was not saved over.", changed.getMessage());
		assertEquals(false, changed.alreadyTold());

		// a save nobody asked for never writes over it, and has nothing new to say
		MicrosoftOneDriveHelper.SaveBackRefused again = assertThrows(MicrosoftOneDriveHelper.SaveBackRefused.class,
				() -> MicrosoftOneDriveHelper.saveBack("token", copy(), false));
		assertEquals(true, again.alreadyTold());
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(any(), any(), any(), any(), any(), any(), any()),
				never());
		assertEquals("edited here", Files.readString(copy().toPath()));

		// the owner was told of that change and asks again: their copy replaces it
		assertEquals(true, MicrosoftOneDriveHelper.saveBack("token", copy(), true).get("success"));
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(
				eq("https://graph.microsoft.com/v1.0/drives/d1/items/i1/content"), anyMap(),
				aryEq("edited here".getBytes(StandardCharsets.UTF_8)), any(), isNull(), isNull(), isNull()));

		// a later change is news again
		Files.writeString(copy().toPath(), "edited once more");
		version = "c3";
		assertThrows(MicrosoftOneDriveHelper.SaveBackRefused.class,
				() -> MicrosoftOneDriveHelper.saveBack("token", copy(), true));
	}

	@Test
	void aCopyWhoseOriginalIsGoneIsRefusedOnceAndIsAnOrdinaryFileAfterThat() throws Exception {
		version = "c1";
		download();
		Files.writeString(copy().toPath(), "edited here");
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull(),
				any(Timeout.class))).thenThrow(new IllegalArgumentException(
						"GET request to https://graph.microsoft.com/v1.0/drives/d1/items/i1 returned HTTP 404. Response body: {}"));

		MicrosoftOneDriveHelper.SaveBackRefused gone = assertThrows(MicrosoftOneDriveHelper.SaveBackRefused.class,
				() -> MicrosoftOneDriveHelper.saveBack("token", copy(), false));
		assertEquals("The original of Recording.mp4 is no longer in OneDrive, so it was not saved back.",
				gone.getMessage());
		assertNull(MicrosoftOneDriveSources.read(copy()));
		assertEquals("edited here", Files.readString(copy().toPath()));
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(any(), any(), any(), any(), any(), any(), any()),
				never());
	}

	@Test
	void aDownloadThatNamesNoFileLandsBesideACopyChangedHereAndOneThatNamesItWritesOverIt() throws Exception {
		version = "c1";
		download();
		Files.writeString(copy().toPath(), "edited here");

		// "download it again" must not cost the changes OneDrive does not have
		version = "c2";
		served = "newer".getBytes(StandardCharsets.UTF_8);
		assertEquals("Recording (2).mp4", download().get("filePath"));
		assertEquals("edited here", Files.readString(copy().toPath()));
		assertEquals("newer", Files.readString(folder.resolve("Recording (2).mp4")));

		// naming the copy is the choice to take OneDrive's version
		assertEquals("Recording.mp4", downloadAs("Recording.mp4").get("filePath"));
		assertEquals("newer", Files.readString(copy().toPath()));
		assertTrue(MicrosoftOneDriveSources.isUntouched(copy(), MicrosoftOneDriveSources.read(copy())));
	}

	@Test
	void aFileThatWasNotDownloadedHasNoOriginalToGoBackOver() throws Exception {
		Files.writeString(copy().toPath(), "made here");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> MicrosoftOneDriveHelper.saveBack("token", copy(), true));
		assertEquals("Recording.mp4 is not a copy of a OneDrive file, so there is no original to save it over.",
				refused.getMessage());
		http.verify(() -> HttpHelperUtility.getRequest(any(), any(), any(), any(), any(), any()), never());
	}

	@Test
	void anUploadThatReplacesGoesBackOverTheOriginalOfACopyHoweverItIsAimedThere() throws Exception {
		version = "c1";
		download();
		Files.writeString(copy().toPath(), "edited here");
		http.when(() -> HttpHelperUtility.putRequestBytesBody(anyString(), anyMap(), any(), any(), isNull(), isNull(),
				isNull())).thenAnswer(call -> item());

		// aimed nowhere: not the drive root, but the file it was copied from
		upload(null, "replace");
		// aimed at the original by its folder: the same file, so the same check
		upload("Recordings/2026", "replace");
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(
				eq("https://graph.microsoft.com/v1.0/drives/d1/items/i1/content"), anyMap(), any(), any(), isNull(),
				isNull(), isNull()), times(2));

		// and refused the same way once the original has changed, with what to do about it
		Files.writeString(copy().toPath(), "edited again");
		version = "c2";
		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> upload("Recordings/2026", "replace"));
		assertEquals("Recording.mp4 was changed in OneDrive after this copy was taken (last changed "
				+ "2026-11-10T14:02:00Z by Jane Doe), so it was not saved over. Nothing was uploaded. Tell the owner; "
				+ "if they still want this file there, upload it again the same way.", refused.getMessage());
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(any(), any(), any(), any(), any(), any(), any()),
				times(2));

		// the owner was told and asks again
		upload("Recordings/2026", "replace");
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(any(), any(), any(), any(), any(), any(), any()),
				times(3));
	}

	@Test
	void anUploadOfACopyToSomewhereElseOrWithoutReplacingIsAnOrdinaryUpload() throws Exception {
		version = "c1";
		download();
		http.when(() -> HttpHelperUtility.putRequestBytesBody(anyString(), anyMap(), any(), any(), isNull(), isNull(),
				isNull())).thenAnswer(call -> item());

		// the default keeps both files, so there is nothing to check
		upload(null, null);
		http.verify(() -> HttpHelperUtility.getRequest(any(), any(), any(), any(), any(), any()), never());
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(eq(
				"https://graph.microsoft.com/v1.0/me/drive/root:/Recording.mp4:/content?@microsoft.graph.conflictBehavior=rename"),
				anyMap(), any(), any(), isNull(), isNull(), isNull()));

		// replacing a different file that happens to share the name
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull(),
				any(Timeout.class))).thenReturn("{\"id\":\"i2\",\"name\":\"Recording.mp4\",\"cTag\":\"x9\"}");
		upload("Archive", "replace");
		http.verify(() -> HttpHelperUtility.putRequestBytesBody(eq(
				"https://graph.microsoft.com/v1.0/me/drive/root:/Archive/Recording.mp4:/content?@microsoft.graph.conflictBehavior=replace"),
				anyMap(), any(), any(), isNull(), isNull(), isNull()));
	}

	@Test
	void aRecordWhoseCopyIsGoneIsForgottenSoANewFileOfThatNameIsNotMistakenForIt() throws Exception {
		version = "c1";
		download();
		Files.delete(copy().toPath());

		assertEquals(Map.of(), MicrosoftOneDriveSources.list(folder.toFile()));
		Files.writeString(copy().toPath(), "made here");
		assertNull(MicrosoftOneDriveSources.read(copy()));
	}

	private Map<String, Object> download() throws Exception {
		return downloadAs(null);
	}

	private Map<String, Object> downloadAs(String fileName) throws Exception {
		return MicrosoftOneDriveHelper.downloadFile("token", null, "i1", null, null, folder.toString(), fileName);
	}

	private File copy() {
		return folder.resolve("Recording.mp4").toFile();
	}

	private Map<String, Object> upload(String folderPath, String conflictBehavior) throws Exception {
		return MicrosoftOneDriveHelper.uploadFile("token", null, null, folderPath, "Recording.mp4",
				copy().getAbsolutePath(), conflictBehavior);
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
		if (version != null) {
			item.put("cTag", version);
			item.put("lastModifiedDateTime", "2026-11-10T14:02:00Z");
			item.put("lastModifiedBy", Map.of("user", Map.of("displayName", "Jane Doe")));
		}
		return new Gson().toJson(item);
	}

	private List<String> files() throws Exception {
		try (Stream<Path> listed = Files.list(folder)) {
			return listed.map(path -> path.getFileName().toString()).sorted().toList();
		}
	}
}
