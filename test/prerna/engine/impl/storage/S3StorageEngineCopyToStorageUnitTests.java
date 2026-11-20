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
package prerna.engine.impl.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Covers copyToStorage's upload result and its best effort folder marker
 * cleanup, against a mocked S3Client.
 */
class S3StorageEngineCopyToStorageUnitTests {

	private static final String BUCKET = "test-bucket";
	private static final String FOLDER = "/SemossUploads/";
	private static final String UPLOADED_KEY = "SemossUploads/Mail.csv";

	@TempDir
	Path tempDir;

	private S3Client client;
	private S3StorageEngine engine;
	private Path localFile;

	@BeforeEach
	void setUp() throws Exception {
		client = mock(S3Client.class);
		engine = new S3StorageEngine();
		setField(engine, "client", client);
		setField(engine, "bucket", BUCKET);
		localFile = Files.writeString(tempDir.resolve("Mail.csv"), "a,b\n1,2\n");

		when(client.putObject(any(PutObjectRequest.class), any(Path.class)))
				.thenReturn(PutObjectResponse.builder().versionId("v1").build());
		when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(DeleteObjectResponse.builder().build());
	}

	@Test
	void uploadSucceedsWithNoMarker() throws Exception {
		stubListing(obj(UPLOADED_KEY, 8));

		assertEquals("v1", engine.copyToStorage(localFile.toString(), FOLDER, Map.of()));
		verify(client, never()).deleteObject(any(DeleteObjectRequest.class));
	}

	@Test
	void uploadSucceedsAndRemovesMarker() throws Exception {
		stubListing(obj("SemossUploads/", 0), obj(UPLOADED_KEY, 8));

		assertEquals("v1", engine.copyToStorage(localFile.toString(), FOLDER, Map.of()));
		assertEquals(List.of("SemossUploads/"), deletedKeys());
	}

	@Test
	void uploadSucceedsWhenMarkerDeleteIsDenied() throws Exception {
		stubListing(obj("SemossUploads/", 0), obj(UPLOADED_KEY, 8));
		when(client.deleteObject(any(DeleteObjectRequest.class))).thenThrow(S3Exception.builder().statusCode(403)
				.message("not authorized to perform s3:DeleteObject").build());

		assertEquals("v1", engine.copyToStorage(localFile.toString(), FOLDER, Map.of()));
		verify(client).putObject(any(PutObjectRequest.class), any(Path.class));
	}

	@Test
	void uploadSucceedsWhenListingIsDenied() throws Exception {
		when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenThrow(S3Exception.builder()
				.statusCode(403).message("not authorized to perform s3:ListBucket").build());

		assertEquals("v1", engine.copyToStorage(localFile.toString(), FOLDER, Map.of()));
	}

	@Test
	void failedPutFailsTheCopyAndDeletesNothing() throws Exception {
		// retryOperation backs off 2s + 4s before giving up, so this test takes ~6s
		when(client.putObject(any(PutObjectRequest.class), any(Path.class)))
				.thenThrow(S3Exception.builder().statusCode(500).message("boom").build());
		// a file already lives at the target key - the failed overwrite must not remove it
		stubListing(obj(UPLOADED_KEY, 99));

		assertThrows(Exception.class, () -> engine.copyToStorage(localFile.toString(), FOLDER, Map.of()));
		verify(client, times(3)).putObject(any(PutObjectRequest.class), any(Path.class));
		verify(client, never()).deleteObject(any(DeleteObjectRequest.class));
	}

	@Test
	void partialFailureKeepsSuccessfulUploadsAndDeletesNothing() throws Exception {
		Path second = Files.writeString(tempDir.resolve("Other.csv"), "x\n");
		when(client.putObject(any(PutObjectRequest.class), any(Path.class))).thenAnswer(inv -> {
			PutObjectRequest req = inv.getArgument(0);
			if (req.key().endsWith("Other.csv")) {
				throw S3Exception.builder().statusCode(500).message("boom").build();
			}
			return PutObjectResponse.builder().versionId("v1").build();
		});
		stubListing(obj(UPLOADED_KEY, 8));

		assertThrows(Exception.class, () -> engine.copyToStorage(localFile + "," + second, FOLDER, Map.of()));
		verify(client, never()).deleteObject(any(DeleteObjectRequest.class));
	}

	@Test
	void missingLocalFileFails() throws Exception {
		stubListing();

		assertThrows(Exception.class,
				() -> engine.copyToStorage(tempDir.resolve("nope.csv").toString(), FOLDER, Map.of()));
		verify(client, never()).putObject(any(PutObjectRequest.class), any(Path.class));
	}

	@Test
	void onlyFolderMarkersAreDeleted() throws Exception {
		stubListing(obj("SemossUploads/", 0), // marker - deleted
				obj("SemossUploads/sub/", 0), // nested marker - deleted
				obj("SemossUploads/existing.csv", 42), // real file - kept
				obj("SemossUploads/empty.txt", 0), // zero byte real file, no trailing slash - kept
				obj(UPLOADED_KEY, 8));

		assertEquals("v1", engine.copyToStorage(localFile.toString(), FOLDER, Map.of()));
		assertEquals(List.of("SemossUploads/", "SemossUploads/sub/"), deletedKeys());
	}

	// --- helpers ---

	private void stubListing(S3Object... objects) {
		ListObjectsV2Iterable pages = mock(ListObjectsV2Iterable.class);
		when(pages.contents()).thenReturn(() -> List.of(objects).iterator());
		when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(pages);
	}

	private static S3Object obj(String key, long size) {
		return S3Object.builder().key(key).size(size).build();
	}

	private List<String> deletedKeys() {
		ArgumentCaptor<DeleteObjectRequest> captor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
		verify(client, atLeastOnce()).deleteObject(captor.capture());
		return captor.getAllValues().stream().map(DeleteObjectRequest::key).toList();
	}

	private static void setField(Object target, String name, Object value) throws Exception {
		Field f = S3StorageEngine.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(target, value);
	}
}
