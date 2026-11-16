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
package prerna.io.connector.ms.outlook;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

class MicrosoftOutlookDownloadAttachmentReactorUnitTests {

	@TempDir
	Path folder;

	private MockedStatic<HttpHelperUtility> http;
	private MockedStatic<MicrosoftLoginUtils> login;
	private Insight insight;
	// the one file attachment Graph lists on the message, and the size it lists
	// for it, or null for none
	private String name;
	private byte[] bytes;
	private Long listedSize;
	// whether its bytes run past the cap or break off part way, and where they
	// were downloaded to
	private boolean oversized;
	private boolean broken;
	private File served;

	@BeforeEach
	void setup() {
		AccessToken token = new AccessToken();
		token.setAccess_token("test-token");
		User user = mock(User.class);
		when(user.getAccessToken(AuthProvider.MICROSOFT)).thenReturn(token);
		insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		when(insight.getInsightFolder()).thenReturn(folder.toString());
		login = mockStatic(MicrosoftLoginUtils.class);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any())).thenReturn("test-token");
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					Map<String, Object> summary = new LinkedHashMap<>(
							Map.of("id", "a1", "name", name, "@odata.type", "#microsoft.graph.fileAttachment"));
					if (listedSize != null) {
						summary.put("size", listedSize);
					}
					return new Gson().toJson(Map.of("value", List.of(summary)));
				});
		// the raw bytes ($value), onto disk
		http.when(() -> HttpHelperUtility.getRequestFileDownload(anyString(), anyMap(), isNull(), isNull(),
				isNull(), anyString(), anyString())).thenAnswer(call -> {
					served = new File(call.<String>getArgument(5), call.<String>getArgument(6));
					try (RandomAccessFile out = new RandomAccessFile(served, "rw")) {
						out.write(bytes);
						if (oversized) {
							// sparse, so nothing that size is written
							out.setLength(100L * 1024L * 1024L + 1);
						}
					}
					if (broken) {
						throw new IllegalArgumentException("the connection dropped");
					}
					return served;
				});
	}

	@AfterEach
	void cleanup() {
		http.close();
		login.close();
	}

	private Map<?, ?> download(boolean includeText) {
		MicrosoftOutlookDownloadAttachmentReactor reactor = new MicrosoftOutlookDownloadAttachmentReactor();
		reactor.setInsight(insight);
		NounStore store = new NounStore("all");
		store.makeNoun("id").addLiteral("m1");
		store.makeNoun("attachmentId").addLiteral("a1");
		store.makeNoun("fileName").addLiteral("k1-" + name);
		store.makeNoun("includeText").addBoolean(includeText);
		reactor.setNounStore(store);
		return (Map<?, ?>) reactor.execute().getValue();
	}

	private String text(Map<?, ?> output) throws Exception {
		return Files.readString(folder.resolve(String.valueOf(output.get("textPath"))), StandardCharsets.UTF_8);
	}

	@Test
	void theRawBytesAreSavedAsTheyCameAndTheirTemporaryFileIsGone() throws Exception {
		name = "Notes.txt";
		bytes = "raw, not base64".getBytes(StandardCharsets.UTF_8);
		listedSize = (long) bytes.length;

		Map<?, ?> output = download(false);
		assertEquals("k1-Notes.txt", output.get("filePath"));
		assertEquals(bytes.length, output.get("size"));
		assertArrayEquals(bytes, Files.readAllBytes(folder.resolve("k1-Notes.txt")));
		assertFalse(served.exists());
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(endsWith("/messages/m1/attachments/a1/$value"),
				anyMap(), isNull(), isNull(), isNull(), anyString(), anyString()));
	}

	@Test
	void anAttachmentListedOverTheCapIsRefusedBeforeItIsFetched() {
		name = "Recording.mp4";
		listedSize = 100L * 1024L * 1024L + 1;

		SemossPixelException refused = assertThrows(SemossPixelException.class, () -> download(false));
		assertEquals("Recording.mp4 is larger than the 100 MB limit for a download.", refused.getMessage());
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(any(), any(), any(), any(), any(), any(), any()),
				never());
		assertEquals(0, folder.toFile().list().length);
	}

	@Test
	void anAttachmentWhoseBytesRunPastTheCapIsDeletedAndRefused() {
		name = "Recording.mp4";
		bytes = new byte[0];
		oversized = true;

		SemossPixelException refused = assertThrows(SemossPixelException.class, () -> download(false));
		assertEquals("Recording.mp4 is larger than the 100 MB limit for a download.", refused.getMessage());
		assertFalse(served.exists());
		assertEquals(0, folder.toFile().list().length);
	}

	@Test
	void anAttachmentWhoseDownloadBreaksPartWayLeavesNothingBehind() {
		name = "Notes.txt";
		bytes = "part of it".getBytes(StandardCharsets.UTF_8);
		broken = true;

		SemossPixelException failed = assertThrows(SemossPixelException.class, () -> download(false));
		assertEquals("the connection dropped", failed.getMessage());
		assertFalse(served.exists());
		assertEquals(0, folder.toFile().list().length);
	}

	@Test
	void anOfficeFileGetsATextCopyBesideItOnlyWhenAskedFor() throws Exception {
		name = "Plan.docx";
		try (XWPFDocument doc = new XWPFDocument(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			doc.createParagraph().createRun().setText("Scope of work");
			doc.write(out);
			bytes = out.toByteArray();
		}
		Map<?, ?> plain = download(false);
		assertNull(plain.get("textPath"));
		assertFalse(Files.exists(folder.resolve("k1-Plan.docx.txt")));

		Map<?, ?> output = download(true);
		assertEquals("k1-Plan.docx", output.get("filePath"));
		assertEquals("k1-Plan.docx.txt", output.get("textPath"));
		assertTrue(text(output).startsWith("Attachment: Plan.docx\n\nScope of work"), text(output));
	}

	@Test
	void mailAndSlideFilesAreReadAsTheBrainReadsThem() throws Exception {
		name = "Forwarded.eml";
		MimeMessage message = new MimeMessage(Session.getInstance(new Properties()));
		message.setFrom(new InternetAddress("jane@example.com", "Jane Doe"));
		message.setSubject("Contract review");
		MimeBodyPart body = new MimeBodyPart();
		body.setText("Please sign by Monday.");
		MimeBodyPart attachment = new MimeBodyPart();
		attachment.setContent(new byte[] { 1, 2, 3 }, "application/octet-stream");
		attachment.setFileName("contract.pdf");
		attachment.setDisposition(MimeBodyPart.ATTACHMENT);
		message.setContent(new MimeMultipart(body, attachment));
		message.saveChanges();
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			message.writeTo(out);
			bytes = out.toByteArray();
		}
		String mail = text(download(true));
		assertTrue(mail.contains("From: Jane Doe <jane@example.com>\n"), mail);
		assertTrue(mail.contains("Subject: Contract review\n\nPlease sign by Monday."), mail);
		assertTrue(mail.endsWith("Attached to this email: contract.pdf"), mail);

		name = "Deck.pptx";
		try (XMLSlideShow show = new XMLSlideShow(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			XSLFSlide slide = show.createSlide();
			slide.createTextBox().setText("Roadmap 2027");
			show.getNotesSlide(slide).getPlaceholder(1).setText("Mention the hiring plan");
			show.write(out);
			bytes = out.toByteArray();
		}
		String deck = text(download(true));
		assertTrue(deck.contains("Roadmap 2027") && deck.contains("Mention the hiring plan"), deck);
	}

	@Test
	void aFileWhoseTextCannotBeReadIsStillSavedAndSaysWhy() {
		name = "Budget.xlsx";
		bytes = "not a workbook".getBytes(StandardCharsets.UTF_8);
		Map<?, ?> output = download(true);
		assertEquals("The text of this file could not be read.", output.get("textError"));
		assertNull(output.get("textPath"));
		assertTrue(Files.isRegularFile(folder.resolve("k1-Budget.xlsx")));
	}
}
