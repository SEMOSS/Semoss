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
package prerna.reactor.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;

class WorkAttachmentTextReactorUnitTests {

	@TempDir
	Path temp;

	// the chat's folder, one level down so a file beside it is outside the chat
	private Path folder;
	private Insight insight;

	@BeforeEach
	void setup() throws Exception {
		folder = Files.createDirectories(temp.resolve("room"));
		insight = mock(Insight.class);
		when(insight.getInsightFolder()).thenReturn(folder.toString());
	}

	private Map<?, ?> text(String filePath) {
		WorkAttachmentTextReactor reactor = new WorkAttachmentTextReactor() {
			@Override
			protected User getUser() {
				return mock(User.class);
			}
		};
		reactor.setInsight(insight);
		NounStore store = new NounStore("all");
		store.makeNoun("filePath").addLiteral(filePath);
		reactor.setNounStore(store);
		return (Map<?, ?>) reactor.execute().getValue();
	}

	private static void write(XWPFDocument doc, OutputStream out, String line) throws Exception {
		doc.createParagraph().createRun().setText(line);
		doc.write(out);
	}

	private void assertRefused(String filePath, String expected) {
		RuntimeException error = assertThrows(RuntimeException.class, () -> text(filePath));
		assertEquals(expected, error.getMessage());
	}

	@Test
	void anUploadedWordFileGetsATextCopyNamingItBesideIt() throws Exception {
		try (XWPFDocument doc = new XWPFDocument(); OutputStream out = Files.newOutputStream(folder.resolve("plan.docx"))) {
			write(doc, out, "Deliver the pilot by Friday");
		}
		assertEquals(Map.of("textPath", "/plan.docx.txt"), text("/plan.docx"));
		String copy = Files.readString(folder.resolve("plan.docx.txt"), StandardCharsets.UTF_8);
		assertTrue(copy.startsWith("Attachment: plan.docx\n\n"), copy);
		assertTrue(copy.contains("Deliver the pilot by Friday"), copy);
	}

	@Test
	void aFileWhoseTextCannotBeReadIsRefusedByNameWithoutACopy() throws Exception {
		Files.write(folder.resolve("broken.docx"), "abc".getBytes(StandardCharsets.UTF_8));
		assertRefused("/broken.docx", "The text of broken.docx could not be read.");
		try (POIFSFileSystem fs = new POIFSFileSystem()) {
			Encryptor encryptor = new EncryptionInfo(EncryptionMode.agile).getEncryptor();
			encryptor.confirmPassword("secret");
			try (XWPFDocument doc = new XWPFDocument(); OutputStream out = encryptor.getDataStream(fs)) {
				write(doc, out, "Salaries");
			}
			try (OutputStream out = Files.newOutputStream(folder.resolve("pay.docx"))) {
				fs.writeFilesystem(out);
			}
		}
		assertRefused("/pay.docx", "pay.docx is password protected, so its text could not be read.");
		Biff8EncryptionKey.setCurrentUserPassword("secret");
		try (HSSFWorkbook book = new HSSFWorkbook();
				OutputStream out = Files.newOutputStream(folder.resolve("pay.xls"))) {
			book.createSheet("Pay").createRow(0).createCell(0).setCellValue("Salaries");
			book.write(out);
		} finally {
			Biff8EncryptionKey.setCurrentUserPassword(null);
		}
		assertRefused("/pay.xls", "pay.xls is password protected, so its text could not be read.");
		for (String name : List.of("broken.docx", "pay.docx", "pay.xls")) {
			assertFalse(Files.exists(folder.resolve(name + ".txt")), name);
		}
	}

	@Test
	void onlyAnOfficeOrMailFileInThisChatIsRead() throws Exception {
		Files.writeString(folder.resolve("report.pdf"), "%PDF");
		Files.writeString(temp.resolve("outside.docx"), "x");
		assertRefused("/report.pdf", "report.pdf is not a Word, Excel, PowerPoint, .msg or .eml file.");
		for (String path : List.of("../outside.docx", "/missing.docx")) {
			assertRefused(path, "No file at " + path + " in this chat.");
		}
		assertFalse(Files.exists(temp.resolve("outside.docx.txt")));
	}
}
