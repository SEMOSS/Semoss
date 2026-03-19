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
package prerna.poi.main.helper.excel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.query.querystruct.ExcelQueryStruct;

class ExcelWorkbookFileHelperUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void parse_unencryptedWorkbook_getSheetsAndGetSheet_work() throws Exception {
		Path xlsx = writeSimpleWorkbook(tempDir.resolve("simple.xlsx"), List.of("Sheet1", "Sheet2"));

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(xlsx.toString(), null);

		assertEquals(xlsx.toString(), helper.getFilePath());
		assertEquals(List.of("Sheet1", "Sheet2"), helper.getSheets());
		assertNotNull(helper.getSheet("Sheet1"));
		helper.clear(); // should not throw
	}

	@Test
	void parse_missingFile_throwsRuntimeExceptionWithCause() {
		Path missing = tempDir.resolve("missing.xlsx");

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		RuntimeException ex = assertThrows(RuntimeException.class, () -> helper.parse(missing.toString(), null));

		assertEquals("Excel file not found", ex.getMessage());
		assertNotNull(ex.getCause());
		// cause type is FileNotFoundException (implementation detail), but we at least
		// ensure it exists
	}

	@Test
	void parse_encryptedWorkbook_wrongPassword_throwsHelpfulRuntimeException() throws Exception {
		Path enc = writeEncryptedWorkbook(tempDir.resolve("encrypted.xlsx"), "correct-password");

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		RuntimeException ex = assertThrows(RuntimeException.class,
				() -> helper.parse(enc.toString(), "wrong-password"));

		assertEquals("Unable to open encrypted Excel file. Please verify the password.", ex.getMessage());
		assertTrue(ex.getCause() instanceof EncryptedDocumentException);
	}

	@Test
	void parse_encryptedWorkbook_correctPassword_succeeds() throws Exception {
		Path enc = writeEncryptedWorkbook(tempDir.resolve("encrypted-ok.xlsx"), "pw123");

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(enc.toString(), "pw123");

		assertEquals(List.of("Sheet1"), helper.getSheets());
		helper.clear();
	}

	@Test
	void buildSheetIterator_smokeTest_returnsIterator() throws Exception {
		Path xlsx = writeSimpleWorkbook(tempDir.resolve("iter.xlsx"), List.of("Sheet1"));

		ExcelQueryStruct qs = new ExcelQueryStruct();
		qs.setFilePath(xlsx.toString());
		qs.setPassword(null);
		qs.setSheetName("Sheet1");
		// If your iterator requires range/types, set them here (left as-is because it
		// depends on your implementation)

		ExcelSheetFileIterator it = ExcelWorkbookFileHelper.buildSheetIterator(qs);
		assertNotNull(it);
	}

	@Test
	void clear_whenSourceFilePresent_closesIt() throws Exception {
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		FileInputStream fis = mock(FileInputStream.class); // requires mockito-inline to mock final classes
		setField(helper, "sourceFile", fis);
		setField(helper, "fileLocation", "dummy.xlsx");

		helper.clear();

		verify(fis, times(1)).close();
	}

	@Test
	void clear_whenCloseThrowsIOException_doesNotThrow() throws Exception {
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		FileInputStream fis = mock(FileInputStream.class);
		doThrow(new IOException("boom")).when(fis).close();
		setField(helper, "sourceFile", fis);
		setField(helper, "fileLocation", "dummy.xlsx");

		assertDoesNotThrow(helper::clear);
	}

	// ---------- Test helpers ----------

	private static Path writeSimpleWorkbook(Path path, List<String> sheetNames) throws Exception {
		try (XSSFWorkbook wb = new XSSFWorkbook()) {
			for (String name : sheetNames) {
				wb.createSheet(name).createRow(0).createCell(0).setCellValue("A1");
			}
			try (OutputStream out = Files.newOutputStream(path)) {
				wb.write(out);
			}
		}
		return path;
	}

	/**
	 * Creates an encrypted .xlsx using Apache POI's Agile encryption. This is an
	 * integration-style test helper; it may need small tweaks depending on your POI
	 * version.
	 */
	private static Path writeEncryptedWorkbook(Path path, String password) throws Exception {
		byte[] plainXlsx;
		try (XSSFWorkbook wb = new XSSFWorkbook(); ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			wb.createSheet("Sheet1").createRow(0).createCell(0).setCellValue("secret");
			wb.write(bos);
			plainXlsx = bos.toByteArray();
		}

		try (POIFSFileSystem fs = new POIFSFileSystem()) {
			EncryptionInfo info = new EncryptionInfo(EncryptionMode.agile);
			Encryptor enc = info.getEncryptor();
			enc.confirmPassword(password);

			try (OPCPackage opc = OPCPackage.open(new ByteArrayInputStream(plainXlsx));
					OutputStream os = enc.getDataStream(fs)) {
				opc.save(os);
			}

			try (OutputStream fileOut = Files.newOutputStream(path)) {
				fs.writeFilesystem(fileOut);
			}
		}
		return path;
	}

	private static void setField(Object target, String fieldName, Object value) throws Exception {
		Field f = target.getClass().getDeclaredField(fieldName);
		f.setAccessible(true);
		f.set(target, value);
	}
}
