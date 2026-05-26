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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

import prerna.SemossUnitTest;

public class ExcelUtilityUnitTests extends SemossUnitTest {

	@Test
	void test_isExcelEncrypted() throws Exception {
		// write file
		Path filePath = tempDir.resolve("test.xlsx");
		String password = "password123";
		SXSSFWorkbook wb = new SXSSFWorkbook();
		ExcelUtility.writeToFile(wb, filePath.toString());

		// test that the file exists
		File excelFile = new File(filePath.toString());
		assertTrue(excelFile.exists());

		// test if the file is encrypted
		assertFalse(ExcelUtility.isExcelEncrypted(filePath.toString()));

		wb = new SXSSFWorkbook();
		ExcelUtility.encrypt(wb, filePath.toString(), password);
		assertTrue(ExcelUtility.isExcelEncrypted(filePath.toString()));
	}

	@Test
	void test_whenFileIsOle2CompoundDocument() throws Exception {
		// create an OLE2/POIFS file
		Path ole2File = tempDir.resolve("sample.xls");
		try (POIFSFileSystem fs = new POIFSFileSystem(); OutputStream out = new FileOutputStream(ole2File.toFile())) {

			fs.createDocument(new ByteArrayInputStream(new byte[] { 1, 2, 3, 4 }), "Workbook");
			fs.writeFilesystem(out);
		}

		// Act
		boolean result = ExcelUtility.isExcelEncrypted(ole2File.toString());

		// Assert
		assertTrue(result);
	}

	@Test
	void test_writeToFile_SXSSFWorkbook() throws Exception {
		// write file
		Path filePath = tempDir.resolve("test.xlsx");
		try (SXSSFWorkbook wb = new SXSSFWorkbook()) {
			ExcelUtility.writeToFile(wb, filePath.toString());
		}

		// test that the file exists
		File excelFile = new File(filePath.toString());
		assertTrue(excelFile.exists());

		// test if the file is encrypted
		boolean result = ExcelUtility.isExcelEncrypted(filePath.toString());
		assertFalse(result);
	}

	@Test
	void test_writeToFile() throws Exception {
		// write file
		Path filePath = tempDir.resolve("test.xlsx");
		try (XSSFWorkbook wb = new XSSFWorkbook()) {
			ExcelUtility.writeToFile(wb, filePath.toString());
		}

		// test that the file exists
		File excelFile = new File(filePath.toString());
		assertTrue(excelFile.exists());

		// test if the file is encrypted
		boolean result = ExcelUtility.isExcelEncrypted(filePath.toString());
		assertFalse(result);
	}

	@Test
	void throwsIllegalArgumentException_whenIOExceptionOccurs() {
		// Arrange: guaranteed I/O failure (missing file) -> FileNotFoundException (an
		// IOException)
		Path missing = tempDir.resolve("does-not-exist.xlsx");

		// Act + Assert
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> ExcelUtility.isExcelEncrypted(missing.toString()));
		assertEquals("Could not handle file location. See logs for details.", ex.getMessage());
	}

}
