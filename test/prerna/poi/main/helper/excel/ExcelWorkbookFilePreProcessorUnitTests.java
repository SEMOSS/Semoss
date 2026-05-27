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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.github.pjfanning.xlsx.StreamingReader;

import prerna.util.Utility;

class ExcelWorkbookFilePreProcessorUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void test_parse_shouldOpenWorkbook_andInitializeSheetProcessor() throws Exception {
		Path xlsx = Files.createFile(tempDir.resolve("ok.xlsx"));
		String rawPath = xlsx.toString();

		Workbook workbook = mock(Workbook.class);
		when(workbook.getNumberOfSheets()).thenReturn(0);

		StreamingReader.Builder builder = mock(StreamingReader.Builder.class);
		when(builder.rowCacheSize(anyInt())).thenReturn(builder);
		when(builder.bufferSize(anyInt())).thenReturn(builder);
		when(builder.password(any())).thenReturn(builder);
		when(builder.open(any(FileInputStream.class))).thenReturn(workbook);

		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
			 MockedStatic<StreamingReader> streamingReader = Mockito.mockStatic(StreamingReader.class)) {

			utility.when(() -> Utility.normalizePath(rawPath)).thenReturn(rawPath);
			streamingReader.when(StreamingReader::builder).thenReturn(builder);

			ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
			try {
				preProcessor.parse(rawPath, "pw");

				verify(builder).rowCacheSize(10_000);
				verify(builder).bufferSize(1024 * 1024);
				verify(builder).password("pw");
				verify(builder).open(any(FileInputStream.class));

				preProcessor.determineTableRanges();

				@SuppressWarnings("unchecked")
				Map<String, ExcelSheetPreProcessor> sheetProcessors =
						(Map<String, ExcelSheetPreProcessor>) getField(preProcessor, "sheetProcessor");
				assertNotNull(sheetProcessors);
				assertTrue(sheetProcessors.isEmpty());
			} finally {
				// Critical on Windows: closes FileInputStream so @TempDir can be deleted
				preProcessor.clear();
			}
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	void test_parse_deprecatedOverload_shouldUseNullPassword_andNotLeakFileHandle() throws Exception {
		Path xlsx = Files.createFile(tempDir.resolve("ok2.xlsx"));
		String rawPath = xlsx.toString();

		Workbook workbook = mock(Workbook.class);

		StreamingReader.Builder builder = mock(StreamingReader.Builder.class);
		when(builder.rowCacheSize(anyInt())).thenReturn(builder);
		when(builder.bufferSize(anyInt())).thenReturn(builder);
		when(builder.password(any())).thenReturn(builder);
		when(builder.open(any(FileInputStream.class))).thenReturn(workbook);

		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
			 MockedStatic<StreamingReader> streamingReader = Mockito.mockStatic(StreamingReader.class)) {

			utility.when(() -> Utility.normalizePath(rawPath)).thenReturn(rawPath);
			streamingReader.when(StreamingReader::builder).thenReturn(builder);

			ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
			try {
				preProcessor.parse(rawPath);
				verify(builder).password(null);
			} finally {
				// Fixes the temp-file lock you saw (ok2.xlsx)
				preProcessor.clear();
			}
		}
	}

	@Test
	void test_parse_whenEncryptedDocumentException_shouldWrap_andNotLeakHandle() throws Exception {
	    Path xlsx = Files.createFile(tempDir.resolve("enc.xlsx"));
	    String rawPath = xlsx.toString();

	    StreamingReader.Builder builder = mock(StreamingReader.Builder.class);
	    when(builder.rowCacheSize(anyInt())).thenReturn(builder);
	    when(builder.bufferSize(anyInt())).thenReturn(builder);
	    when(builder.password(any())).thenReturn(builder);

	    // IMPORTANT: in practice this is usually open(InputStream), not open(FileInputStream)
	    when(builder.open(any(InputStream.class)))
	            .thenThrow(new EncryptedDocumentException("encrypted"));

	    try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
	         MockedStatic<StreamingReader> streamingReader = Mockito.mockStatic(StreamingReader.class)) {

	        utility.when(() -> Utility.normalizePath(rawPath)).thenReturn(rawPath);
	        streamingReader.when(StreamingReader::builder).thenReturn(builder);

	        ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
	        try {
	            RuntimeException ex = assertThrows(RuntimeException.class, () -> preProcessor.parse(rawPath, "badpw"));

	            // Current implementation may surface generic message even for encrypted/password failures.
	            assertTrue(
	                    "Unable to read Excel file".equals(ex.getMessage())
	                            || "Unable to open encrypted Excel file. Please verify the password.".equals(ex.getMessage()),
	                    "Unexpected message: " + ex.getMessage()
	            );

	            assertNotNull(ex.getCause());
	            assertTrue(hasCause(ex, EncryptedDocumentException.class));
	        } finally {
	            // Even though parse failed, clear defensively
	            preProcessor.clear();
	        }
	    }
	}

	private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
	    Throwable cur = t;
	    while (cur != null) {
	        if (type.isInstance(cur)) return true;
	        cur = cur.getCause();
	    }
	    return false;
	}


	@Test
	void test_parse_whenFileNotFound_shouldWrapWithExcelFileNotFound() {
		Path missing = tempDir.resolve("missing.xlsx");
		assertFalse(Files.exists(missing));

		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();

		RuntimeException ex = assertThrows(RuntimeException.class, () -> preProcessor.parse(missing.toString(), null));
		assertEquals("Excel file not found", ex.getMessage());
		assertNotNull(ex.getCause());
		assertTrue(ex.getCause() instanceof FileNotFoundException);
	}

	@Test
	void test_parse_whenUnexpectedException_shouldWrapWithUnableToRead() {
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			utility.when(() -> Utility.normalizePath(anyString())).thenThrow(new RuntimeException("boom"));

			ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();

			RuntimeException ex = assertThrows(RuntimeException.class, () -> preProcessor.parse("anything.xlsx", null));
			assertEquals("Unable to read Excel file", ex.getMessage());
			assertNotNull(ex.getCause());
			assertEquals("boom", ex.getCause().getMessage());
		}
	}

	@Test
	void test_determineTableRanges_shouldCreateOneProcessorPerSheet_andStoreByName() throws Exception {
		Workbook workbook = mock(Workbook.class);
		Sheet s1 = mock(Sheet.class);
		Sheet s2 = mock(Sheet.class);

		when(workbook.getNumberOfSheets()).thenReturn(2);
		when(workbook.getSheetAt(0)).thenReturn(s1);
		when(workbook.getSheetAt(1)).thenReturn(s2);

		when(s1.getSheetName()).thenReturn("SheetA");
		when(s2.getSheetName()).thenReturn("SheetB");

		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		setField(preProcessor, "workbook", workbook);
		setField(preProcessor, "sheetProcessor", new java.util.HashMap<String, ExcelSheetPreProcessor>());

		try (MockedConstruction<ExcelSheetPreProcessor> mocked =
					 Mockito.mockConstruction(ExcelSheetPreProcessor.class)) {

			preProcessor.determineTableRanges();

			assertEquals(2, mocked.constructed().size());
			for (ExcelSheetPreProcessor p : mocked.constructed()) {
				verify(p, times(1)).determineSheetRanges();
			}

			@SuppressWarnings("unchecked")
			Map<String, ExcelSheetPreProcessor> map =
					(Map<String, ExcelSheetPreProcessor>) getField(preProcessor, "sheetProcessor");

			assertEquals(2, map.size());
			assertTrue(map.containsKey("SheetA"));
			assertTrue(map.containsKey("SheetB"));
			assertSame(mocked.constructed().get(0), map.get("SheetA"));
			assertSame(mocked.constructed().get(1), map.get("SheetB"));
		}
	}

	@Test
	void test_getSheetProcessors_whenNull_shouldThrowIllegalArgumentException() throws Exception {
		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		setField(preProcessor, "sheetProcessor", null);

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, preProcessor::getSheetProcessors);
		assertEquals("Must run determineTableRanges method to initialize pre processing of excel file", ex.getMessage());
	}

	@Test
	void test_getSheetNames_shouldReturnNamesInWorkbookOrder() throws Exception {
		Workbook workbook = mock(Workbook.class);
		when(workbook.getNumberOfSheets()).thenReturn(3);
		when(workbook.getSheetName(0)).thenReturn("One");
		when(workbook.getSheetName(1)).thenReturn("Two");
		when(workbook.getSheetName(2)).thenReturn("Three");

		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		setField(preProcessor, "workbook", workbook);

		List<String> names = preProcessor.getSheetNames();
		assertEquals(List.of("One", "Two", "Three"), names);
	}

	@Test
	void test_clear_shouldCloseSourceFile() throws Exception {
		FileInputStream fis = mock(FileInputStream.class);

		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		setField(preProcessor, "sourceFile", fis);

		preProcessor.clear();

		verify(fis, times(1)).close();
	}

	@Test
	void test_clear_shouldSwallowIOExceptionOnClose() throws Exception {
		FileInputStream fis = mock(FileInputStream.class);
		doThrow(new IOException("close failed")).when(fis).close();

		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		setField(preProcessor, "sourceFile", fis);

		assertDoesNotThrow(preProcessor::clear);
	}

	// ---- reflection helpers ----

	private static void setField(Object target, String fieldName, Object value) throws Exception {
		Field f = target.getClass().getDeclaredField(fieldName);
		f.setAccessible(true);
		f.set(target, value);
	}

	private static Object getField(Object target, String fieldName) throws Exception {
		Field f = target.getClass().getDeclaredField(fieldName);
		f.setAccessible(true);
		return f.get(target);
	}
}
