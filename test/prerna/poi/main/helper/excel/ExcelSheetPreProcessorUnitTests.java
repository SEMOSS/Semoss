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

import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.SemossUnitTest;

public class ExcelSheetPreProcessorUnitTests extends SemossUnitTest {
	private static String excelFilePath = null;
	private static String sheetName = "TestData";
	private static String sheetRange = "A1:E3";
	private static List<String> sheetHeaders = List.of("UserID", "Name", "Active", "DateCreated", "Score");

	@BeforeEach
	void setUp() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());
		List<List<? extends Object>> data = List.of(sheetHeaders,
				List.of(1, "user1", true, LocalDate.of(2026, 3, 1), 98.5),
				List.of(2, "user2", false, LocalDate.of(2026, 3, 5), 87));

		File excelFile = new File(tempDir.toFile(), "test.xlsx");
		excelFilePath= excelFile.getAbsolutePath();
		
		try {
			createExcel(excelFile, sheetName, data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("unable to create excel file");
		}
	}
	

	@Test
	void testGetRangeHeaders() {
		// create pre processor
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(excelFilePath);
		Sheet sheet = helper.getSheet(sheetName);
		ExcelSheetPreProcessor sheetProcessor = new ExcelSheetPreProcessor(sheet);
		
		// test range
		ExcelRange range = new ExcelRange(sheetRange);
		try {
			sheetProcessor.getRangeHeaders(range);
			fail("");
		}
		catch (IllegalStateException ise) {
			assertEquals("Must call determineSheetRanges() before getRangeHeaders()", ise.getMessage());
		}
		sheetProcessor.determineSheetRanges();
		
		// validate getRangeHeaders()
		String[] headers = sheetProcessor.getRangeHeaders(range);
		List<String> actual = Arrays.asList(headers);
		assertEquals(sheetHeaders, actual);
	}

	@Test
	void testGetRangeHeaders_cacheHeaders() {
		// create pre processor
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(excelFilePath);
		Sheet sheet = helper.getSheet(sheetName);
		ExcelSheetPreProcessor sheetProcessor = new ExcelSheetPreProcessor(sheet);
		sheetProcessor.determineSheetRanges();

		// test range cache headers
		ExcelRange range = new ExcelRange("A2:E3");
		List<Integer> expected = List.of(1, 2, 3, 2);
		List<Integer> actualRange = IntStream.of(range.getIndices()).boxed().toList();
		assertEquals(expected, actualRange);

		// validate getRangeHeaders()
		String[] headers = sheetProcessor.getRangeHeaders(range);
		List<String> actual = Arrays.asList(headers);
		assertEquals(sheetHeaders, actual);		
	}

	private static void createExcel(File outputPath, String sheetName, List<List<? extends Object>> data) throws Exception {
		try (XSSFWorkbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet(sheetName);

			for (int r = 0; r < data.size(); r++) {
				Row row = sheet.createRow(r);
				List<Object> rowData = (List<Object>) data.get(r);

				for (int c = 0; c < rowData.size(); c++) {
					Cell cell = row.createCell(c);
					Object v = rowData.get(c);

					if (v == null) {
						cell.setBlank();
					} else if (v instanceof Number n) {
						cell.setCellValue(n.doubleValue());
					} else if (v instanceof Boolean b) {
						cell.setCellValue(b);
					} else if (v instanceof LocalDate d) {
						cell.setCellValue(java.sql.Date.valueOf(d));
					} else {
						cell.setCellValue(v.toString());
					}
				}
			}

			int maxCols = data.stream().mapToInt(List::size).max().orElse(0);
			for (int c = 0; c < Math.min(maxCols, 50); c++)
				sheet.autoSizeColumn(c);

			ExcelUtility.writeToFile(wb, outputPath.getAbsolutePath());
		}
	}
}
