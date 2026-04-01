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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

public class ExcelSheetPreProcessorUnitTests {

	@Test
	void testGetRangeHeaders() {
		// create sheet
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");

		// add headers
		int row = 0;
		int col = 0;
		Row header = sheet.createRow(row++);
		header.createCell(col++).setCellValue("H1");
		header.createCell(col++).setCellValue("H2");
		header.createCell(col++).setCellValue("H3");

		// add data
		col = 0;
		Row data = sheet.createRow(row++);
		data.createCell(col++).setCellValue("v1");
		data.createCell(col++).setCellValue("v2");
		data.createCell(col++).setCellValue("v3");

		// create preprocessor
		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);
		p.determineSheetRanges();

		ExcelRange range = new ExcelRange("A1:C4");

		// validate getRangeHeaders()
		String[] headers = p.getRangeHeaders(range);
		List<String> actual = Arrays.asList(headers);
		List<String> expectedHeaders = List.of("H1", "H2", "H3");
		assertEquals(expectedHeaders, actual);

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	void testGetRangeHeaders_throwsDetermineSheetRangesError() {
		// create sheet
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");

		// create preprocessor
		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);

		ExcelRange range = new ExcelRange("A1:C4");
		try {
			p.getRangeHeaders(range);
			fail("must call determinSheetRanges() required method call");
		} catch (IllegalStateException ise) {
			assertEquals("Must call determineSheetRanges() before getRangeHeaders()", ise.getMessage());
		}

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	void testGetRangeHeaders_emptyHeader() {
		// create sheet
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");

		// add headers
		int row = 0;
		int col = 0;
		Row header = sheet.createRow(row++);
		header.createCell(col++).setCellValue("H1");
		header.createCell(col++).setCellValue("H2");
		header.createCell(col++).setCellValue("H3");

		// add data
		col = 0;
		Row data = sheet.createRow(row++);
		data.createCell(col++).setCellValue("v1");
		data.createCell(col++).setCellValue("v2");
		data.createCell(col++).setCellValue("v3");
		data.createCell(col++).setCellValue("v3");

		// create preprocessor
		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);
		p.determineSheetRanges();

		ExcelRange range = new ExcelRange("A1:D4");

		// validate getRangeHeaders()
		List<String> actual = Arrays.asList(p.getRangeHeaders(range));
		List<String> expected = List.of("H1", "H2", "H3", "");
		assertEquals(expected, actual);

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	void testGetRangeHeaders_badRange_returnsEmptyArray() {
		// create pre processor
		// create sheet
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");
		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);
		p.determineSheetRanges();

		// test range
		ExcelRange range = new ExcelRange("A10:D40");
		assertArrayEquals(new String[0], p.getRangeHeaders(range));

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	void testGetSheet() {
		// create pre processor
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");
		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);

		Sheet actualSheet = p.getSheet();
		assertEquals("TestSheet", actualSheet.getSheetName());

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	void testGetAllBlocks() {
		String actualSheetRange = "A1:C2";
		// create sheet
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");

		// add headers
		int row = 0;
		int col = 0;
		Row header = sheet.createRow(row++);
		header.createCell(col++).setCellValue("H1");
		header.createCell(col++).setCellValue("H2");
		header.createCell(col++).setCellValue("H3");

		// add data
		col = 0;
		Row data = sheet.createRow(row++);
		data.createCell(col++).setCellValue("v1");
		data.createCell(col++).setCellValue("v2");
		data.createCell(col++).setCellValue("v3");

		// create preprocessor
		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);
		p.determineSheetRanges();

		List<ExcelBlock> blocks = p.getAllBlocks();
		assertEquals(1, blocks.size());

		ExcelBlock bl = blocks.get(0);
		List<ExcelRange> ranges = bl.getRanges();
		assertEquals(1, ranges.size());
		ExcelRange r = ranges.get(0);
		assertEquals(actualSheetRange, r.getRangeSyntax());

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	void testGetCleanedRangeHeaders() {
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");
		// add headers
		int row = 0;
		int col = 0;
		Row header = sheet.createRow(row++);
		header.createCell(col++).setCellValue("H1");
		header.createCell(col++).setCellValue("H2");
		header.createCell(col++).setCellValue("H3");

		// add data
		col = 0;
		Row data = sheet.createRow(row++);
		data.createCell(col++).setCellValue("v1");
		data.createCell(col++).setCellValue("v2");
		data.createCell(col++).setCellValue("v3");
		data.createCell(col++).setCellValue("v3");
		data.createCell(col++).setCellValue("v3");

		ExcelSheetPreProcessor p = new ExcelSheetPreProcessor(sheet);
		p.determineSheetRanges();
		ExcelRange range = new ExcelRange("A1:E5");

		// validate getRangeHeaders()
		List<String> actual = Arrays.asList(p.getCleanedRangeHeaders(range));
		List<String> expected = List.of("H1", "H2", "H3", "BLANK_HEADER", "BLANK_HEADER_1");
		assertEquals(expected, actual);

		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	void testGetCleanedRangeHeaders_FromStrArr() {
		List<String> actual = Arrays
				.asList(ExcelSheetPreProcessor.getCleanedRangeHeaders(new String[] { "test", "", "" }));
		List<String> expected = List.of("test", "BLANK_HEADER", "BLANK_HEADER_1");
		assertEquals(expected, actual);

	}

}
