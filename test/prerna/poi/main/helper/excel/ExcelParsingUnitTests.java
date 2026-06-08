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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Calendar;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.SemossUnitTest;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;

public class ExcelParsingUnitTests extends SemossUnitTest {

	@ParameterizedTest
	@ValueSource(strings = { ".xlsx", ".xlsm", ".xls" })
	void test_isExcelFile(String input) {
		assertTrue(ExcelParsing.isExcelFile(input));
	}

	@ParameterizedTest
	@ValueSource(strings = { "", ".xls.x" })
	void test_isExcelFile_false(String input) {
		assertFalse(ExcelParsing.isExcelFile(input));
	}

	@Test
	void test_isEmptyCell() throws IOException {
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");

		// add headers
		int row = 0;
		int col = 0;
		Row header = sheet.createRow(row++);
		Cell cell = header.createCell(col++);
		cell.setCellValue("");
		assertTrue(ExcelParsing.isEmptyCell(cell));

		cell.setCellValue(5);
		assertFalse(ExcelParsing.isEmptyCell(cell));

		cell.setCellValue("    ");
		assertTrue(ExcelParsing.isEmptyCell(cell));

		cell = null;
		assertTrue(ExcelParsing.isEmptyCell(cell));

		wb.close();
	}

	@Test
	void test_getCell() throws IOException {
		XSSFWorkbook wb = new XSSFWorkbook();
		XSSFSheet sheet = wb.createSheet("TestSheet");

		// add headers
		int rowInt = 0;
		int col = 0;
		Row row = sheet.createRow(rowInt++);
		Cell cell = row.createCell(col++);

		// simple tests
		{
			cell.setBlank();
			assertEquals("", ExcelParsing.getCell(cell));

			cell.setCellValue("xyz");
			assertEquals("xyz", ExcelParsing.getCell(cell));

			cell.setCellValue("9/12/2020");
			assertEquals("9/12/2020", ExcelParsing.getCell(cell));

			cell.setCellValue(true);
			assertEquals(true, ExcelParsing.getCell(cell));

			cell.setCellValue(100.0);
			assertEquals(100.0, ExcelParsing.getCell(cell));
		}

		// test semoss date
		{
			String datePattern = "MM/dd/yyyy";

			// set cell style for date
			CellStyle dateStyle = wb.createCellStyle();
			dateStyle.setDataFormat(wb.createDataFormat().getFormat(datePattern));
			cell.setCellStyle(dateStyle);

			// create date
			Calendar cal = Calendar.getInstance();
			cal.clear();
			cal.set(2025, 1, 15, 0, 0, 0);
			cell.setCellValue(cal.getTime());

			// validate
			SemossDate actualDate = (SemossDate) ExcelParsing.getCell(cell);
			assertEquals("02/15/2025", actualDate.getFormatted(datePattern));
		}

		// test string formula cell reference
		{
			Cell a1 = row.createCell(0);
			a1.setCellValue("hello");

			Cell formula = row.createCell(1);
			formula.setCellFormula("A1");
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateFormulaCell(formula);
			assertEquals("hello", ExcelParsing.getCell(formula));

		}

		// test numeric formula cell reference
		{
			Cell a1 = row.createCell(0);
			a1.setCellValue(100);

			Cell formula = row.createCell(1);
			formula.setCellFormula("A1");
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateFormulaCell(formula);
			assertEquals(100.0, ExcelParsing.getCell(formula));
		}

		// test boolean formula cell reference
		{
			Cell a1 = row.createCell(0);
			a1.setCellValue(false);

			Cell formula = row.createCell(1);
			formula.setCellFormula("A1");
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateFormulaCell(formula);
			assertEquals(false, ExcelParsing.getCell(formula));
		}

		// test semoss date formula cell reference
		{
			Cell a1 = row.createCell(0);

			String datePattern = "MM/dd/yyyy";

			// set cell style for date
			CellStyle dateStyle = wb.createCellStyle();
			dateStyle.setDataFormat(wb.createDataFormat().getFormat(datePattern));
			a1.setCellStyle(dateStyle);

			// create date
			Calendar cal = Calendar.getInstance();
			cal.clear();
			cal.set(2025, 1, 15, 0, 0, 0);
			a1.setCellValue(cal.getTime());

			// validate
			Cell formulaCell = row.createCell(1);
			formulaCell.setCellFormula("A1");
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateFormulaCell(formulaCell);
			// need to format reference cell also as date
			formulaCell.setCellStyle(dateStyle);

			SemossDate actualDate = (SemossDate) ExcelParsing.getCell(formulaCell);
			assertEquals("02/15/2025", actualDate.getFormatted(datePattern));
		}

		// test blank formula cell reference
		{
			Cell a1 = row.createCell(0);
			a1.setBlank();

			Cell formula = row.createCell(1);
			formula.setCellFormula("A1");
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateFormulaCell(formula);
			assertEquals(0.0, ExcelParsing.getCell(formula));
		}

		assertEquals(null, ExcelParsing.getCell(null));

		wb.close();

	}

	@Test
	void test_getTypeByCast() throws IOException {
		Object testValue = "hi";
		assertEquals(SemossDataType.STRING, ExcelParsing.getTypeByCast(testValue));

		testValue = null;
		assertEquals(SemossDataType.STRING, ExcelParsing.getTypeByCast(testValue));

		testValue = 1.25;
		assertEquals(SemossDataType.DOUBLE, ExcelParsing.getTypeByCast(testValue));

		testValue = 1.0;
		assertEquals(SemossDataType.INT, ExcelParsing.getTypeByCast(testValue));

		testValue = 1;
		assertEquals(SemossDataType.INT, ExcelParsing.getTypeByCast(testValue));

		testValue = true;
		assertEquals(SemossDataType.BOOLEAN, ExcelParsing.getTypeByCast(testValue));

		// test semoss date
		{
			String datePattern = "MM/dd/yyyy";

			// create date
			Calendar cal = Calendar.getInstance();
			cal.clear();
			cal.set(2025, 1, 15, 0, 0, 0);

			ZoneId id = ZoneId.of("UTC");
			// validate
			testValue = new SemossDate(cal.getTime(), datePattern, id);
			assertEquals(SemossDataType.DATE, ExcelParsing.getTypeByCast(testValue));

		}

		// test semoss timestamp
		{
			String datePattern = "MM/dd/yyyy hh:mm:ss";

			// create date
			Calendar cal = Calendar.getInstance();
			cal.clear();
			cal.set(2025, 1, 15, 12, 30, 45);

			ZoneId id = ZoneId.of("UTC");
			// validate
			testValue = new SemossDate(cal.getTime(), datePattern, id);
			assertEquals(SemossDataType.TIMESTAMP, ExcelParsing.getTypeByCast(testValue));

		}

	}

	@Test
	void predictTypes_emptyColumn_defaultsToString() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// create empty rows
			int rowInt = 0;
			sheet.createRow(rowInt++);
			sheet.createRow(rowInt++);
			sheet.createRow(rowInt++);

			// validate string type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A3");
			assertEquals(SemossDataType.STRING, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_intEmpty() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add int row
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(1);

			// create empty rows
			sheet.createRow(rowInt++);
			sheet.createRow(rowInt++);
			sheet.createRow(rowInt++);

			// validate string type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A4");
			assertEquals(SemossDataType.INT, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_double() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add int row
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(1);

			// add double
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue(1.99);

			// validate double type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A2");
			assertEquals(SemossDataType.DOUBLE, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_onlyDouble() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add int row
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(1.2);

			// add double
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue(1.99);

			// validate double type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A2");
			assertEquals(SemossDataType.DOUBLE, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_onlyInt() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add int row
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(1);

			// add int
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue(2);

			// validate double type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A2");
			assertEquals(SemossDataType.INT, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_numberThenString() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add int
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(1);

			// add string
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue("stringValue");

			// add double
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue(1.99);

			// validate str type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A3");
			assertEquals(SemossDataType.STRING, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_booleanMixedWithNumber_defaultsToString() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add int
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(1);

			// add boolean
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue(true);

			// validate string type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A2");
			assertEquals(SemossDataType.STRING, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_boolean() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");

			// add boolean
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellValue(false);

			// add boolean
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue(true);

			// validate string type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A2");
			assertEquals(SemossDataType.BOOLEAN, predicted[0][0]);
		}
	}

	@Test
	void predictTypes_dateAndTimestamp_becomesTimestamp() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");
			String timestampFmt = "MM/dd/yyyy HH:mm:ss";

			CellStyle dateStyle = wb.createCellStyle();
			dateStyle.setDataFormat(wb.createDataFormat().getFormat("MM/dd/yyyy"));

			CellStyle tsStyle = wb.createCellStyle();
			tsStyle.setDataFormat(wb.createDataFormat().getFormat(timestampFmt));

			Calendar cal = Calendar.getInstance();

			// date
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellStyle(dateStyle);
			cal.clear();
			cal.set(2025, 2, 15, 0, 0, 0);
			cell.setCellValue(cal.getTime());

			// timestamp
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellStyle(tsStyle);
			cal.clear();
			cal.set(2025, 2, 15, 13, 30, 45);
			cell.setCellValue(cal.getTime());

			// date
			row = sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellStyle(dateStyle);
			cal.clear();
			cal.set(2025, 2, 15, 0, 0, 0);
			cell.setCellValue(cal.getTime());

			// validate timestamp
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A3");
			assertEquals(SemossDataType.TIMESTAMP, predicted[0][0]);
			assertEquals(timestampFmt, predicted[0][1]);
		}
	}

	@Test
	void predictTypes_tsAndString() throws IOException {
		try (Workbook wb = new XSSFWorkbook()) {
			Sheet sheet = wb.createSheet("testSheet");
			String timestampFmt = "MM/dd/yyyy HH:mm:ss";

			CellStyle tsStyle = wb.createCellStyle();
			tsStyle.setDataFormat(wb.createDataFormat().getFormat(timestampFmt));
			Calendar cal = Calendar.getInstance();

			// add timestamp
			int rowInt = 0;
			Row row = sheet.createRow(rowInt++);
			Cell cell = row.createCell(0);
			cell.setCellStyle(tsStyle);
			cal.clear();
			cal.set(2025, 2, 15, 13, 30, 45);
			cell.setCellValue(cal.getTime());

			// add string
			sheet.createRow(rowInt++);
			cell = row.createCell(0);
			cell.setCellValue("test");

			// validate str type
			Object[][] predicted = ExcelParsing.predictTypes(sheet, "A1:A2");
			assertEquals(SemossDataType.STRING, predicted[0][0]);
		}
	}

}
