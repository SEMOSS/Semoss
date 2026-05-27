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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.github.pjfanning.xlsx.StreamingReader;

import prerna.query.querystruct.ExcelQueryStruct;
import prerna.util.Utility;

/**
 * Opens Excel workbooks and creates iterators for sheet-range queries.
 */
public class ExcelWorkbookFileHelper {

	private static final Logger classLogger = LogManager.getLogger(ExcelWorkbookFileHelper.class);

	private Workbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;
	private String password = null;

	/**
	 * Parses an Excel workbook without password protection.
	 *
	 * @param fileLocation workbook path
	 * @deprecated use {@link #parse(String, String)} to support both protected and
	 *             unprotected files
	 */
	@Deprecated
	public void parse(String fileLocation) {
		parse(fileLocation, null);
	}

	/**
	 * Parses an Excel workbook and prepares it for sheet access.
	 *
	 * @param fileLocation workbook path
	 * @param password     optional workbook password
	 */
	public void parse(String fileLocation, String password) {
		this.fileLocation = fileLocation;
		this.password = password;
		createParser();
	}

	/**
	 * Opens the workbook
	 */
	private void createParser() {
		try {
			sourceFile = new FileInputStream(Utility.normalizePath(fileLocation));
			workbook = StreamingReader.builder().rowCacheSize(10_000).bufferSize(1024 * 1024).password(this.password)
					.open(sourceFile);
		} catch (FileNotFoundException e) {
			classLogger.error("Excel file not found: {}", this.fileLocation, e);
			throw new RuntimeException("Excel file not found", e);
		} catch (EncryptedDocumentException e) {
			classLogger.error("Failed to open encrypted Excel file: {}", this.fileLocation, e);
			throw new RuntimeException("Unable to open encrypted Excel file. Please verify the password.", e);
		} catch (Exception e) {
			classLogger.error("Error reading Excel file: {}", this.fileLocation, e);
			throw new RuntimeException("Unable to read Excel file", e);
		}
	}

	/**
	 * Get all sheets
	 *
	 * @return workbook sheet names in workbook order
	 */
	public List<String> getSheets() {
		int numSheets = workbook.getNumberOfSheets();
		List<String> sheets = new ArrayList<String>(numSheets);
		for (int i = 0; i < numSheets; i++) {
			sheets.add(workbook.getSheetName(i));
		}

		return sheets;
	}

	/**
	 * Get the Sheet object
	 *
	 * @param sheetName workbook sheet name
	 * @return matching sheet or {@code null} when not found
	 */
	public Sheet getSheet(String sheetName) {
		return workbook.getSheet(sheetName);
	}

	/**
	 * Get the file path
	 *
	 * @return workbook path provided to {@link #parse(String, String)}
	 */
	public String getFilePath() {
		return fileLocation;
	}

	/**
	 * Builds a row iterator for the sheet/range in the supplied query struct.
	 *
	 * @param qs query struct containing file, sheet, range, and type metadata
	 * @return iterator over rows for the configured range
	 */
	public ExcelSheetFileIterator getSheetIterator(ExcelQueryStruct qs) {
		String sheetName = qs.getSheetName();
		Sheet sheet = workbook.getSheet(sheetName);
		ensureSheetRange(qs, sheet);
		ExcelSheetFileIterator it = new ExcelSheetFileIterator(sheet, qs);
		return it;
	}

	/**
	 * Builder to get to the sheet iterator
	 *
	 * @param qs query struct containing workbook, sheet, and range details
	 * @return configured sheet iterator
	 */
	public static ExcelSheetFileIterator buildSheetIterator(ExcelQueryStruct qs) {
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(qs.getFilePath(), qs.getPassword());
		return helper.getSheetIterator(qs);
	}

	/**
	 * Validate that the excel query struct has a sheet range defined. If not
	 * defined, infer the sheet range using the first range found
	 * 
	 * @param qs    query struct containing metadata for the range
	 * @param sheet the sheet the query struct is running on
	 */
	private void ensureSheetRange(ExcelQueryStruct qs, Sheet sheet) {
		String sheetRange = qs.getSheetRange();
		if (sheetRange != null && !sheetRange.trim().isEmpty()) {
			return;
		}
		if (sheet == null) {
			throw new IllegalArgumentException(
					"Unable to infer range because sheet was not found: " + qs.getSheetName());
		}

		String inferredSheetRange = inferFirstSheetRange(sheet.getSheetName());
		if (inferredSheetRange != null) {
			qs.setSheetRange(inferredSheetRange);
			return;
		}

		int startRow = Math.max(1, sheet.getFirstRowNum() + 1);
		int endRow = Math.max(startRow, sheet.getLastRowNum() + 1);
		// Safe fallback when range inference cannot find a table-like range.
		qs.setSheetRange(new ExcelRange(1, 1, startRow, endRow).getRangeSyntax());
	}

	/**
	 * Retrieve the first sheet range in the sheet to use
	 * 
	 * @param sheetName the name of the sheet in the excel
	 * @return the first sheet range found in the sheet
	 */
	private String inferFirstSheetRange(String sheetName) {
		ExcelWorkbookFilePreProcessor preProcessor = new ExcelWorkbookFilePreProcessor();
		try {
			preProcessor.parse(this.fileLocation, this.password);
			preProcessor.determineTableRanges();
			ExcelSheetPreProcessor sheetProcessor = preProcessor.getSheetProcessors().get(sheetName);
			if (sheetProcessor == null) {
				return null;
			}

			List<ExcelBlock> blocks = sheetProcessor.getAllBlocks();
			for (ExcelBlock block : blocks) {
				List<ExcelRange> ranges = block.getRanges();
				if (ranges != null && !ranges.isEmpty()) {
					return ranges.get(0).getRangeSyntax();
				}
			}
			return null;
		} catch (Exception e) {
			classLogger.warn("Unable to infer sheet range for '{}' in file '{}'. Falling back to default range.",
					sheetName, this.fileLocation, e);
			return null;
		} finally {
			preProcessor.clear();
		}
	}

	/**
	 * Clears the parser and requires you to start the parsing from scratch
	 */
	public void clear() {
		try {
			if (sourceFile != null) {
				sourceFile.close();
			}
		} catch (IOException e) {
			classLogger.error("Error closing file input stream for: {}", this.fileLocation, e);
		} catch (Exception e) {
			classLogger.error("Unexpected error during cleanup for: {}", this.fileLocation, e);
		}
	}

//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//		
//		String fileLocation = "C:\\Users\\SEMOSS\\Desktop\\shifted.xlsx";
//		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
//		helper.parse(fileLocation);
//		System.out.println(helper.getSheets());
//		
//		
//		ExcelQueryStruct qs = new ExcelQueryStruct();
//		qs.setSheetName("Sheet1");
//		qs.setSheetRange("E7:R28");
//		
//		ExcelSheetFileIterator it = helper.getSheetIterator(qs);
//		while(it.hasNext()) {
//			System.out.println(Arrays.toString(it.next().getValues()));
//		}
//	}

}
