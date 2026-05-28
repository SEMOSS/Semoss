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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;

import com.github.pjfanning.xlsx.StreamingReader;

import prerna.util.Utility;

/**
 * Pre-processes workbook sheets to detect table-like data ranges prior to
 * querying.
 */
public class ExcelWorkbookFilePreProcessor {

	private static final Logger classLogger = LogManager.getLogger(ExcelWorkbookFilePreProcessor.class);

	static {
		// limit to 1GB excels
		IOUtils.setByteArrayMaxOverride(1024 * 1024 * 1024);
	}

	private Workbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;
	private String password = null;

	private Map<String, ExcelSheetPreProcessor> sheetProcessor = null;

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
	 * Parses an Excel workbook and prepares sheet preprocessors.
	 *
	 * @param fileLocation workbook path
	 * @param password optional workbook password
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

		this.sheetProcessor = new HashMap<String, ExcelSheetPreProcessor>();
	}

	/**
	 * Loop through all the sheets to determine the ranges of tables
	 */
	public void determineTableRanges() {
		int numSheets = workbook.getNumberOfSheets();
		for (int sheetIndex = 0; sheetIndex < numSheets; sheetIndex++) {
			determineTableRanges(workbook.getSheetAt(sheetIndex));
		}
	}

	/**
	 * Determine ranges in a specific sheet
	 *
	 * @param sheet sheet to analyze
	 */
	private void determineTableRanges(Sheet sheet) {
		ExcelSheetPreProcessor sProcessor = new ExcelSheetPreProcessor(sheet);
		sProcessor.determineSheetRanges();
		sheetProcessor.put(sheet.getSheetName(), sProcessor);
	}

	/**
	 * Returns computed sheet preprocessors keyed by sheet name.
	 *
	 * @return map of sheet names to their processors
	 */
	public Map<String, ExcelSheetPreProcessor> getSheetProcessors() {
		if (this.sheetProcessor == null) {
			throw new IllegalArgumentException(
					"Must run determineTableRanges method to initialize pre processing of excel file");
		}
		return this.sheetProcessor;
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

	/**
	 * Get the sheets in order
	 *
	 * @return sheet names in workbook order
	 */
	public List<String> getSheetNames() {
		int numSheets = this.workbook.getNumberOfSheets();
		List<String> sheets = new ArrayList<String>(numSheets);
		for (int i = 0; i < numSheets; i++) {
			sheets.add(this.workbook.getSheetName(i));
		}
		return sheets;
	}

}
