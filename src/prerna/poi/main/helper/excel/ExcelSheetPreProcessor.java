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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.om.HeadersException;

/**
 * Analyzes a single sheet to identify contiguous data blocks and expose cleaned
 * header metadata for each block.
 */
public class ExcelSheetPreProcessor {

	private static final Logger classLogger = LogManager.getLogger(ExcelSheetPreProcessor.class);

	private Sheet sheet;
	private String sheetName;

	// contain a list of all the blocks within this sheet
	private List<ExcelBlock> allBlocks = new ArrayList<ExcelBlock>();

	// Cache only header rows (first row of each block) to avoid re-iteration with
	// streaming reader
	private Map<Integer, String[]> cachedHeaders = new HashMap<>();
	private boolean hasProcessed = false;

	/**
	 * Creates a preprocessor for a single sheet.
	 *
	 * @param sheet sheet to preprocess
	 */
	public ExcelSheetPreProcessor(Sheet sheet) {
		this.sheet = sheet;
		this.sheetName = sheet.getSheetName();
	}

	/**
	 * Gets the wrapped sheet.
	 *
	 * @return source sheet
	 */
	public Sheet getSheet() {
		return this.sheet;
	}

	/**
	 * Gets all discovered data blocks in the sheet.
	 *
	 * @return detected data blocks
	 */
	public List<ExcelBlock> getAllBlocks() {
		return this.allBlocks;
	}

	/**
	 * Returns raw headers for a specific range from the cached header row.
	 *
	 * @param range range to resolve headers for
	 * @return raw header values for the range columns
	 */
	public String[] getRangeHeaders(ExcelRange range) {
		if (!hasProcessed) {
			throw new IllegalStateException("Must call determineSheetRanges() before getRangeHeaders()");
		}
		int[] rangeIndices = range.getIndices();
		int startCol = rangeIndices[0] - 1;
		int startRow = rangeIndices[1] - 1;
		int endCol = rangeIndices[2];

		// Use cached headers
		String[] cachedRow = cachedHeaders.get(startRow);
		if (cachedRow == null) {
			classLogger.warn("Header row not found at index: {} in sheet: {}", startRow, sheetName);
			return new String[0];
		}

		// Extract the range from the cached headers
		int numHeaders = endCol - startCol;
		String[] curHeaders = new String[numHeaders];

		for (int i = 0; i < numHeaders; i++) {
			int colIndex = startCol + i;
			if (colIndex < cachedRow.length) {
				curHeaders[i] = cachedRow[colIndex];
			} else {
				curHeaders[i] = "";
			}
		}

		return curHeaders;
	}

	/**
	 * Returns cleaned, unique headers for the provided range.
	 *
	 * @param range range to resolve headers for
	 * @return cleaned and de-duplicated headers
	 */
	public String[] getCleanedRangeHeaders(ExcelRange range) {
		String[] oHeaders = getRangeHeaders(range);

		// grab the headerChecker
		HeadersException headerChecker = HeadersException.getInstance();
		List<String> newUniqueCleanHeaders = new ArrayList<String>();

		int numCols = oHeaders.length;
		for (int colIdx = 0; colIdx < numCols; colIdx++) {
			String origHeader = oHeaders[colIdx];
			if (origHeader.trim().isEmpty()) {
				origHeader = "BLANK_HEADER";
			}
			String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueCleanHeaders);

			// now update the unique headers, as this will be used to match duplications
			newUniqueCleanHeaders.add(newHeader);
		}

		return newUniqueCleanHeaders.toArray(new String[0]);
	}

	/**
	 * Cleans and deduplicates a list of raw header values.
	 *
	 * @param oHeaders raw headers
	 * @return cleaned and unique headers
	 */
	public static String[] getCleanedRangeHeaders(String[] oHeaders) {
		// grab the headerChecker
		HeadersException headerChecker = HeadersException.getInstance();
		List<String> newUniqueCleanHeaders = new ArrayList<String>();

		int numCols = oHeaders.length;
		for (int colIdx = 0; colIdx < numCols; colIdx++) {
			String origHeader = oHeaders[colIdx];
			if (origHeader.trim().isEmpty()) {
				origHeader = "BLANK_HEADER";
			}
			String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueCleanHeaders);

			// now update the unique headers, as this will be used to match duplications
			newUniqueCleanHeaders.add(newHeader);
		}

		return newUniqueCleanHeaders.toArray(new String[0]);
	}

	/**
	 * Determine table ranges within a specific sheet
	 */
	public void determineSheetRanges() {
		if (hasProcessed) {
			classLogger.info("Sheet {} has already been processed, skipping", sheetName);
			return;
		}

		ExcelBlock thisBlock = new ExcelBlock();

		int startRow = sheet.getFirstRowNum();
		int lastRow = sheet.getLastRowNum();
		boolean isFirstRowOfBlock = false;

		classLogger.info("Processing {} from rows {} to {}", sheetName, startRow, lastRow);
		for (Row thisRow : sheet) {
			if (thisRow == null) {
				if (!thisBlock.isEmpty()) {
					// add to the list of blocks
					allBlocks.add(thisBlock);
					// create a new block
					thisBlock = new ExcelBlock();
					isFirstRowOfBlock = true;
				}
				continue;
			}

			int rowNum = thisRow.getRowNum();
			if (rowNum < startRow) {
				continue;
			}
			if (rowNum > lastRow) {
				break;
			}
			int excelRowNum = rowNum + 1;

			if (excelRowNum % 1000 == 0) {
				classLogger.info("Processing {} current row {}", sheetName, excelRowNum);
			}

			int startCol = thisRow.getFirstCellNum();
			int lastCol = thisRow.getLastCellNum();

			// sometimes, we can have an empty row
			// treat this as being a null row as well
			if (lastCol <= 0) {
				if (thisBlock.numIndicesInBlock() > 1) {
					// add to the list of blocks
					allBlocks.add(thisBlock);
					// create a new block
					thisBlock = new ExcelBlock();
					isFirstRowOfBlock = true;
				}
				continue;
			}

			// we want to keep track
			// if we are at the first column
			boolean initStartCol = true;
			int filledInColumns = 0;

			// Check if this is the first row of a new block
			if (thisBlock.isEmpty()) {
				isFirstRowOfBlock = true;
			}

			// Cache header row (first row of each block or potential header rows)
			if (isFirstRowOfBlock) {
				cacheHeaderRow(thisRow, rowNum, lastCol);
			}

			// loop through the row and add to the current block
			for (int colIndex = startCol; colIndex <= lastCol; colIndex++) {
				Cell thisCell = thisRow.getCell(colIndex);
				Object cellValue = ExcelParsing.getCell(thisCell);
				// if the cell is empty
				if (cellValue == null || cellValue.toString().trim().isEmpty()) {
					// ignore
					continue;
				} else {
					if (initStartCol) {
						thisBlock.addStartColumnIndex(colIndex);
						initStartCol = false;
					}

					// add column to row + type metadata
					String additionalFormatting = null;
					if (cellValue instanceof SemossDate) {
						additionalFormatting = ((SemossDate) cellValue).getPattern();
					}

					SemossDataType cellType = ExcelParsing.getTypeByCast(cellValue);
					thisBlock.addColumnToRowIndexWithData(colIndex, excelRowNum, cellType, additionalFormatting);
					filledInColumns++;
				}
			}

			// now see if the block is the same as the last one or not
			if (filledInColumns > 0) {
				// add the total number of columns that have values
				thisBlock.addTotalColumnsInRowStats(filledInColumns);
				// add the row index that has data
				thisBlock.addRowIndexContainingData(excelRowNum);
				// set the max column
				thisBlock.trySetLastColMaxIndex(lastCol);
				isFirstRowOfBlock = false;
			} else if (!thisBlock.isEmpty()) {
				tryMergeBlocks(thisBlock);
				// create a new block
				thisBlock = new ExcelBlock();
				isFirstRowOfBlock = true;
			}
		}

		classLogger.info("Processed {} from rows {} to {}, cached {} header rows", sheetName, startRow, lastRow,
				cachedHeaders.size());

		// we gotta add the last block into the list
		if (thisBlock.numIndicesInBlock() > 1) {
			this.allBlocks.add(thisBlock);
		}

		hasProcessed = true;
	}

	/**
	 * Cache a header row by extracting all cell values
	 */
	private void cacheHeaderRow(Row row, int rowIndex, int maxCol) {
		// Don't cache if already cached
		if (cachedHeaders.containsKey(rowIndex)) {
			return;
		}

		// Extract all cell values from the row
		String[] headerValues = new String[maxCol + 1];
		for (int i = 0; i <= maxCol; i++) {
			Cell cell = row.getCell(i);
			Object cellValue = ExcelParsing.getCell(cell);
			headerValues[i] = cellValue != null ? cellValue.toString() : "";
		}

		cachedHeaders.put(rowIndex, headerValues);
	}

	/**
	 * 
	 * @param thisBlock
	 */
	private void tryMergeBlocks(ExcelBlock thisBlock) {
		if (!allBlocks.isEmpty()) {
			// see if this was the same as the last block
			// or add it to the list of blocks
			ExcelBlock lastBlock = allBlocks.get(allBlocks.size() - 1);
			if (lastBlock.sameAs(thisBlock)) {
				lastBlock.merge(thisBlock);
			} else {
				allBlocks.add(thisBlock);
			}
		} else {
			// add our first block
			allBlocks.add(thisBlock);
		}
	}

}
