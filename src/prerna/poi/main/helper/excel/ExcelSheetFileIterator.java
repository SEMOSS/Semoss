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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.flatfile.AbstractFileIterator;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ArrayUtilityMethods;

/**
 * Iterates through a configured sheet range and returns rows coerced to the
 * expected SEMOSS types.
 */
public class ExcelSheetFileIterator extends AbstractFileIterator {

	// classes around the sheet
	private Sheet sheet;
	private Iterator<Row> sheetIterator;
	private ExcelRange range;
	private int[] rangeIndex;

	// classes around the query struct
	private ExcelQueryStruct qs;
	private String sheetRange;

	// speed improvements
	private int[] headerIndices;
	private int numHeaders;
	private String[] cleanedRangeHeaders;

	// for looping through
	private int curRow;
	private int startCol;
	private int endCol;
	private int endRow;

	/**
	 * Simple iterator used when all the information can be parsed from the QS
	 *
	 * @param qs query struct containing file, sheet, range, and selector metadata
	 */
	public ExcelSheetFileIterator(ExcelQueryStruct qs) {
		this(null, qs);
	}

	/**
	 * Constructor for file iterator
	 *
	 * @param sheet optional pre-opened sheet; when {@code null}, the sheet is
	 *              opened from {@code qs}
	 * @param qs    query struct containing range and selector metadata
	 */
	public ExcelSheetFileIterator(Sheet sheet, ExcelQueryStruct qs) {
		if (sheet == null) {
			ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
			helper.parse(qs.getFilePath(), qs.getPassword());
			sheet = helper.getSheet(qs.getSheetName());
		}
		// get the excel elements
		this.sheet = sheet;
		this.sheetIterator = this.sheet.iterator();

		// get the qs elements
		this.qs = qs;
		this.sheetRange = qs.getSheetRange();

		// range index is start col, start row, end col, end row
		this.range = new ExcelRange(this.sheetRange);
		this.rangeIndex = range.getIndices();

		// range start row contains the headers; data starts on the following row
		this.curRow = this.rangeIndex[1] + 1;
		this.startCol = this.rangeIndex[0];
		this.endCol = this.rangeIndex[2];

		this.endRow = this.rangeIndex[3];

		// now that I have set the headers from the setSelectors
		this.dataTypeMap = qs.getColumnTypes();
		this.additionalTypesMap = qs.getAdditionalTypes();
		this.newHeaders = qs.getNewHeaderNames();

		Row headerRow = null;
		int counter = 0;
		while (counter < this.rangeIndex[1]) {
			headerRow = this.sheetIterator.next();
			counter++;
		}
		if (headerRow == null) {
			throw new IllegalArgumentException("Unable to locate header row for range " + this.sheetRange);
		}
		// minus 1 because startCol will be 1 above
		this.numHeaders = endCol - (startCol - 1);
		String[] curHeaders = new String[numHeaders];
		for (int i = 0; i < numHeaders; i++) {
			int colIndex = (startCol - 1) + i;
			if (colIndex < headerRow.getLastCellNum()) {
				Object cellValue = ExcelParsing.getCell(headerRow.getCell(colIndex));
				curHeaders[i] = cellValue != null ? cellValue.toString() : "";
			} else {
				curHeaders[i] = "";
			}
		}
		// grab the headers
		cleanedRangeHeaders = ExcelSheetPreProcessor.getCleanedRangeHeaders(curHeaders);
		// need to figure out the selectors
		setSelectors(qs.getSelectors());

		this.numHeaders = this.headerIndices.length;
		// grab the first row in preparation for iterating
		getNextRow();

		// set limit and offset
		this.limit = qs.getLimit();
		this.offset = qs.getOffset();
	}

	/**
	 * Advances the iterator to the next row in the configured range.
	 */
	@Override
	public void getNextRow() {
		if (this.curRow > this.endRow) {
			this.nextRow = null;
			return;
		}

		// get the new row to return
		this.nextRow = new Object[this.headerIndices.length];

		Row row = this.sheetIterator.next();
		if (row != null) {
			for (int i = 0; i < numHeaders; i++) {
				int cellIndex = this.headerIndices[i];
				// remember, excel is 1 based while java is 0
				Cell c = row.getCell(cellIndex - 1);
				this.nextRow[i] = ExcelParsing.getCell(c);
			}
		} else {
			// set all values to empty string
			for (int i = 0; i < this.headerIndices.length; i++) {
				this.nextRow[i] = "";
			}
		}
		// set up for the next row
		this.curRow++;
	}

	/**
	 * Since we have types in excel We will use a better version for getting the
	 * clean types
	 */
	@Override
	protected Object[] cleanRow(Object[] row, SemossDataType[] types, String[] additionalTypes) {
		Object[] cleanRow = new Object[row.length];
		for (int i = 0; i < row.length; i++) {
			Object val = row[i];
			if (val == null) {
				continue;
			}
			SemossDataType type = types[i];
			String additionalFormatting = additionalTypes[i];

			// try to get correct type
			if (type == SemossDataType.STRING) {
				cleanRow[i] = val; // Utility.cleanString(val.toString(), true, true, false);
			} else if (type == SemossDataType.INT) {
				if (val instanceof Number) {
					cleanRow[i] = ((Number) val).intValue();
				} else {
					String strVal = val.toString();
					try {
						// added to remove $ and , in data and then try parsing as Double
						int mult = 1;
						if (strVal.startsWith("(") || strVal.startsWith("-")) { // this is a negativenumber
							mult = -1;
						}
						strVal = strVal.replaceAll("[^0-9\\.E]", "");
						cleanRow[i] = mult * Integer.parseInt(strVal.trim());
					} catch (NumberFormatException ex) {
						// do nothing
						cleanRow[i] = null;
					}
				}
			} else if (type == SemossDataType.DOUBLE) {
				if (val instanceof Number) {
					cleanRow[i] = ((Number) val).doubleValue();
				} else {
					String strVal = val.toString();
					try {
						// added to remove $ and , in data and then try parsing as Double
						int mult = 1;
						if (strVal.startsWith("(") || strVal.startsWith("-")) { // this is a negativenumber
							mult = -1;
						}
						strVal = strVal.replaceAll("[^0-9\\.E]", "");
						cleanRow[i] = mult * Double.parseDouble(strVal.trim());
					} catch (NumberFormatException ex) {
						// do nothing
						cleanRow[i] = null;
					}
				}
			} else if (type == SemossDataType.DATE) {
				if (val instanceof SemossDate) {
					if (additionalFormatting != null) {
						cleanRow[i] = new SemossDate(((SemossDate) val).getZonedDateTime(), additionalFormatting);
					} else {
						cleanRow[i] = val;
					}
				} else {
					String strVal = val.toString();
					if (additionalFormatting != null) {
						cleanRow[i] = new SemossDate(strVal, additionalFormatting);
					} else {
						cleanRow[i] = SemossDate.genDateObj(strVal);
					}
				}
			} else if (type == SemossDataType.TIMESTAMP) {
				if (val instanceof SemossDate) {
					if (additionalFormatting != null) {
						cleanRow[i] = new SemossDate(((SemossDate) val).getZonedDateTime(), additionalFormatting);
					} else {
						cleanRow[i] = val;
					}
				} else {
					String strVal = val.toString();
					if (additionalFormatting != null) {
						cleanRow[i] = new SemossDate(strVal, additionalFormatting);
					} else {
						cleanRow[i] = SemossDate.genTimeStampDateObj(strVal);
					}
				}
			} else if (type == SemossDataType.BOOLEAN) {
				cleanRow[i] = Boolean.parseBoolean(val.toString());
			}

		}

		return cleanRow;
	}

	/**
	 * Determine the selectors for the sheet
	 * 
	 * @param qsSelectors
	 */
	private void setSelectors(List<IQuerySelector> qsSelectors) {
		/*
		 * Here is the order We first try to use the specific headers defined in the QS
		 * Otherwise, we use the ones from the data type map If neither, we use all
		 * 
		 */

		// get headers from qs
		if (!qsSelectors.isEmpty()) {

			int numSelectors = qsSelectors.size();

			String[] selectors = new String[numSelectors];
			for (int i = 0; i < numSelectors; i++) {
				QueryColumnSelector newSelector = (QueryColumnSelector) qsSelectors.get(i);
				if (newSelector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
					throw new IllegalArgumentException("Cannot perform math on a excel import");
				}
				selectors[i] = newSelector.getAlias();
			}

			String[] allHeaders = cleanedRangeHeaders;
			if (allHeaders.length != selectors.length) {
				// order the selectors
				// all headers will be ordered
				String[] orderedAliasSelectors = new String[selectors.length];
				String[] orderedCSVSelectors = new String[selectors.length];

				int counter = 0;
				for (String alias : selectors) {
					String oldHeader = alias;
					if (this.newHeaders.containsKey(alias)) {
						oldHeader = newHeaders.get(alias);
					}
					if (ArrayUtilityMethods.arrayContainsValue(allHeaders, oldHeader)) {
						orderedAliasSelectors[counter] = alias;
						orderedCSVSelectors[counter] = oldHeader;
						counter++;
					}
				}
				this.headers = orderedAliasSelectors;
				this.headerIndices = findHeaderIndicies(allHeaders, orderedCSVSelectors);
			} else {
				this.headers = allHeaders;
				this.headerIndices = new int[this.headers.length];
				for (int i = 0; i < this.headers.length; i++) {
					this.headerIndices[i] = i + startCol;
					String header = this.headers[i];
					// new headers alias:oldHeader
					// here the headers are the old headers so we need to look
					// at the value of the map :/
					for (String key : this.newHeaders.keySet()) {
						if (this.newHeaders.get(key).equals(header)) {
							this.headers[i] = key;
						}
					}
				}
			}
		} else if (this.dataTypeMap != null && !this.dataTypeMap.isEmpty() && qsSelectors.isEmpty()) {
			// grab the headers defined in the dataTypeMap
			this.headers = dataTypeMap.keySet().toArray(new String[dataTypeMap.size()]);
			// get the header indices
			String[] headersInRange = cleanedRangeHeaders;
			// get additional datatypes
			String[] tempHeaders = new String[this.headers.length];
			for (int index = 0; index < this.headers.length; index++) {
				String header = this.headers[index];
				// change new headers to old to find the indices
				if (this.newHeaders != null && this.newHeaders.containsKey(header)) {
					tempHeaders[index] = this.newHeaders.get(header);
				} else {
					tempHeaders[index] = this.headers[index];
				}
			}
			this.headerIndices = this.findHeaderIndicies(headersInRange, tempHeaders);
		}

		if (dataTypeMap == null || dataTypeMap.isEmpty()) {
			if (this.headers == null) {
				// define the headers using everything
				this.headers = this.cleanedRangeHeaders;
				this.headerIndices = new int[this.headers.length];
				for (int i = 0; i < this.headers.length; i++) {
					this.headerIndices[i] = i + startCol;
					String header = this.headers[i];
					// new headers alias:oldHeader
					// here the headers are the old headers so we need to look
					// at the value of the map :/
					for (String key : this.newHeaders.keySet()) {
						if (this.newHeaders.get(key).equals(header)) {
							this.headers[i] = key;
						}
					}
				}
			}
			setUnknownTypes();
		}

		// order headers
		List<Integer> headerIndiciesList = Arrays.stream(headerIndices).boxed().collect(Collectors.toList());

		String[] sortedHeaders = new String[this.headers.length];
		int[] sortedIndicies = Arrays.copyOf(this.headerIndices, this.headerIndices.length);
		Arrays.sort(sortedIndicies);
		for (int i = 0; i < sortedIndicies.length; i++) {
			int index = sortedIndicies[i];
			int headerIndex = headerIndiciesList.indexOf(index);
			sortedHeaders[i] = this.headers[headerIndex];
		}
		this.headers = sortedHeaders;
		this.headerIndices = sortedIndicies;

		// now that we have defined the headers need to set types
		this.types = new SemossDataType[this.headers.length];
		this.additionalTypes = new String[this.headers.length];
		for (int i = 0; i < this.headers.length; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(this.dataTypeMap.get(this.headers[i]));
			this.additionalTypes[i] = this.additionalTypesMap.get(this.headers[i]);
		}

		qs.setColumnTypes(this.dataTypeMap);
		qs.setAdditionalTypes(this.additionalTypesMap);
	}

	/**
	 * Sets the data types
	 */
	private void setUnknownTypes() {
		int startRowForTypes = this.rangeIndex[1] + 1;
		if (startRowForTypes > this.rangeIndex[3]) {
			startRowForTypes = this.rangeIndex[3];
		}
		ExcelRange dataRange = new ExcelRange(this.rangeIndex[0], this.rangeIndex[2], startRowForTypes,
				this.rangeIndex[3]);
		Object[][] prediction = ExcelParsing.predictTypes(this.sheet, dataRange.getRangeSyntax());
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(this.headers, prediction);
		this.dataTypeMap = predictionMaps[0];
		this.additionalTypesMap = predictionMaps[1];
	}

	/**
	 * Get the indices for the headers within the excel block
	 * 
	 * @param sheetHeaders
	 * @param headers
	 * @return
	 */
	private int[] findHeaderIndicies(String[] sheetHeaders, String[] headers) {
		int numHeadersToGet = headers.length;
		int[] indicesToGet = new int[numHeadersToGet];
		for (int colIdx = 0; colIdx < numHeadersToGet; colIdx++) {
			String headerToGet = headers[colIdx];
			// find the index in sheet headers to return
			// add start col so the offset is accurate
			indicesToGet[colIdx] = ArrayUtilityMethods.arrayContainsValueAtIndex(sheetHeaders, headerToGet) + startCol;
		}

		return indicesToGet;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}

	/**
	 * Closes iterator resources.
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	/**
	 * Gets the query struct used to configure this iterator.
	 *
	 * @return query struct
	 */
	public ExcelQueryStruct getQs() {
		return this.qs;
	}

	/**
	 * Updates the query struct reference for this iterator.
	 *
	 * @param qs query struct
	 */
	public void setQs(ExcelQueryStruct qs) {
		this.qs = qs;
	}

	/**
	 * Gets the sheet being iterated.
	 *
	 * @return source sheet
	 */
	public Sheet getSheet() {
		return this.sheet;
	}

	/**
	 * Gets one-based column indices corresponding to the selected headers.
	 *
	 * @return selected header indices
	 */
	public int[] getHeaderIndicies() {
		return this.headerIndices;
	}

}
