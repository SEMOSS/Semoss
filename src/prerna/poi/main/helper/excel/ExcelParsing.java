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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.poi.main.helper.FileHelperUtil;

/**
 * Parsing helpers for extracting typed values and inferring column data types
 * from Excel sheets.
 */
public class ExcelParsing {

	private static final int NUM_ROWS_TO_PREDICT_TYPES = 500;

	private ExcelParsing() {

	}

	/**
	 * Validates that the provided file path points to a supported Excel file type.
	 *
	 * @param filePath input file path
	 * @return {@code true} when the file extension is one of {@code .xlsx},
	 *         {@code .xlsm}, or {@code .xls}
	 */
	public static boolean isExcelFile(String filePath) {
		if (filePath == null) {
			return false;
		}
		String file = filePath.toLowerCase();
		if (file.endsWith(".xlsx") || file.endsWith(".xlsm") || file.endsWith(".xls")) {
			return true;
		}

		return false;

	}

	/**
	 * Determines whether a cell should be treated as empty.
	 *
	 * @param thisCell cell to evaluate
	 * @return {@code true} when the cell is {@code null}, blank, or whitespace only
	 */
	public static boolean isEmptyCell(Cell thisCell) {
		if (thisCell == null || thisCell.getCellType() == CellType.BLANK || thisCell.toString().trim().isEmpty()) {
			return true;
		}
		return false;
	}

	/**
	 * Get the cell values
	 *
	 * @param thisCell cell to read
	 * @return the typed Java value for the cell, or {@code null} when no value can
	 *         be resolved
	 */
	public static Object getCell(Cell thisCell) {
		if (thisCell == null) {
			return null;
		}

		CellType type = thisCell.getCellType();
		if (type == CellType.BLANK) {
			return "";
		}
		if (type == CellType.STRING) {
			return thisCell.getStringCellValue();
		} else if (type == CellType.NUMERIC) {
			if (DateUtil.isCellDateFormatted(thisCell)) {
				return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
			}
			return thisCell.getNumericCellValue();
		} else if (type == CellType.BOOLEAN) {
			return thisCell.getBooleanCellValue();
		} else if (type == CellType.FORMULA) {
			// do the same for the formula value
			CellType formulatype = thisCell.getCachedFormulaResultType();
			if (formulatype == CellType.BLANK) {
				return "";
			}
			if (formulatype == CellType.STRING) {
				return thisCell.getStringCellValue();
			} else if (formulatype == CellType.NUMERIC) {
				if (DateUtil.isCellDateFormatted(thisCell)) {
					return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
				}
				return thisCell.getNumericCellValue();
			} else if (formulatype == CellType.BOOLEAN) {
				return thisCell.getBooleanCellValue();
			}
		}

		return null;
	}

	/*
	 * Methods around predicting types
	 */

	/**
	 * Predicts the column types for a range in a sheet.
	 *
	 * @param sheet source sheet
	 * @param range excel range in A1 notation
	 * @return matrix of per-column type predictions and optional formatting
	 *         metadata
	 */
	public static Object[][] predictTypes(Sheet sheet, String range) {
		// for a given sheet
		// loop through and determine the types
		// based on a block in a given range

		// range index is start col, start row, end col, end row
		int[] rangeIndex = ExcelRange.getSheetRangeIndex(range);

		int numCols = rangeIndex[2] - rangeIndex[0] + 1;

		Object[][] predictedTypes = new Object[numCols][3];
		List<Map<String, Integer>> additionalFormatTracker = new ArrayList<Map<String, Integer>>(numCols);

		// Loop through cols, and up to 1000 rows
		int counter = 0;
		for (int colIndex = rangeIndex[0]; colIndex <= rangeIndex[2]; colIndex++) {
			predictTypesLoop(sheet.iterator(), rangeIndex, predictedTypes, additionalFormatTracker, colIndex, counter);
			counter++;
		}

		return predictedTypes;
	}

	private static void predictTypesLoop(Iterator<Row> sheetIterator, int[] rangeIndex, Object[][] predictedTypes,
			List<Map<String, Integer>> additionalFormatTracker, int cellIndex, int colIndex) {
		int startRow = rangeIndex[1] - 1;
		int endRow = rangeIndex[3] - 1;
		// only use up to 500 rows for determining the types
		if (endRow - startRow + 1 > NUM_ROWS_TO_PREDICT_TYPES) {
			endRow = startRow + NUM_ROWS_TO_PREDICT_TYPES - 1;
		}

		boolean forceBreak = false;
		SemossDataType type = null;
		Map<String, Integer> formatTracker = new HashMap<String, Integer>();
		additionalFormatTracker.add(formatTracker);

		ROW_LOOP: while (sheetIterator.hasNext()) {
			Row row = sheetIterator.next();
			if (row == null) {
				continue ROW_LOOP;
			}
			int rowNum = row.getRowNum();
			if (rowNum < startRow) {
				continue ROW_LOOP;
			}
			if (rowNum > endRow) {
				break ROW_LOOP;
			}

			// remember, excel is 1 based while java is 0 based
			Object value = ExcelParsing.getCell(row.getCell(cellIndex - 1));
			if (value == null || value instanceof String && value.toString().isEmpty()) {
				continue ROW_LOOP;
			}

			SemossDataType newTypePrediction = getTypeByCast(value);
			String additionalFormatting = null;
			if (value instanceof SemossDate) {
				additionalFormatting = ((SemossDate) value).getPattern();
			}

			// handle the additional formatting
			if (additionalFormatting != null) {
				if (formatTracker.containsKey(additionalFormatting)) {
					// increase counter by 1
					formatTracker.put(additionalFormatting,
							Integer.valueOf(formatTracker.get(additionalFormatting) + 1));
				} else {
					formatTracker.put(additionalFormatting, Integer.valueOf(1));
				}
			}

			// if we hit a string
			// we are done
			if (newTypePrediction == SemossDataType.STRING) {
				forceBreak = true;
				Object[] columnPrediction = new Object[2];
				columnPrediction[0] = newTypePrediction;
				predictedTypes[colIndex] = columnPrediction;
				break ROW_LOOP;
			}

			if (type == null) {
				// this is the first time we go through
				// just set the type and we are done
				// we only need to go through when we hit a difference
				type = newTypePrediction;
				continue;
			}

			if (type == newTypePrediction) {
				// well, nothing for us to do if its the same
				// again, we handle additional formatting
				// at the top
				continue;
			}

			// if we hit a boolean
			else if (newTypePrediction == SemossDataType.BOOLEAN) {
				// we have a boolean and something else we dont know
				// default to string
				type = SemossDataType.STRING;
				// clear the tracker so we dont send additional format logic
				formatTracker.clear();
				break ROW_LOOP;
			}

			// if we hit an integer
			else if (newTypePrediction == SemossDataType.INT) {
				if (type == SemossDataType.DOUBLE) {
					// the type stays as double
					type = SemossDataType.DOUBLE;
				} else {
					// we have a number and something else we dont know
					// default to string
					type = SemossDataType.STRING;
					// clear the tracker so we dont send additional format logic
					formatTracker.clear();
					break ROW_LOOP;
				}
			}

			// if we hit a double
			else if (newTypePrediction == SemossDataType.DOUBLE) {
				if (type == SemossDataType.INT) {
					// the type stays as double
					type = SemossDataType.DOUBLE;
				} else {
					// we have a number and something else we dont know
					// default to string
					type = SemossDataType.STRING;
					// clear the tracker so we dont send additional format logic
					formatTracker.clear();
					break ROW_LOOP;
				}
			}

			// if we hit a date
			else if (newTypePrediction == SemossDataType.DATE) {
				if (type == SemossDataType.TIMESTAMP) {
					// stick with timestamp
					type = SemossDataType.TIMESTAMP;
				} else {
					// we have a number and something else we dont know
					// default to string
					type = SemossDataType.STRING;
					// clear the tracker so we dont send additional format logic
					formatTracker.clear();
					break ROW_LOOP;
				}
			}

			// if we hit a timestamp
			else if (newTypePrediction == SemossDataType.TIMESTAMP) {
				if (type == SemossDataType.DATE) {
					// stick with timestamp
					type = SemossDataType.TIMESTAMP;
				} else {
					// we have a number and something else we dont know
					// default to string
					type = SemossDataType.STRING;
					// clear the tracker so we dont send additional format logic
					formatTracker.clear();
					break ROW_LOOP;
				}
			}
		}

		if (!forceBreak) {
			// if an entire column is empty, type will be null
			// why someone has a csv file with an empty column, i do not know...
			if (type == null) {
				type = SemossDataType.STRING;
			}

			// if format tracking is empty
			// just add the type to the matrix
			// and continue
			if (formatTracker.isEmpty()) {
				Object[] columnPrediction = new Object[2];
				columnPrediction[0] = type;
				predictedTypes[colIndex] = columnPrediction;
			} else {
				// format tracker is not empty
				// need to figure out the date situation
				if (type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
					Object[] results = FileHelperUtil.determineDateFormatting(type, formatTracker);
					predictedTypes[colIndex] = results;
				} else {
					// UGH... how did you get here if you are not a date???
					Object[] columnPrediction = new Object[2];
					columnPrediction[0] = type;
					predictedTypes[colIndex] = columnPrediction;
				}
			}
		}
	}

	/**
	 * Predict the type via casting
	 *
	 * @param value value to inspect
	 * @return inferred SEMOSS type
	 */
	public static SemossDataType getTypeByCast(Object value) {
		if (value instanceof String) {
			return SemossDataType.STRING;
		} else if (value instanceof Number) {
			if (((Number) value).doubleValue() == Math.rint(((Number) value).doubleValue())) {
				return SemossDataType.INT;
			}
			return SemossDataType.DOUBLE;
		} else if (value instanceof SemossDate) {
			// not a perfect check by any means
			// but quick and easy to do
			if (((SemossDate) value).dateHasTimeNotZero()) {
				return SemossDataType.TIMESTAMP;
			} else {
				return SemossDataType.DATE;
			}
		} else if (value instanceof Boolean) {
			return SemossDataType.BOOLEAN;
		}

		return SemossDataType.STRING;
	}

}
