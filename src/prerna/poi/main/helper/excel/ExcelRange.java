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

import org.apache.poi.ss.util.CellReference;

/**
 * Represents an Excel range using one-based row and column coordinates.
 */
public class ExcelRange {

	private int startCol = -1;
	private int endCol = -1;

	private String startC = null;
	private String endC = null;

	private int startRow = -1;
	private int endRow = -1;

	/**
	 * Creates a range from numeric boundaries.
	 *
	 * @param startCol one-based starting column index
	 * @param endCol   one-based ending column index
	 * @param startRow one-based starting row index
	 * @param endRow   one-based ending row index
	 */
	public ExcelRange(int startCol, int endCol, int startRow, int endRow) {
		this.startCol = startCol;
		this.endCol = endCol;
		this.startRow = startRow;
		this.endRow = endRow;

		this.startC = getCol(startCol);
		this.endC = getCol(endCol);
	}

	/**
	 * Creates a range from A1 range syntax.
	 *
	 * @param rangeSyntax range in {@code A1:B10} form
	 */
	public ExcelRange(String rangeSyntax) {
		// range index is start col, start row, end col, end row
		int[] rangeIndex = ExcelRange.getSheetRangeIndex(rangeSyntax);
		this.startCol = rangeIndex[0];
		this.endCol = rangeIndex[2];
		this.startRow = rangeIndex[1];
		this.endRow = rangeIndex[3];

		this.startC = getCol(startCol);
		this.endC = getCol(endCol);
	}

	/**
	 * Converts this range back into A1 syntax.
	 *
	 * @return string representation such as {@code A1:B10}
	 */
	public String getRangeSyntax() {
		String rangeSyntax = startC + startRow + ":" + endC + endRow;
		return rangeSyntax;
	}

	/**
	 * Returns the numeric boundaries of this range.
	 *
	 * @return array in the order: start column, start row, end column, end row
	 */
	public int[] getIndices() {
		return new int[] { startCol, startRow, endCol, endRow };
	}

	/**
	 * Converts a one-based column index to an Excel column name.
	 *
	 * @param columnNumber one-based column index
	 * @return excel column label such as {@code A}, {@code Z}, or {@code AA}
	 */
	public static String getCol(int columnNumber) {
		// To store result (Excel column name)
		StringBuilder columnName = new StringBuilder();

		while (columnNumber > 0) {
			// Find remainder
			int rem = columnNumber % 26;

			// If remainder is 0, then a
			// 'Z' must be there in output
			if (rem == 0) {
				columnName.append("Z");
				columnNumber = (columnNumber / 26) - 1;
			} else {
				// If remainder is non-zero
				columnName.append((char) ((rem - 1) + 'A'));
				columnNumber = columnNumber / 26;
			}
		}

		// Reverse the string and print result
		return columnName.reverse().toString();
	}

	/**
	 * Converts an Excel column label into a one-based numeric column index.
	 *
	 * @param column excel column label
	 * @return one-based column index
	 */
	public static int getExcelColumnNumber(String column) {
		int result = 0;
		for (int i = 0; i < column.length(); i++) {
			result *= 26;
			result += column.charAt(i) - 'A' + 1;
		}
		return result;
	}

	/**
	 * Parse a range to get the start col, start row, end col, end row as a vector
	 * of integers
	 *
	 * @param rangeSyntax range in {@code A1:B10} form
	 * @return array in the order: start column, start row, end column, end row
	 */
	public static int[] getSheetRangeIndex(String rangeSyntax) {
		if (rangeSyntax == null) {
			throw new IllegalArgumentException("Invalid range syntax of " + rangeSyntax);
		}
		String[] split = rangeSyntax.split(":");
		if (split.length != 2) {
			throw new IllegalArgumentException("Invalid range syntax of " + rangeSyntax);
		}

		int[] start = convertExcelCellIndex(split[0]);
		int[] end = convertExcelCellIndex(split[1]);

		return new int[] { start[0], start[1], end[0], end[1] };
	}

	/**
	 * Parse a string representation of an excel reference as a vector of integers
	 * for the column and row
	 * 
	 * @param excelCellIndex cell reference
	 * @return array in the order of column, row
	 */
	private static int[] convertExcelCellIndex(String excelCellIndex) {
		CellReference cellReference = new CellReference(excelCellIndex);
		int col = cellReference.getCol() + 1;
		int row = cellReference.getRow() + 1;
		return new int[] { col, row };
	}

	/**
	 * Gets the one-based starting row for the range.
	 *
	 * @return start row index
	 */
	public int getStartRow() {
		return this.startRow;
	}

//	public static void main(String[] args) {
//		String rStr = "A1:EJ1459";
//		int[] rIdx = getSheetRangeIndex(rStr);
//		System.out.println("START : " + rIdx[0] + ", " + rIdx[1]);
//		System.out.println("END : " + rIdx[2] + ", " + rIdx[3]);
//
//		System.out.println(">>> ");
//		
//		rStr = "A1:AA9";
//		rIdx = getSheetRangeIndex(rStr);
//		System.out.println("START : " + rIdx[0] + ", " + rIdx[1]);
//		System.out.println("END : " + rIdx[2] + ", " + rIdx[3]);
//	}

}
