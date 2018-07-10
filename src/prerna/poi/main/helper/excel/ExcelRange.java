package prerna.poi.main.helper.excel;

public class ExcelRange {

	private int startCol = -1;
	private int endCol = -1;

	private String startC = null;
	private String endC = null;
	
	private int startRow = -1;
	private int endRow = -1;
	
	public ExcelRange(int startCol, int endCol, int startRow, int endRow) {
		this.startCol = startCol;
		this.endCol = endCol;
		this.startRow = startRow;
		this.endRow = endRow;
		
		this.startC = getCol(startCol);
		this.endC = getCol(endCol);
	}
	
	public ExcelRange(String rangeSyntax) {
		// range index is start col, start row, end col, end row
		int[] rangeIndex = ExcelRange.getSheetRangeIndex(rangeSyntax);
		this.startCol = rangeIndex[0];
		this.endCol = rangeIndex[2];
		this.startRow = rangeIndex[1]+1;
		this.endRow = rangeIndex[3]+1;
		
		this.startC = getCol(startCol);
		this.endC = getCol(endCol);
	}

	public String getRangeSyntax() {
		String rangeSyntax = startC + startRow + ":" + endC + endRow;
		return rangeSyntax;
	}
	
	public int[] getIndices() {
		return new int[]{startCol, startRow, endCol, endRow};
	}

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
				columnName.append((char)((rem - 1) + 'A'));
				columnNumber = columnNumber / 26;
			}
		}

		// Reverse the string and print result
		return columnName.reverse().toString();
	}

	public static int getExcelColumnNumber(String column) {
		int result = 0;
		for (int i = 0; i < column.length(); i++) {
			result *= 26;
			result += column.charAt(i) - 'A';
		}
		return result;
	}
	
	/**
	 * Parse a range to get the start col, start row, end col, end row
	 * as a vector of integers
	 * @param rangeSyntax
	 * @return
	 */
	public static int[] getSheetRangeIndex(String rangeSyntax) {
		String[] split = rangeSyntax.split(":");
		if(split.length != 2) {
			throw new IllegalArgumentException("Invalid range syntax of " + rangeSyntax);
		}
		
		int[] start = convertExcelCellIndex(split[0]);
		int[] end = convertExcelCellIndex(split[1]);
		
		return new int[]{start[0], start[1], end[0], end[1]};
	}
	
	private static int[] convertExcelCellIndex(String excelCellIndex) {
		int col = 0;
		int row = 0;
		
		// excel syntax is AAB50
		// break apart the numbers to get the row
		// convert string to number to get the col
		
		// lets do the easy one...
		String rowStr = excelCellIndex.replaceAll("[A-Z]+", "");
		row = Integer.parseInt(rowStr);
		
		// this isn't that bad since ASCII 'A' starts at 1
		String colStr = excelCellIndex.replaceAll("\\d+", "");
		int colLength = colStr.length();
		for(int i = 0; i < colLength; i++) {
			char c = colStr.charAt(i);
			col += ((int) c);
		}
		
		col = col - (((int) 'A') * colLength) + 1;
		
		return new int[]{col, row};
	}
	
}
