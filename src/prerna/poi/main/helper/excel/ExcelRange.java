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
			result += column.charAt(i) - 'A' + 1;
		}
		return result;
	}

	public String getRangeSyntax() {
		String rangeSyntax = startC + (startRow + 1) + ":" + endC + (endRow + 1);
		return rangeSyntax;

	}
	
}
