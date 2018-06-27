package prerna.poi.main.helper;

public class Range {
	
	int startCol = -1;
	String startC = null;
	int endCol = -1;
	String endC = null;
	
	int startRow = -1;
	int endRow = -1;
	
	public Range(int startCol, int endCol, int startRow, int endRow)
	{
		this.startCol = startCol;
		this.endCol = endCol;
		this.startRow = startRow;
		this.endRow = endRow;
		
		startC = getCol(startCol);
		endC = getCol(endCol);
	}
	
	public void print()
	{
		System.out.println("Range .. " + startC + (startRow + 1) + ":" + endC + (endRow + 1));
	}
	
	public static String getCol(int columnNumber)
    {
        // To store result (Excel column name)
        StringBuilder columnName = new StringBuilder();
 
        while (columnNumber > 0)
        {
            // Find remainder
            int rem = columnNumber % 26;
 
            // If remainder is 0, then a 
            // 'Z' must be there in output
            if (rem == 0)
            {
                columnName.append("Z");
                columnNumber = (columnNumber / 26) - 1;
            }
            else // If remainder is non-zero
            {
                columnName.append((char)((rem - 1) + 'A'));
                columnNumber = columnNumber / 26;
            }
        }
 
        // Reverse the string and print result
        //System.out.println(columnName.reverse());
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

}
