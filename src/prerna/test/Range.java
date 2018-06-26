package prerna.test;

public class Range {
	
	int startCol = -1;
	int endCol = -1;
	
	int startRow = -1;
	int endRow = -1;
	
	public Range(int startCol, int endCol, int startRow, int endRow)
	{
		this.startCol = startCol;
		this.endCol = endCol;
		this.startRow = startRow;
		this.endRow = endRow;
	}
	
	public void print()
	{
		System.out.println("Range .. " + startRow + ":" + startCol + " <> " + endRow + ":" + endCol);
	}

}
