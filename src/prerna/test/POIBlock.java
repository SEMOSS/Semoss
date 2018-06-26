package prerna.test;

import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class POIBlock {
	
	public int startRow = -1;
	public int endRow = -1;
	public int startCol = -1;
	public int endCol = -1;
	
	public SummaryStatistics filledCols = new SummaryStatistics();
	public SummaryStatistics startCols = new SummaryStatistics();
	public SummaryStatistics endCols = new SummaryStatistics();
	public SummaryStatistics endColsData = new SummaryStatistics();
	

	public List <Integer> rows = new Vector<Integer>();
	public List <Integer> filledColsData = new Vector<Integer>();
	public List <Integer> startColsData = new Vector<Integer>();
	
	public Hashtable emptyColCount = new Hashtable();
	
	public Hashtable <Integer, SummaryStatistics> col2Rows = new Hashtable <Integer, SummaryStatistics>();
	
	Vector <Range> ranges = new Vector<Range>();
	
	// need a filled cols per row
	
	

	public boolean sameAs(POIBlock block)
	{
		boolean retValue = false;
		
		double startDiff = Math.abs(this.startCols.getMean() - block.startCols.getMean());
		double endDiff = Math.abs(this.endCols.getMean() - block.endCols.getMean());
		
		// need to check to see if they are within the standard deviation
		
		// if the start column is similar
		// and if the number of columns / endcols is similar then there is a possibilty this is the same block
		if(startDiff <= this.startCols.getStandardDeviation())
		{
			//System.out.println("Starts at the same point");
			if(endDiff <= this.endCols.getStandardDeviation())
			{
				//System.out.println("Endss at the same point");				
				
				retValue = true;
			}
		}
		return retValue;
	}
	
	public void addRowForCol(int col, int row)
	{
		/*Vector <Integer> rowVec = new Vector<Integer>();
		if(col2Rows.containsKey(col))
			rowVec = col2Rows.get(col);
		
		rowVec.add(row);
		col2Rows.put(col, rowVec); */
		
		SummaryStatistics rowStat = new SummaryStatistics();
		
		if(col2Rows.containsKey(col))
			rowStat = col2Rows.get(col);
		
		rowStat.addValue(row);
		
		col2Rows.put(col, rowStat);
		
	}
	
	public void addEmptyCol(int col)
	{
		int count = 0;
		if(emptyColCount.containsKey(col))
			count = (Integer)emptyColCount.get(col);
		count++;
		emptyColCount.put(col, count);
	}
	
	public void mergeBlock(POIBlock block)
	{
		this.rows.addAll(block.rows);
		this.filledColsData.addAll(block.filledColsData);
		this.startColsData.addAll(block.startColsData);
	}
	
	public void print()
	{
		
		/*int numCols = ((Double)filledCols.getMean()).intValue();
		
		System.out.println("Average of filled columns is.. " + numCols);
		for(int filledIndex = 0;filledIndex < filledColsData.size();filledIndex++)
		{
			if(filledColsData.get(filledIndex) >= numCols)
			{
				startRow = rows.get(filledIndex);
				break;
			}
		}
		
		if(rows.size() > 0)
		{
			System.out.println("Start row " + rows.get(0));
			System.out.println("Header " + startRow);
			System.out.println("Projected Start Column" + startCol);
			System.out.println("Columns.. " + this.startCols.getMin() + ".." + this.endCols.getMax());
			System.out.println("End row " + rows.get(rows.size() - 1));
		}
		else
			System.out.println("Ok.. that is a weird block");
		*/
		// print the empty col count
		
		//System.out.println("Empty Col Count >> " + emptyColCount);
		findSubBlocks();
	}
	
	public void findSubBlocks()
	{
		int numSub = 0;
		boolean started = false;
		
		int startCol = 0;
		int endCol = 0;
		
		for(int col = 0;col <= this.endColsData.getMax();col++)
		{
			if(!col2Rows.containsKey(col) && started)
			{
				numSub++;
				//System.out.println("Split on.. " + col);
				started = false;
				
				endCol = col--;
				//System.out.println("Start column.. " + startCol + " ," + endCol + " >> Max " + this.endColsData.getMax());
				
				//System.out.println("Range Start Row, Col - " + col2Rows.get(startCol).getMin() + "," + startCol);
				//System.out.println("Range Start Row, Col - " + col2Rows.get(startCol).getMax() + "," + endCol);
				if(ranges.size() == 0)
					startCol--;
				
				if(col2Rows.containsKey(startCol))
				{
					Range r = new Range(startCol, endCol, new Double(col2Rows.get(startCol).getMin()).intValue(), new Double(col2Rows.get(startCol).getMax()).intValue());
					ranges.add(r);
					r.print();
				}
				
				startCol = 0;
				endCol = 0;
			}
			else if(col2Rows.containsKey(col))
			{
				started = true;
				startCol = col;
			}
		}
		//System.out.println("Number of Ranges " + ranges.size());
	}

	public Vector<Range> getRange() {
		// TODO Auto-generated method stub
		return ranges;
	}
	
	

}
