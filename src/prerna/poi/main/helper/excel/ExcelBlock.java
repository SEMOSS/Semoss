package prerna.poi.main.helper.excel;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class ExcelBlock {

	// keep track of statistics on the start column
	private SummaryStatistics startColumnIndexStats = new SummaryStatistics();
	
	// keep track of the statistics on the end column
	private SummaryStatistics totalColumnsInRowStats = new SummaryStatistics();
	
	// contain a list of the indicies that this block contains
	private List<Integer> rowIndicesInBlock = new Vector<Integer>();
	
	private int lastColMaxIndex = -1;
	
	private Map<Integer, SummaryStatistics> columnToRowIndexStats = new Hashtable <Integer, SummaryStatistics>();

	/**
	 * Get the ranges of the block
	 * This will split up the entire segment if there are empty columns
	 * @return
	 */
	public List<ExcelRange> getRanges() {
		List<ExcelRange> ranges = new Vector<ExcelRange>();

		boolean started = false;
		int startCol = 0;
		
		for(int colIndex = 0; colIndex <= lastColMaxIndex; colIndex++) {
			if(!columnToRowIndexStats.containsKey( new Integer(colIndex) ) && started) {
				
				if(columnToRowIndexStats.containsKey(startCol)) {
					ExcelRange r = new ExcelRange(
							startCol+1, 
							colIndex, 
							new Double(columnToRowIndexStats.get(new Integer(startCol)).getMin()).intValue(), 
							new Double(columnToRowIndexStats.get(new Integer(colIndex-1)).getMax()).intValue());
					ranges.add(r);
				}
				
				startCol = 0;
				started = false;
			}
			
			if(columnToRowIndexStats.containsKey(new Integer(colIndex)) && !started ) {
				started = true;
				startCol = colIndex;
			}
		}
		
		return ranges;
	}

	/**
	 * Determine if 2 blocks are the same
	 * @param block
	 * @return
	 */
	public boolean sameAs(ExcelBlock newBlock) {
		boolean retValue = false;
		
		double startDiff = Math.abs(this.startColumnIndexStats.getMean() - newBlock.startColumnIndexStats.getMean());
		double endDiff = Math.abs(this.totalColumnsInRowStats.getMean() - newBlock.totalColumnsInRowStats.getMean());
		
		// need to check to see if they are within the standard deviation
		// if the start column is similar
		// and if the number of columns is similar then there is a possibilty this is the same block
		
		if(startDiff <= this.startColumnIndexStats.getStandardDeviation()) {
			if(endDiff <= this.totalColumnsInRowStats.getStandardDeviation()) {
				retValue = true;
			}
		}
		
		return retValue;
	}
	
	public void merge(ExcelBlock equivBlock) {
		this.rowIndicesInBlock.addAll(equivBlock.rowIndicesInBlock);
		// try to set the last column
		this.trySetLastColMaxIndex(equivBlock.lastColMaxIndex);
	}
	
	
	/**
	 * Add the start column index of this row
	 * @param colIndex
	 */
	public void addStartColumnIndex(int colIndex) {
		startColumnIndexStats.addValue(colIndex);
	}
	
	/**
	 * Add the total number of columns to this row
	 * @param numRows
	 */
	public void addTotalColumnsInRowStats(int numRows) {
		totalColumnsInRowStats.addValue(numRows);
	}
	
	/**
	 * Add the row index if it contains data
	 * @param rowIndex
	 */
	public void addRowIndexContainingData(int rowIndex) {
		rowIndicesInBlock.add(rowIndex);
	}
	
	/**
	 * Determine if this block is empty
	 * @return
	 */
	public boolean isEmpty() {
		return rowIndicesInBlock.isEmpty();
	}
	
	public void addColumnToRowIndexWithData(int columnIndex, int rowIndex) {
		SummaryStatistics stats = null;
		if(columnToRowIndexStats.containsKey(columnIndex)) {
			stats = columnToRowIndexStats.get(new Integer(columnIndex));
		} else {
			stats = new SummaryStatistics();
			columnToRowIndexStats.put(new Integer(columnIndex), stats);
		}
		
		stats.addValue(rowIndex);
	}
	
	/**
	 * Try to set a new last col max index
	 * @param startColMinIndex
	 */
	public void trySetLastColMaxIndex(int lastColMaxIndex) {
		if(this.lastColMaxIndex == -1) {
			this.lastColMaxIndex = lastColMaxIndex;
		} else if(this.lastColMaxIndex < lastColMaxIndex) {
			this.lastColMaxIndex = lastColMaxIndex;
		}
	}
	
}
