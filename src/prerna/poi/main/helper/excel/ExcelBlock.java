package prerna.poi.main.helper.excel;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.FileHelperUtil;

public class ExcelBlock {

	// keep track of statistics on the start column
	private SummaryStatistics startColumnIndexStats = new SummaryStatistics();
	
	// keep track of the statistics on the end column
	private SummaryStatistics totalColumnsInRowStats = new SummaryStatistics();
	
	// contain a list of the indicies that this block contains
	private List<Integer> rowIndicesInBlock = new Vector<Integer>();
	
	private int lastColMaxIndex = -1;
	
	private Map<Integer, SummaryStatistics> columnToRowIndexStats = new Hashtable <Integer, SummaryStatistics>();
	
	// for data types
	private Map<Integer, Map<SemossDataType, SummaryStatistics>> columnToTypeStats = new Hashtable <Integer, Map<SemossDataType, SummaryStatistics>>();
	private Map<Integer, Map<String, SummaryStatistics>> additionalFormatTracker = new HashMap<Integer, Map<String, SummaryStatistics>>();
	
	/**
	 * Get the ranges of the block
	 * This will split up the entire segment if there are empty columns
	 * @return
	 */
	public List<ExcelRange> getRanges() {
		List<ExcelRange> ranges = new Vector<ExcelRange>();
		boolean started = false;
		int startCol = 0;
		int max = new Double(columnToRowIndexStats.get(new Integer(0)).getMax()).intValue();
		for(int colIndex = 0; colIndex <= lastColMaxIndex; colIndex++) {
			// get max row by comparing every max in each column
			SummaryStatistics rowIndexStats = columnToRowIndexStats.get(new Integer(colIndex - 1));
			if (rowIndexStats != null) {
				int tempMax = new Double(rowIndexStats.getMax()).intValue();
				if (tempMax > max) {
					max = tempMax;
				}
			}
			
			if(!columnToRowIndexStats.containsKey( new Integer(colIndex) ) && started) {
				if(columnToRowIndexStats.containsKey(startCol)) {
					int min = new Double(columnToRowIndexStats.get(new Integer(startCol)).getMin()).intValue();
					ExcelRange r = new ExcelRange(startCol + 1, colIndex, min, max);
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
	
	public Object[][] getRangeTypes(ExcelRange range) {
		int[] rangeIndex = range.getIndices();
		int numCols = rangeIndex[2] - rangeIndex[0] + 1;
		Object[][] predictedTypes = new Object[numCols][3];
		
		// loop through based on the range to figure out the types
		// note, this is just < and not <= in the loop
		int colIndex = 0;
		COLUMN_LOOP : for(int cellIndex = rangeIndex[0]-1; cellIndex < rangeIndex[2]; cellIndex++, colIndex++) {
			// get column index
			
			int startRow = rangeIndex[1];
			int endRow = rangeIndex[3];
			
			Map<SemossDataType, SummaryStatistics> typesMap = columnToTypeStats.get(cellIndex);
			
			// we gotta see based on the range
			// what the type will be
			// basically
			// if the summary stats min/max contains the startrow/endrow
			// we know it is that type
			// but the order of the checks is important
			// if it contains 1 string, then string
			// it it contains multiple types, then string
			// if it is combination of int and double, then double
			// if it is combination of timestamp and date, then timestamp
			// if other combination, then string
			
			boolean containsStr = false;
			boolean containsInt = false;
			boolean containsDouble = false;
			boolean containsDate = false;
			boolean containsTimestamp = false;
			
			if(typesMap.containsKey(SemossDataType.STRING)) {
				containsStr = testTypeContainedWtihinRange(startRow, endRow, typesMap.get(SemossDataType.STRING));
				// if we have string, we are done
				if(containsStr) {
					Object[] columnPrediction = new Object[2];
					columnPrediction[0] = SemossDataType.STRING;
					predictedTypes[colIndex] = columnPrediction;
					continue COLUMN_LOOP;
				}
			}
			
			if(typesMap.containsKey(SemossDataType.INT)) {
				containsInt = testTypeContainedWtihinRange(startRow, endRow, typesMap.get(SemossDataType.INT));
			}
			if(typesMap.containsKey(SemossDataType.DOUBLE)) {
				containsDouble = testTypeContainedWtihinRange(startRow, endRow, typesMap.get(SemossDataType.DOUBLE));
			}
			if(typesMap.containsKey(SemossDataType.DATE)) {
				containsDate = testTypeContainedWtihinRange(startRow, endRow, typesMap.get(SemossDataType.DATE));
			}
			if(typesMap.containsKey(SemossDataType.TIMESTAMP)) {
				containsTimestamp = testTypeContainedWtihinRange(startRow, endRow, typesMap.get(SemossDataType.TIMESTAMP));
			}
			
			
			// if we have some kind of number
			// if only int, then int
			// otherwise, double
			if(!containsDate && !containsTimestamp && (containsInt || containsDouble)) {
				Object[] columnPrediction = new Object[2];
				if(containsInt && !containsDouble) {
					columnPrediction[0] = SemossDataType.INT;
				} else {
					columnPrediction[0] = SemossDataType.DOUBLE;
				}
				predictedTypes[colIndex] = columnPrediction;
				continue COLUMN_LOOP;
			}
			
			// if we have some kind of date or timestamp
			// if only date, then date
			// otherwise, timestamp
			if(!containsInt && !containsDouble && (containsDate || containsTimestamp)) {
				Map<String, SummaryStatistics> formatting = additionalFormatTracker.get(cellIndex);
				
				Map<String, Integer> mostPopularFormat = new HashMap<String, Integer>();
				for(String s : formatting.keySet()) {
					mostPopularFormat.put(s, (int) formatting.get(s).getN());
				}
				// reconcile formats
				FileHelperUtil.reconcileDateFormats(mostPopularFormat);
				String mostOccuringFormat = Collections.max(mostPopularFormat.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
				
				Object[] columnPrediction = new Object[2];
				if(containsDate && !containsTimestamp) {
					columnPrediction[0] = SemossDataType.DATE;
				} else {
					columnPrediction[0] = SemossDataType.TIMESTAMP;
				}
				columnPrediction[1] = mostOccuringFormat;
				predictedTypes[colIndex] = columnPrediction;
				continue COLUMN_LOOP;
			}
			
			// we have mixed types
			// return string
			Object[] columnPrediction = new Object[2];
			columnPrediction[0] = SemossDataType.STRING;
			predictedTypes[colIndex] = columnPrediction;
			continue COLUMN_LOOP;
		}
		
		
		return predictedTypes;
	}
	
	private boolean testTypeContainedWtihinRange(int startRow, int endRow, SummaryStatistics stats) {
		// we need to ignore the type for the startRow
		double minRow = stats.getMin();
		double maxRow = stats.getMax();
		
		if(startRow > minRow) {
			return true;
		}
		
		if(endRow <= maxRow) {
			return true;
		}
		
		return false;
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
	
	/**
	 * Get the number of indices in the block
	 * @return
	 */
	public int numIndicesInBlock() {
		return rowIndicesInBlock.size();
	}
	
	public void addColumnToRowIndexWithData(int columnIndex, int rowIndex, SemossDataType type, String additionalType) {
		Integer objColumnIndex = new Integer(columnIndex);
		// update index stats
		SummaryStatistics stats = null;
		if(columnToRowIndexStats.containsKey(objColumnIndex)) {
			stats = columnToRowIndexStats.get(objColumnIndex);
		} else {
			stats = new SummaryStatistics();
			columnToRowIndexStats.put(objColumnIndex, stats);
		}
		
		stats.addValue(rowIndex);
		
		// update data type stats
		Map<SemossDataType, SummaryStatistics> typeMap = null;
		if(columnToTypeStats.containsKey(objColumnIndex)) {
			typeMap = columnToTypeStats.get(objColumnIndex);
		} else {
			typeMap = new HashMap<SemossDataType, SummaryStatistics>();
			columnToTypeStats.put(objColumnIndex, typeMap);
		}
		
		SummaryStatistics typeStats = null;
		if(typeMap.containsKey(type)) {
			typeStats = typeMap.get(type);
		} else {
			typeStats = new SummaryStatistics();
			typeMap.put(type, typeStats);
		}
		
		typeStats.addValue(rowIndex);
		
		// also acount for additional types
		if(additionalType != null) {
			Map<String, SummaryStatistics> tracker = null;
			if(additionalFormatTracker.containsKey(objColumnIndex)) {
				tracker = additionalFormatTracker.get(objColumnIndex);
			} else {
				tracker = new HashMap<String, SummaryStatistics>();
				additionalFormatTracker.put(objColumnIndex, tracker);
			}
			
			SummaryStatistics additionalTypeStats = null;
			if(tracker.containsKey(additionalType)) {
				additionalTypeStats = tracker.get(additionalType);
			} else {
				additionalTypeStats = new SummaryStatistics();
				tracker.put(additionalType, additionalTypeStats);
			}
			stats.addValue(rowIndex);
		}
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
