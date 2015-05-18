package prerna.algorithm.impl;

import java.util.Map;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;

public class ExactStringMatcher implements IAnalyticRoutine {

	private Map options;
	private final String COLUMN_ONE_KEY = "table1Col";
	private final String COLUMN_TWO_KEY = "table2Col";
	
	@Override
	public void setOptions(Map options) {
		this.options = options;
	}

	@Override
	public Map getOptions() {
		return options;
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		if(data.length != 2) {
			throw new IllegalArgumentException("Input data does not contain exactly 2 ITableDataFrames");
		}
		if(!options.containsKey(COLUMN_ONE_KEY)) {
			throw new IllegalArgumentException("Table 1 Column Header is not specified under " + COLUMN_ONE_KEY + " in options");
		} 
		if(!options.containsKey(COLUMN_TWO_KEY)) {
			throw new IllegalArgumentException("Table 2 Column Header is not specified under " + COLUMN_TWO_KEY + " in options");
		}
		
		ITableDataFrame table1 = data[0];
		ITableDataFrame table2 = data[1];
		
		Object[] table1Col = table1.getColumn(options.get(COLUMN_ONE_KEY).toString());
		Object[] table2Col = table2.getColumn(options.get(COLUMN_TWO_KEY).toString());

		ITableDataFrame results = performMatch(table1Col, table2Col);
		
		return results;
	}

	private ITableDataFrame performMatch(Object[] table1Col, Object[] table2Col) {
		ITableDataFrame bTree = new BTreeDataFrame(new String[]{options.get(COLUMN_ONE_KEY).toString(), options.get(COLUMN_TWO_KEY).toString()});
		
		for(int i = 0; i < table1Col.length; i++) {
			for(int j = 0; j < table2Col.length; j++) {
				if(table1Col[i].equals(table2Col[j])) {
					bTree.addRow(new Object[]{table1Col[i], table2Col[j]});
				}
			}
		}
		
		return bTree;
	}

}
