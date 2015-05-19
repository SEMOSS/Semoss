package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.om.SEMOSSParam;

public class ExactStringMatcher implements IAnalyticRoutine {

	private Map options;
	public final String COLUMN_ONE_KEY = "table1Col";
	public final String COLUMN_TWO_KEY = "table2Col";
	int success = 0;
	int total = 0;
	
	@Override
	public void setOptions(Map options) {
		this.options = options;
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		List<SEMOSSParam> options = new ArrayList<SEMOSSParam>();
		
		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.COLUMN_ONE_KEY);
		options.set(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.COLUMN_TWO_KEY);
		options.set(1, p2);
		
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
					success++;
				}
				total++;
			}
		}
		
		return bTree;
	}

	@Override
	public String getName() {
		return "Exact String Matcher";
	}

	@Override
	public String getDefaultViz() {
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		Map<String, Object> results = new HashMap<String, Object>();
		results.put("success", this.success);
		results.put("total", this.total);
		return null;
	}

	@Override
	public String getResultDescription() {
		return "This routine matches matches objects by calling .toString and then comparing using .equals method";
	}

}
