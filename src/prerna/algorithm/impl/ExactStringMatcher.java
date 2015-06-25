package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.om.SEMOSSParam;

public class ExactStringMatcher implements IAnalyticRoutine {

	private static final Logger LOGGER = LogManager.getLogger(ExactStringMatcher.class.getName());
	
	public final String COLUMN_ONE_KEY = "table1Col";
	public final String COLUMN_TWO_KEY = "table2Col";
	
	protected List<SEMOSSParam> options;
	protected Map<String, Object> resultMetadata = new HashMap<String, Object>();
	
	public ExactStringMatcher(){
		this.options = new ArrayList<SEMOSSParam>();
		
		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.COLUMN_ONE_KEY);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.COLUMN_TWO_KEY);
		options.add(1, p2);
	}
	
	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet)
		{
			for(SEMOSSParam param : options)
			{
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		if(data.length != 2) {
			throw new IllegalArgumentException("Input data does not contain exactly 2 ITableDataFrames");
		}
		
		String table1Header = (String) options.get(0).getSelected();
		String table2Header = (String) options.get(1).getSelected();
		if(table1Header == null) {
			throw new IllegalArgumentException("Table 1 Column Header is not specified under " + COLUMN_ONE_KEY + " in options");
		} 
		if(table2Header == null) {
			throw new IllegalArgumentException("Table 2 Column Header is not specified under " + COLUMN_TWO_KEY + " in options");
		}
		
		ITableDataFrame table1 = data[0];
		ITableDataFrame table2 = data[1];
		
		LOGGER.info("Getting from first table column " + table1Header);
		Object[] table1Col = table1.getColumn(table1Header);

		LOGGER.info("Getting from second table column " + table2Header);
		Object[] table2Col = table2.getColumn(table2Header);

		ITableDataFrame results = performMatch(table1Col, table2Col);
		
		return results;
	}

	protected ITableDataFrame performMatch(Object[] table1Col, Object[] table2Col) {
		String table1ValueKey = "Table1Value";
		String table2ValueKey = "Table2Value";
		
		ITableDataFrame bTree = new BTreeDataFrame(new String[]{table1ValueKey, table2ValueKey});

		int success = 0;
		int total = 0;
		for(int i = 0; i < table1Col.length; i++) {
			for(int j = 0; j < table2Col.length; j++) {
				if(match(table1Col[i],table2Col[j])) {
//					System.out.println("MATCHED::::::::::::::::: " + table1Col[i] + "      " +   table2Col[j]  );
					Map<String, Object> row = new HashMap<String, Object>();
					row.put(table1ValueKey , table1Col[i]);
					row.put(table2ValueKey, table2Col[j]);
					bTree.addRow(row, row); //TODO: adding values as both raw and clean
					success++;
				}
				total++;
			}
		}
		
		this.resultMetadata.put("success", success);
		this.resultMetadata.put("total", total);
		
		return bTree;
	}
	
	protected boolean match(Object obj1, Object obj2){
		return obj1.equals(obj2);
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
		return this.resultMetadata;
	}

	@Override
	public String getResultDescription() {
		return "This routine matches matches objects by calling .toString and then comparing using .equals method";
	}
}
