package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class RSortSelector implements IExpressionSelector {

	private String columnName;
	private String sortDir;
	
	public RSortSelector(String columnName, String sortDir) {
		this.columnName = columnName;
		this.sortDir = sortDir;
	}
	
	@Override
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		tableColumns.add(columnName);
		return tableColumns;
	}
	
	@Override
	public String toString() {
		if(sortDir.equalsIgnoreCase("DESC")) {
			return "[ order(-rank(" + columnName + ")), ]";
		} else {
			return "[ order(rank(" + columnName + ")), ]";
		}
	}

	@Override
	public String getName() {
		return columnName;
	}
}
