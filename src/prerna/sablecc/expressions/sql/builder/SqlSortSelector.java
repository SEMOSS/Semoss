package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.expressions.IExpressionSelector;

public class SqlSortSelector implements IExpressionSelector {

	private H2Frame frame;
	private String columnName;
	private String sortDir;
	
	public SqlSortSelector(H2Frame frame, String columnName, String sortDir) {
		this.frame = frame;
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
		return " ORDER BY " + columnName + " " + sortDir.toUpperCase();
	}

	@Override
	public String getName() {
		return this.columnName;
	}
}
