package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.expressions.IExpressionSelector;

public class SqlColumnSelector implements IExpressionSelector{

	private H2Frame frame;
	private String columnName;
	
	public SqlColumnSelector(H2Frame frame, String columnName) {
		this.frame = frame;
		this.columnName = columnName;
	}
	
	@Override
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		tableColumns.add(columnName);
		return tableColumns;
	}
	
	@Override
	public String toString() {
		return columnName;
	}

	@Override
	public String getName() {
		return this.columnName;
	}
}
