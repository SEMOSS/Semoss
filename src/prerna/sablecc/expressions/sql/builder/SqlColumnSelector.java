package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.ds.H2.H2Frame;

public class SqlColumnSelector implements ISqlSelector{

	private H2Frame frame;
	private String columnName;
	
	public SqlColumnSelector(H2Frame frame, String columnName) {
		this.frame = frame;
		this.columnName = columnName;
	}
	
	@Override
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		tableColumns.add(frame.getTableColumnName(columnName));
		return tableColumns;
	}
	
	@Override
	public String toString() {
		return frame.getTableColumnName(columnName);
	}
}
