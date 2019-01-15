package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.sablecc.expressions.IExpressionSelector;

public class RColumnSelector implements IExpressionSelector {

	private RDataTable frame;
	private String columnName;
	
	public RColumnSelector(RDataTable frame, String columnName) {
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
		String tableName = frame.getName();
		String uniqueName = tableName + "__" + columnName;
		if(frame.getMetaData().getHeaderTypeAsEnum(uniqueName) == SemossDataType.DATE) {
			StringBuilder builder = new StringBuilder();
			builder.append("format(").append(columnName).append(", '%m/%d/%Y')");
			return builder.toString();
		} else {
			return columnName;
		}
	}

	@Override
	public String getName() {
		return columnName;
	}

}
