package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

public class SqlConstantSelector implements ISqlSelector {

	Object value = null;
	
	public SqlConstantSelector(Object value) {
		this.value = value;
	}
	
	public String toString() {
		return value + "";
	}
	
	@Override
	public List<String> getTableColumns() {
		return new Vector<String>();
	}

}
