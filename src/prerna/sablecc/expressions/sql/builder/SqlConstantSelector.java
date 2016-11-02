package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlConstantSelector implements IExpressionSelector {

	Object value = null;
	
	public SqlConstantSelector(Object value) {
		this.value = value;
	}
	
	public String toString() {
		return "'" + value + "'";
	}
	
	public Object getValue() {
		return this.value;
	}
	
	@Override
	public List<String> getTableColumns() {
		return new Vector<String>();
	}

	@Override
	public String getName() {
		return "Constant_" + value.toString().replaceAll("[^a-zA-Z0-9]", "") + "";
	}

}
