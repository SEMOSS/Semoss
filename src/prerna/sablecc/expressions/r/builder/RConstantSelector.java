package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class RConstantSelector implements IExpressionSelector{

	private Object value = null;
	private List<String> tableColumnsUsed = new Vector<String>();
	
	public RConstantSelector(Object value) {
		this.value = value;
	}
	
	public String toString() {
		return value + "";
	}
	
	public Object getValue() {
		return this.value;
	}
	
	@Override
	public List<String> getTableColumns() {
		return tableColumnsUsed;
	}

	public void setTableColumnsUsed(List<String> tableColumnsUsed) {
		this.tableColumnsUsed = tableColumnsUsed;
	}
	
	@Override
	public String getName() {
		return value + "";
	}
	
}
