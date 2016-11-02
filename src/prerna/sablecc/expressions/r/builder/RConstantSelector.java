package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class RConstantSelector implements IExpressionSelector{

	Object value = null;
	
	public RConstantSelector(Object value) {
		this.value = value;
	}
	
	public String toString() {
		return value + "";
	}
	
	@Override
	public List<String> getTableColumns() {
		return new Vector<String>();
	}

	@Override
	public String getName() {
		return value + "";
	}
	
}
