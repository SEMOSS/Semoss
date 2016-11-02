package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

public class RConstantSelector implements IRExpressionSelector{

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
		return value.toString();
	}
	
}
