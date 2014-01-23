package prerna.rdf.util;

import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

class ValueConstantCollector extends QueryModelVisitorBase<Exception> {
	public Object value;

	@Override
	public void meet(ValueConstant node) {
		System.out.println("Value Constant is  " + node.getValue());
		value = node.getValue();
		// System.out.println(node.getValue().);
	}

}