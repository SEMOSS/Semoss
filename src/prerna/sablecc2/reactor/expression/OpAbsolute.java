package prerna.sablecc2.reactor.expression;

public class OpAbsolute extends OpBasicMath {

	public OpAbsolute() {
		this.operation = "abs";
	}

	@Override
	protected double evaluate(Object[] values) {
		double inputNum = ((Number) values[0]).doubleValue();		
		return Math.abs(inputNum); 
	}
}
