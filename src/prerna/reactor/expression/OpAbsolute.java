package prerna.reactor.expression;

public class OpAbsolute extends OpBasicMath {

	public OpAbsolute() {
		this.operation = "abs";
	}

	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}
	
	public static double eval(Object...values) {
		double inputNum = ((Number) values[0]).doubleValue();		
		return Math.abs(inputNum);
	}
	
	public static double eval(double[] values) {
		double inputNum = ((Number) values[0]).doubleValue();		
		return Math.abs(inputNum);
	}
}
