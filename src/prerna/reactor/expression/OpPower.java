package prerna.reactor.expression;

public class OpPower extends OpBasicMath {

	public OpPower() {
		this.operation = "power";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}
	
	public static double eval(Object...values) {
		double number = ((Number)values[0]).doubleValue();
		double power = ((Number)values[1]).doubleValue();
		double powerVal = Math.pow(number, power);
		return powerVal;
	}
	
	public static double eval(double[] values) {
		double number = values[0];
		double power = values[1];
		double powerVal = Math.pow(number, power);
		return powerVal;
	}
}
