package prerna.sablecc2.reactor.expression;

public class OpPower extends OpBasicMath {

	public OpPower() {
		this.operation = "power";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		double number = ((Number)values[0]).doubleValue();
		double power = ((Number)values[1]).doubleValue();
		double powerVal = Math.pow(number, power);
		return powerVal;
	}
}
