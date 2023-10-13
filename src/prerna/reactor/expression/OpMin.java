package prerna.reactor.expression;

public class OpMin extends OpBasicMath {

	public OpMin() {
		this.operation = "min";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}
	
	public static double eval(Object...values) {
		double min = Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextNumber = ((Number) val).doubleValue();
			min = min > nextNumber ? nextNumber : min;
		}
		
		return min;
	}
	
	public static double eval(double[] values) {
		double min = Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextNumber = ((Number) val).doubleValue();
			min = min > nextNumber ? nextNumber : min;
		}
		
		return min;
	}
}
