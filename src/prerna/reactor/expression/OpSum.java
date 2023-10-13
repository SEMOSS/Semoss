package prerna.reactor.expression;

public class OpSum extends OpBasicMath {

	public OpSum() {
		this.operation = "sum";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}
	
	
	public static double eval(Object... values) {
		double sum = 0;
		
		for(Object val : values) {
			sum += ((Number) val).doubleValue();
		}
		return sum;
	}
	
	public static double eval(double[] values) {
		double sum = 0;
		
		for(Object val : values) {
			sum += ((Number) val).doubleValue();
		}
		return sum;
	}
}
