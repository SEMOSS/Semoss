package prerna.reactor.expression;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class OpRound extends OpBasicMath {
	
	public OpRound() {
		this.operation = "round";
	}

	@Override
	protected double evaluate(Object[] values) {
        return eval(values);
	}
	
	public static double eval(Object...values) {
		double inputVal = ((Number) values[0]).doubleValue();
        int digitsToRound = ((Number) values[1]).intValue();        
        BigDecimal roundedNum = new BigDecimal(String.valueOf(inputVal)).setScale(digitsToRound, RoundingMode.HALF_UP);
        return roundedNum.doubleValue();
	}
	
	public static double eval(double[] values) {
		double inputVal = values[0];
        int digitsToRound = ((Number) values[1]).intValue();        
        BigDecimal roundedNum = new BigDecimal(String.valueOf(inputVal)).setScale(digitsToRound, RoundingMode.HALF_UP);
        return roundedNum.doubleValue();
	}
}