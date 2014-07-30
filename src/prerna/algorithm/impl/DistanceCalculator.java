package prerna.algorithm.impl;

import java.math.BigDecimal;

public class DistanceCalculator {
	
	public Double calculateEuclidianDistance(Double[] values) {
		Double sum = (double) 0;
		for(int i = 0; i < values.length; i++) {
			sum += Math.pow(values[i],2);
		}
		return Math.sqrt(sum);
	}
	
	public BigDecimal calculateEuclidianDistance(BigDecimal[] values) {
		BigDecimal sum = new BigDecimal(0);
		for(int i = 0; i < values.length; i++) {
			sum.add(values[i].pow(2));
		}
		return sqrt(sum);
	}

	private BigDecimal sqrt(BigDecimal num) {
		BigDecimal val = new BigDecimal(Math.sqrt(num.doubleValue()));
		return val.add(new BigDecimal(num.subtract(val.multiply(val)).doubleValue() / (val.doubleValue() * 2.0) ));
	}

}
