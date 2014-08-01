package prerna.algorithm.impl;

import java.math.BigDecimal;

public class DistanceCalculator {
	
	public Double calculateEuclidianDistance(Double[] values1, Double[] values2) throws IllegalArgumentException {
		if(values1.length != values2.length) {
			throw new IllegalArgumentException("The inputs must be of the same size!");
		}
		
		Double sumSquare = (double) 0;
		for(int i = 0; i < values1.length; i++) {
			sumSquare += Math.pow(values1[i]-values2[i],2);
		}
		
		return Math.sqrt(sumSquare);
	}
	
	public BigDecimal calculateEuclidianDistance(BigDecimal[] values1, BigDecimal[] values2) throws IllegalArgumentException {
		if(values1.length != values2.length) {
			throw new IllegalArgumentException("The inputs must be of the same size!");
		}
		
		BigDecimal sumSquare = new BigDecimal(0);
		for(int i = 0; i < values1.length; i++) {
			sumSquare.add(values1[i].subtract(values2[i]).pow(2));
		}
		return sqrt(sumSquare);
	}

	private BigDecimal sqrt(BigDecimal num) {
		BigDecimal val = new BigDecimal(Math.sqrt(num.doubleValue()));
		return val.add(new BigDecimal(num.subtract(val.multiply(val)).doubleValue() / (val.doubleValue() * 2.0) ));
	}

	public double calculateEuclidianDistance(double[] values1, double[] values2) throws IllegalArgumentException {
		if(values1.length != values2.length) {
			throw new IllegalArgumentException("The inputs must be of the same size!");
		}
		
		Double sumSquare = (double) 0;
		for(int i = 0; i < values1.length; i++) {
			sumSquare += Math.pow(values1[i]-values2[i],2);
		}
		
		return Math.sqrt(sumSquare);
	}

}
