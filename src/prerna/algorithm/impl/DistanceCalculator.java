/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
			sumSquare = sumSquare.add(values1[i].subtract(values2[i]).pow(2));
		}
		return sqrt(sumSquare);
	}

	private BigDecimal sqrt(BigDecimal num) {
		BigDecimal val = BigDecimal.valueOf(Math.sqrt(num.doubleValue()));
		return val.add( BigDecimal.valueOf(num.subtract(val.multiply(val)).doubleValue() / (val.doubleValue() * 2.0)) );
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
