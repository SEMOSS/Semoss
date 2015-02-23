/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.algorithm.cluster;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MatrixRegressionAlgorithm extends OLSMultipleLinearRegression{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionAlgorithm.class.getName());

	private double[] coeffArray;
	private double[] coeffErrorsArray;
	private double[] residualArray;
	private double[] estimateArray;
	private double standardError;

	public double[] getCoeffArray() {
		return coeffArray;
	}

	public double[] getCoeffErrorsArray() {
		return coeffErrorsArray;
	}
	
	public double[] getResidualArray() {
		return residualArray;
	}

	public double[] getEstimateArray() {
		return estimateArray;
	}

	public double getStandardError() {
		return standardError;
	}
	
	/**
	 * Creates a MatrixRegressionAlgorithm object to solve A*theta = y.
	 * @param A  
	 * @param y
	 */
	public MatrixRegressionAlgorithm(double[][] A, double[] y) {
		newSampleData(y, A);
		setNoIntercept(false);
	}
	

	/**
	 * Determines the coefficients, errors in the coefficients, estimates, and residuals.
	 */
	public void execute() {

		long startTime = System.currentTimeMillis();

		RealVector theta = calculateBeta();
		coeffArray = theta.toArray();
		//create estimate matrix from the coefficients and x values
		estimateArray = getX().operate(theta).toArray();
		
		coeffErrorsArray = estimateRegressionParametersStandardErrors();
		residualArray = estimateResiduals();
		
		double residualSumOfSquares = calculateResidualSumOfSquares();
		standardError = Math.sqrt(residualSumOfSquares / residualArray.length);
				
		long endTime = System.currentTimeMillis();
		System.out.println("Total Time = " + (endTime-startTime)/1000 );
	}

}