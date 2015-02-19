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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import Jama.Matrix;

public class MatrixRegressionAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionAlgorithm.class.getName());

	private Matrix A;
	private Matrix b;
	private Matrix coeffMatrix;
	private double rNorm;

	public Matrix getCoeffMatrix() {
		return coeffMatrix;
	}
	
	public double[] getCoeffArray() {
		return coeffMatrix.getRowPackedCopy();
	}

	public double getRNorm() {
		return rNorm;
	}

	
	/**
	 * Sets the matricies for regression analysis to solve Ax = b.
	 * @param A  
	 * @param b
	 */
	public MatrixRegressionAlgorithm(double[][] A, double[] b) {
		this.A = new Matrix(A);
		this.b = new Matrix(b,b.length);
	}
	

	/**
	 * Determines the coefficients for the matricies provided.
	 */
	public void execute() {

		long startTime = System.currentTimeMillis();
		
		coeffMatrix = A.solve(b);
		Matrix residual = A.times(coeffMatrix).minus(b);
		rNorm = residual.normInf();
		
		long endTime = System.currentTimeMillis();
		System.out.println("Total Time = " + (endTime-startTime)/1000 );
	}


}
