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
package prerna.algorithm.learning.supervized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;

public class MatrixRegressionAlgorithm extends OLSMultipleLinearRegression implements IAnalyticRoutine {

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionAlgorithm.class.getName());

	private static final String DATA_MATRIX = "A";
	private static final String RESPONSE_VECTOR = "y";
	
	private ArrayList<SEMOSSParam> options;

	private double[] coeffArray;
	private double[] coeffErrorsArray;
	private double[] residualArray;
	private double[] estimateArray;
	private double standardError;

	/**
	 * Creates a MatrixRegressionAlgorithm object to solve A*theta = y.
	 */
	public MatrixRegressionAlgorithm() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(DATA_MATRIX);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(RESPONSE_VECTOR);
		options.add(1, p2);
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		double[][] A = (double[][]) options.get(0).getSelected();
		double[] y = (double[]) options.get(1).getSelected();
		
		newSampleData(y, A);
		setNoIntercept(false);
		
		RealVector theta = calculateBeta();
		coeffArray = theta.toArray();
		//create estimate matrix from the coefficients and x values
		estimateArray = getX().operate(theta).toArray();
		
		coeffErrorsArray = estimateRegressionParametersStandardErrors();
		residualArray = estimateResiduals();
		
		double residualSumOfSquares = calculateResidualSumOfSquares();
		standardError = Math.sqrt(residualSumOfSquares / residualArray.length);
		
		return null;
	}

	@Override
	public String getName() {
		return "Matrix Regression";
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}
		
	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public String getDefaultViz() {
		return "prerna.ui.components.playsheets.MatrixRegressionVizPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
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

}