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
package prerna.algorithm.learning.supervized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;
import prerna.ui.components.playsheets.MatrixRegressionHelper;

public class MatrixRegressionAlgorithm extends OLSMultipleLinearRegression implements IAnalyticActionRoutine {

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionAlgorithm.class.getName());

	public static final String INCLUDE_INSTANCES = "includeInstances";
	public static final String SKIP_ATTRIBUTES = "skipAttributes";
	public static final String B_INDEX = "bIndex";
	
	private ITableDataFrame dataFrame;
	private boolean includesInstance;
	private int bIndex;
	
	private ArrayList<SEMOSSParam> options;
	private double[] coeffArray;
	private double[] coeffErrorsArray;
	private double[] residualArray;
	private double[] estimateArray;
	private double standardError;
	private double[][] A;
	private double[] b;
	
	
	/**
	 * Creates a MatrixRegressionAlgorithm object to solve A*theta = y.
	 */
	public MatrixRegressionAlgorithm() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INCLUDE_INSTANCES);
		options.add(0, p1);
		
		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(B_INDEX);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(SKIP_ATTRIBUTES);
		options.add(2, p3);
	}

	@Override
	public void runAlgorithm(ITableDataFrame... data) {
		this.dataFrame = data[0];
		this.includesInstance = (boolean) options.get(0).getSelected();
		int bIndex = ((Number) options.get(1).getSelected()).intValue();
		List<String> skipAttribues = (List<String>) options.get(2).getSelected();
		dataFrame.setColumnsToSkip(skipAttribues);

		if(bIndex==-1) {
			bIndex = dataFrame.getColumnHeaders().length - 1;
		}
		//create the b and A arrays which are used in matrix regression to determine coefficients
		b = MatrixRegressionHelper.createB(dataFrame, bIndex);
		if(includesInstance) {
			A = MatrixRegressionHelper.createA(dataFrame, 1, bIndex);
		} else {
			A = MatrixRegressionHelper.createA(dataFrame, 0, bIndex);
		}
		
		newSampleData(b, A);
		setNoIntercept(false);
		
		RealVector theta = calculateBeta();
		coeffArray = theta.toArray();
		//create estimate matrix from the coefficients and x values
		estimateArray = getX().operate(theta).toArray();
		
		coeffErrorsArray = estimateRegressionParametersStandardErrors();
		residualArray = estimateResiduals();
		
		double residualSumOfSquares = calculateResidualSumOfSquares();
		standardError = Math.sqrt(residualSumOfSquares / residualArray.length);
	}
	
	@Override
	public Object getAlgorithmOutput() {
		//create Ab array
		double[][] Ab = MatrixRegressionHelper.appendB(A, b);
		PearsonsCorrelation correlation = new PearsonsCorrelation(Ab);
		double[][] correlationArray = correlation.getCorrelationMatrix().getData();		
				
		int numVariables;
		String id = "";
		int offset = 0;
		String[] columnHeaders = dataFrame.getColumnHeaders();
		if(includesInstance) {
			numVariables = columnHeaders.length - 1;
			id = columnHeaders[0];
			offset = 1;
		}else {
			numVariables = columnHeaders.length;
		}

		int i = 0;
		List<String> names = new ArrayList<String>();
		for(i = 0 + offset; i < columnHeaders.length; i++) {
			if(i != bIndex) {
				names.add(columnHeaders[i]);
			}
		}
		names.add(0, columnHeaders[bIndex]);
		Collections.reverse(names);
		if(includesInstance) {
			names.add(0, id);
		}
		
		Object[] stdErrors = new Object[numVariables];
		for(i = 0; i<numVariables ; i++) {
			stdErrors[i] = standardError;
		}

		Object[] coefficients = new Object[numVariables + 1];
		for(i = 0; i< numVariables - 1; i++) {
			coefficients[i + 1] = coeffArray[i+1];
		}
		coefficients[numVariables] = coeffArray[0];

		Object[] correlations = new Object[numVariables + 1];
		for(i = 0; i<numVariables-1; i++) {
			correlations[i+1] = correlationArray[i][numVariables - 1];
		}
		
		List<Object> corrList = Arrays.asList(correlations);
		Collections.reverse(corrList);

		List<Object> coeffList = Arrays.asList(coefficients);
		Collections.reverse(coeffList);

		Map<String, String> dataTableAlign = new HashMap<String, String>();
		for(i = 0; i < names.size(); i++) {
			dataTableAlign.put("dim " + i, names.get(i));
		}
			
		Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
		dataHash.put("one-row",true);
		dataHash.put("id",id);
		dataHash.put("shifts", stdErrors);
		dataHash.put("coefficients", coeffList);
		dataHash.put("correlations", corrList);
		
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("specificData", dataHash);
		allHash.put("data", this.dataFrame.getData());
		allHash.put("headers", names);
		allHash.put("layout", getDefaultViz());
		allHash.put("dataTableAlign", dataTableAlign);
		
		return allHash;
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
		return "ScatterplotMatrix";
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

	public double[] getB() {
		return b;
	}
	
	public double[][] getA() {
		return A;
	}
}