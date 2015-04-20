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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.Hashtable;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class MatrixRegressionVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionVizPlaySheet.class.getName());	
	private int bIndex = -1;
	private double[][] Ab;
	
	private double standardError;
	private double[] coeffArray;
	private double[][] correlationArray;

	private boolean includesInstance = true;
	

	/**
	 * Constructor for MatrixRegressionVizPlaySheet.
	 */
	public MatrixRegressionVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatter-plot-matrix.html";
	}
	
	@Override
	public void createData() {
		if(list==null)
			super.createData();
		else
			dataHash = processQueryData();
	}
	
	public void runAlgorithm() {

		//the bIndex should have been provided. if not, will use the last column
		if(bIndex==-1)
			bIndex = names.length - 1;
		
		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[] b = MatrixRegressionHelper.createB(list, bIndex);
		double[][] A;
		if(includesInstance)
			A = MatrixRegressionHelper.createA(list, 1, bIndex);
		else
			A = MatrixRegressionHelper.createA(list, 0, bIndex);
			
		
		//run regression so we have coefficients and error
		MatrixRegressionAlgorithm alg = new MatrixRegressionAlgorithm(A, b);
		alg.execute();
		coeffArray = alg.getCoeffArray();
		standardError = alg.getStandardError();
		
		//create Ab array
		Ab = MatrixRegressionHelper.appendB(A, b);

		//run covariance 
//		Covariance covariance = new Covariance(Ab);
//		double[][] covarianceArray = covariance.getCovarianceMatrix().getData();
		//run correlation
		//TODO this can be simplified to only get correlation for the params we need
		PearsonsCorrelation correlation = new PearsonsCorrelation(Ab);
		correlationArray = correlation.getCorrelationMatrix().getData();		
	}
	
	@Override
	public Hashtable processQueryData()
	{	
		runAlgorithm();
		int i;
		int j;
		int listNumRows = list.size();
//		int numCols = names.length;
		int numVariables;
			
		if(includesInstance) {
			numVariables = names.length - 1;
		}else {
			numVariables = names.length;
			String[] newNames = new String[numVariables + 1];
			newNames[0] = "";
			for(i = 0; i< numVariables; i++)
				newNames[i + 1] = names[i];
			names = newNames;
		}
		
		String id = names[0];

		//reorder names so b is at the end
//		names = MatrixRegressionHelper.moveNameToEnd(names, bIndex);
		
		//for each element/instance
		//add its values for all independent variables to the dataSeriesHash
		Object[][] dataSeries = new Object[listNumRows][numVariables + 1];
		for(i=0;i<listNumRows;i++) {
			if(includesInstance)
				dataSeries[i][0] = list.get(i)[0];
			else
				dataSeries[i][0] = "";
			
			for(j=0;j<numVariables;j++) {
				dataSeries[i][j + 1] = Ab[i][j];
			}
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

		dataHash = new Hashtable();
		dataHash.put("one-row",true);
		dataHash.put("id",id);
		dataHash.put("names", names);
		dataHash.put("dataSeries", dataSeries);
		dataHash.put("shifts", stdErrors);
		dataHash.put("coefficients", coefficients);
		dataHash.put("correlations", correlations);
		
//		Gson gson = new Gson();
//		System.out.println(gson.toJson(dataHash));
		
		return dataHash;
	}

	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel()
	{
		//if this is to be a separate playsheet, create the tab in a new window
		//otherwise, if this is to be just a new tab in an existing playsheet,
		if(jTab==null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Regression");
		}
		new CSSApplication(getContentPane());
	}

	public void setbColumnIndex(int bColumnIndex) {
		this.bIndex = bColumnIndex;
	}
	
	public int getbColumnIndex() {
		return bIndex;
	}
	
	public void setIncludesInstance(boolean includesInstance) {
		this.includesInstance = includesInstance;
	}

}
