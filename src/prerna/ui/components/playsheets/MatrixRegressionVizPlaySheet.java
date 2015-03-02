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
		runAlgorithm();
		dataHash = processQueryData();
	}
	
	public void runAlgorithm() {

		int numCols = names.length;
		
		//the bIndex should have been provided. if not, will use the last column
		if(bIndex==-1)
			bIndex = numCols - 1;
		
		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[] b = MatrixRegressionHelper.createB(list, bIndex);
		double[][] A = MatrixRegressionHelper.createA(list, bIndex);
		
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
		int i;
		int j;
		int listNumRows = list.size();
		int numCols = names.length;
		
		//for each element/instance
		//add its values for all independent variables to the dataSeriesHash
		Object[][] dataSeries = new Object[listNumRows][numCols];
		for(i=0;i<listNumRows;i++) {
			dataSeries[i][0] = list.get(i)[0];
			for(j=1;j<numCols;j++) {
				dataSeries[i][j] = Ab[i][j-1];
			}
		}
	
		//pull out the id column name
		String id = names[0];
		//reorder names so b is at the end
		names = MatrixRegressionHelper.moveNameToEnd(names, bIndex);
	
		//add each equation to the hash
		Hashtable equations = new Hashtable();
		double constant = coeffArray[0];
		String y = names[numCols - 1];
		
		for(i=1; i<numCols - 1; i++) {
			String x = names[i];
			double xCoeff= coeffArray[i];
			Hashtable objectHash = new Hashtable();
			objectHash.put("x", x);
			objectHash.put("y", y);
			objectHash.put("m",xCoeff);
			objectHash.put("b",constant);
//			objectHash.put("equation", "y = " + xCoeff + "x + " + constant);
			objectHash.put("shift", standardError);
	//		objectHash.put("covariance", covarianceArray[i][numCols - 1]);
			objectHash.put("correlation", correlationArray[i][numCols - 2]);
			equations.put(x+"-"+y,objectHash);
			
		}
		
		Hashtable dataHash = new Hashtable();
		dataHash.put("one-row",true);
		dataHash.put("id",id);
		dataHash.put("names", names);
		dataHash.put("dataSeries", dataSeries);
		dataHash.put("equations", equations);
		
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
}
