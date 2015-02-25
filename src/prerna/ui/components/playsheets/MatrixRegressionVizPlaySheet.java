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

import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;

public class MatrixRegressionVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionVizPlaySheet.class.getName());	
	private int bIndex = -1;
	private Boolean addAsTab = false;//determines whether to add this playsheet as a tab to the jTab or to create a new playsheet
	
	
	/**
	 * Constructor for MatrixRegressionVizPlaySheet.
	 */
	public MatrixRegressionVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatter-plot-matrix.html";//TODO change to new name
	}
	
	@Override
	public void createData() {
		if(list==null)
			super.createData();
		else
			dataHash = processQueryData();
	}
	
	@Override
	public Hashtable processQueryData()
	{
		int i;
		int j;
		int listNumRows = list.size();
		int numCols = names.length;
		int numVariables = numCols - 1;
		
		//the bIndex should have been provided. if not, will use the last column
		if(bIndex==-1)
			bIndex = numCols - 1;
		
		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[] b = MatrixRegressionHelper.createB(list, bIndex);
		double[][] A = MatrixRegressionHelper.createA(list, bIndex);
		
		//run regression so we have coefficients and error
		MatrixRegressionAlgorithm alg = new MatrixRegressionAlgorithm(A, b);
		alg.execute();
		double[] coeffArray = alg.getCoeffArray();
		double standardError = alg.getStandardError();
		
		//create Ab array
		double[][] Ab = MatrixRegressionHelper.appendB(A, b);

		//run covariance 
//		Covariance covariance = new Covariance(Ab);
//		double[][] covarianceArray = covariance.getCovarianceMatrix().getData();
		//run correlation
		PearsonsCorrelation correlation = new PearsonsCorrelation(Ab);
		double[][] correlationArray = correlation.getCorrelationMatrix().getData();		
		
		//for each element/instance
		//add its values for all independent variables to the dataSeriesHash
		Hashtable dataSeries = new Hashtable();
		for(i=0;i<listNumRows;i++) {
			Object[] row = list.get(i);
			Hashtable objectHash = new Hashtable();
			for(j=0;j<numCols;j++) {
				objectHash.put(names[j], row[j]);
			}
			int groupID = i % 6;
			objectHash.put("group", groupID);//TODO make a groupID
			dataSeries.put(row[0], objectHash);
		}
				
		//pull out the id column name
		String id = names[0];
		//reorder names so b is at the end
		names = MatrixRegressionHelper.moveNameToEnd(names, bIndex);

		String[] variables = new String[numVariables];
		for(i=0;i<numVariables;i++) {
			variables[i] = names[i+1];
		}
		
		//add each equation to the hash
		//the coefficient is -1 if it is our special variable
		Hashtable equations = new Hashtable();
		double constant = coeffArray[0];
		for(i=0; i<numVariables; i++) {
			String x = variables[i];
			double xCoeff;
			if(i==numVariables - 1) {
				xCoeff = -1.0;
			}else
				xCoeff= coeffArray[i+1];
			for(j=0; j<numVariables; j++) {
				String y = variables[j];
				double yCoeff;
				if(j==numVariables - 1) {
					yCoeff = -1.0;
				}else {
					yCoeff = coeffArray[j+1];
				}
				if(i!=j) {
					Hashtable objectHash = new Hashtable();
					objectHash.put("x", x);
					objectHash.put("y", y);
					objectHash.put("equation", "y = " + (-1*xCoeff / yCoeff) + "x + " + (-1*constant/yCoeff) );
					objectHash.put("shift", standardError);
//					objectHash.put("covariance", covarianceArray[i][j]);
					objectHash.put("correlation", correlationArray[i][j]);

					equations.put(x+"-"+y,objectHash);
				}
			}
			
		}
		
		Hashtable dataHash = new Hashtable();
		dataHash.put("names", variables);
		dataHash.put("id",id);
		dataHash.put("equations", equations);
		dataHash.put("dataSeries", dataSeries);
		
		Gson gson = new Gson();
		System.out.println(gson.toJson(dataHash));
		
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
		if(!addAsTab) {
			super.addPanel();
		}else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Regression");
		}
		new CSSApplication(getContentPane());
	}
	
	public void setAddAsTab(Boolean addAsTab) {
		this.addAsTab = addAsTab;
	}
	
	public void setbColumnIndex(int bColumnIndex) {
		this.bIndex = bColumnIndex;
	}
}
