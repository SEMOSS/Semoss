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

import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class NumericalCorrelationVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(NumericalCorrelationVizPlaySheet.class.getName());		
	private double[][] correlationArray;
	
	/**
	 * Constructor for MatrixRegressionVizPlaySheet.
	 */
	public NumericalCorrelationVizPlaySheet() {
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
		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[][] dataArr = MatrixRegressionHelper.createA(list, numCols);

		//run covariance 
//		Covariance covariance = new Covariance(dataArr);
//		double[][] covarianceArray = covariance.getCovarianceMatrix().getData();
		//run correlation
		//TODO this can be simplified to only get correlation for the params we need
		PearsonsCorrelation correlation = new PearsonsCorrelation(dataArr);
		correlationArray = correlation.getCorrelationMatrix().getData();		
	}
	
	@Override
	public Hashtable processQueryData()
	{
		int i;
		int j;
		int listNumRows = list.size();
		int numCols = names.length;
		int numVariables = numCols - 1;
		
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

		String[] variables = new String[numVariables];
		for(i=0;i<numVariables;i++) {
			variables[i] = names[i+1];
		}

		//add each correlation value to the hash
		Hashtable equations = new Hashtable();
		for(i=0; i<numVariables; i++) {
			String x = variables[i];
			for(j=0; j<numVariables; j++) {
				String y = variables[j];
				if(i!=j) {
					Hashtable objectHash = new Hashtable();
					objectHash.put("x", x);
					objectHash.put("y", y);
//					objectHash.put("covariance", covarianceArray[i][j]);
					objectHash.put("correlation", correlationArray[i][j]);
					equations.put(x+"-"+y,objectHash);
				}
			}
		}
		
		Hashtable dataHash = new Hashtable();
		dataHash.put("one-row",false);
		dataHash.put("id",id);
		dataHash.put("names", variables);
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
			addPanelAsTab(count+". Correlation");
		}
		new CSSApplication(getContentPane());
	}

}
