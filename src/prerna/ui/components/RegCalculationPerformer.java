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
package prerna.ui.components;


import java.util.ArrayList;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jet.regression.estimators.OLSMultipleLinearRegressionEstimator;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.RegExplorerPlaySheet;
import prerna.ui.components.playsheets.RegressionAnalysisPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class is used to perform regression calculations.
 */
public class RegCalculationPerformer implements IAlgorithm{

	RegressionAnalysisPlaySheet regPlaySheet;
	String nodeUri;
	String dependentVar;
	ArrayList<String> independentVarList;
	static final Logger logger = LogManager.getLogger(RegCalculationPerformer.class.getName());
	double[] yValues;
	double[][] xValues;

	/**
	 * Constructor for RegCalculationPerformer.
	 */
	public RegCalculationPerformer() {
		
	}
	/**
	 * Method regCalculate.
	 * Calculates the regression coefficients for the selected dependent variables
	 * and regressors and then creates a new tab to display on.
	 */
	public void regCalculate() {
		
		runQuery();

		OLSMultipleLinearRegressionEstimator regEst = new OLSMultipleLinearRegressionEstimator();
		regEst.addData(yValues,xValues,null);
		double[] indepVarSlopes = regEst.estimateRegressionParameters();
		ArrayList<Double> indepVarMedianValues = new ArrayList<Double>();
		ArrayList<Double> indepVarSlopeValues = new ArrayList<Double>();
		for(int i=0;i<indepVarSlopes.length;i++)
		{
			indepVarSlopeValues.add(indepVarSlopes[i]);
		}
		for(int i=0;i<xValues[0].length;i++)
		{
			double sum=0.0;
			for(int j=0;j<xValues.length;j++)
			{
				sum+=xValues[j][i];
			}
			indepVarMedianValues.add(sum/xValues.length);
		}		
		RegExplorerPlaySheet newTab = new RegExplorerPlaySheet();
		newTab.setValues(dependentVar,independentVarList,indepVarMedianValues,indepVarSlopeValues);
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		newTab.setJDesktopPane(pane);
		newTab.createView();
		regPlaySheet.jTab.add("Regression Explorer", newTab);
		regPlaySheet.jTab.setSelectedComponent(newTab);
	}
	
	/**
	 * Method runQuery.
	 * Creates and executes the query to pull statistics from the database that are necessary for the regression analysis.
	 */
	public void runQuery()
	{
		String independentVarVariables = "";
		String independentVarDefinitions = "";
		for(int i=0;i<independentVarList.size();i++)
		{
			independentVarVariables+="?indep"+i+" ";
			independentVarDefinitions+="{?node <http://semoss.org/ontologies/Relation/Contains/"+independentVarList.get(i)+"> ?indep"+i+"}";
		}
		String query = "SELECT DISTINCT ?node ?dep " +independentVarVariables+
				"WHERE{"+"{?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+nodeUri+"> ;}"
				+"{?node <http://semoss.org/ontologies/Relation/Contains/"+dependentVar+"> ?dep}"
				+independentVarDefinitions +"}";
		
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		String[] names=null;
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(regPlaySheet.engine, query);
		/*
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(regPlaySheet.engine);
		wrapper.executeQuery();
		*/
		
		names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				try{
				Object[] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++) {
					if(sjss.getVar(names[colIndex]) != null) {
						if(colIndex!=0)
						{
							values[colIndex] = (Double)sjss.getVar(names[colIndex]);
						}
						else
							values[colIndex] = sjss.getVar(names[colIndex]);
					}
				}
				list.add(values);
				}
				catch(RuntimeException ex){
				}
			}
		} 
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		yValues = new double[list.size()];
		xValues = new double[list.size()][list.get(0).length-2];
		for(int i=0;i<list.size();i++)
		{
			Object[] row = list.get(i);
			yValues[i] = (Double)row[1];//Double.parseDouble((String)row[1]);
			for(int j=2;j<row.length;j++)
				xValues[i][j-2] = (Double)row[j];//Double.parseDouble((String)row[j]);
		}
		
	}
	
	/**
	 * Sets the regression analysis playsheet.
	 * @param playSheet IPlaySheet to be cast for regression analysis.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		regPlaySheet=(RegressionAnalysisPlaySheet)playSheet;
	}


	/**
	 * Gets variables.
	// TODO: Return empty object instead of null
	 * @return String[] */
	@Override
	public String[] getVariables() {
		return null;
	}


	/**
	 * Runs the algorithm.
	 */
	@Override
	public void execute() {
		
	}


	/**
	 * Gets algorithm name.
	// TODO Return empty object instead of null
	 * @return String */
	@Override
	public String getAlgoName() {
		return null;
	}

	/**
	 * Sets the node URI.
	 * @param uri Node URI.
	 */
	public void setNodeUri(String uri)
	{
		nodeUri = uri;
	}
	/**
	 * Sets the dependent variable.
	 * @param depVarText 	Name of the dependent variable.
	 */
	public void setDependentVar(String depVarText)
	{
		dependentVar = depVarText;
	}
	/**
	 * Sets the independent variable(s).
	 * @param indepVarModelList 	List of independent variables.
	 */
	public void setIndependentVar(ArrayList<String> indepVarModelList)
	{
		independentVarList = indepVarModelList;
	}
	
}