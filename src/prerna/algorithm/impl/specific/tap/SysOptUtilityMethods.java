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
package prerna.algorithm.impl.specific.tap;

import java.io.IOException;
import java.util.ArrayList;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;
import prerna.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class SysOptUtilityMethods {

	
	protected static final Logger logger = LogManager.getLogger(SysOptUtilityMethods.class);

	public static String makeBindingString(String type,ArrayList<String> vals) {
		String bindings = "";
		for(String val : vals)
		{
			bindings+= "(<http://health.mil/ontologies/Concept/"+type+"/"+val+">)";
		}
		return bindings;
	}
	
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public static Object runSingleResultQuery(IDatabaseEngine engine, String query){
		if(!query.isEmpty()) {
			ISelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
				String[] names = wrapper.getVariables();
				while (wrapper.hasNext()) {
					ISelectStatement sjss = wrapper.next();
					return sjss.getVar(names[0]);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		return null;
	}
	
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public static ArrayList<String> runListQuery(IDatabaseEngine engine, String query){
		ArrayList<String> list = new ArrayList<String>();
		if(!query.isEmpty()) {
			ISelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
				String[] names = wrapper.getVariables();
				while (wrapper.hasNext()) {
					ISelectStatement sjss = wrapper.next();
					list.add((String) sjss.getVar(names[0]));
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		return list;
	}
	
//	/** TODO remove
//	 * Gets the list of all capabilities for a selected functional area
//	 * @param sparqlQuery 		String containing the query to get all capabilities for a selected functional area
//	 * @return capabilities		Vector<String> containing list of all capabilities for a selected functional area
//	 */
//	public static Vector<String> getList(IDatabase engine, String type, String sparqlQuery)
//	{
//		Vector<String> retList=new Vector<String>();
//		try{
//			EntityFiller filler = new EntityFiller();
//			filler.engineName = engine.getEngineName();
//			filler.type = type;
//			filler.setExternalQuery(sparqlQuery);
//			filler.run();
//			Vector names = filler.nameVector;
//			for (int i = 0;i<names.size();i++) {
//				retList.add((String) names.get(i));
//			}
//		}catch(NullPointerException e) {
//			System.out.println("Concept does not exist in DB");
//		}
//		return retList;
//		
//	}
	
	/**
	 * Runs a query on a specific engine.
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public static ArrayList<Object []> runQuery (IDatabaseEngine engine, String query){
		ArrayList<Object []> list = new ArrayList<Object []>();
		ISelectWrapper wrapper = Utility.processQuery(engine, query);

		String[] names = wrapper.getVariables();
		int numVals = names.length;
		while (wrapper.hasNext()) {
			ISelectStatement statement = wrapper.next();
			Object[] row = new Object[numVals];
			int i = 0;
			for(i = 0; i<numVals; i++) {
				row[i] = statement.getVar(names[i]);
			}
			list.add(row);
		}
		return list;
	}

	
	public static int[][] fillMatrixFromQuery(IDatabaseEngine engine, String query,int[][] matrix,ArrayList<String> rowNames,ArrayList<String> colNames) {
		ISelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			// get the bindings from it
			String[] names = wrapper.getVariables();
			// now get the bindings and generate the data
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				Object rowName = sjss.getVar(names[0]);
				Object colName = sjss.getVar(names[1]);

				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex>-1)
				{
					int colIndex = colNames.indexOf(colName);
					if(colIndex>-1)
					{
						matrix[rowIndex][colIndex] = 1;
					}
				}

			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return matrix;
	}

	
	public static ArrayList<String> inBothLists(ArrayList<String> list1,ArrayList<String> list2) {
		ArrayList<String> duplicates = new ArrayList<String>();
		
		int size1 = list1.size();
		for(int i=0; i<size1; i++) {
			if(list2.contains(list1.get(i)))
				duplicates.add(list1.get(i));
		}
		
		return duplicates;
	}
	
	public static ArrayList<String> removeDuplicates(ArrayList<String> list) {
		ArrayList<String> retList = new ArrayList<String>();
		for (String entry : list) {
			if (!retList.contains(entry))
				retList.add(entry);
		}
		return retList;
	}
	
	public static ArrayList<String> deepCopy(ArrayList<String> list)
	{
		ArrayList<String> retList = new ArrayList<String>();
		for(String element : list)
		{
			retList.add(element);
		}
		return retList;
	}
	

	public static String convertToString(ArrayList<String> list)
	{
		String printString = "";
		for(String entry : list) {
			printString += entry+", ";
		}
		return printString;
	}
	

	public static String convertToStringIfZero(ArrayList<String> sysList,double[] listToCheck)
	{
		String ret = "";
		for(int i=0;i<listToCheck.length;i++)
			if(listToCheck[i]==0)
				ret+=sysList.get(i)+", ";
		return ret;
	}
	
	public static ArrayList<String> convertToStringIfNonZero(ArrayList<String> sysList,int[] listToCheck)
	{
		ArrayList<String> nonZeroList = new ArrayList<String>();
		
		for(int i=0;i<listToCheck.length;i++)
			if(listToCheck[i]>0)
				nonZeroList.add(sysList.get(i));
		return nonZeroList;
	}	

	public static int[][] createEmptyIntMatrix(int row,int col) {
		int[][] matrix = new int[row][col];
		for(int x=0;x<row;x++)
			for(int y=0;y<col;y++)
				matrix[x][y] = 0;
		return matrix;
	}
	
	public static double[][] createEmptyDoubleMatrix(int row,int col) {
		double[][] matrix = new double[row][col];
		for(int x=0;x<row;x++)
			for(int y=0;y<col;y++)
				matrix[x][y] = 0;
		return matrix;
	}
	
	public static int sumRow(int[] row) {
		int sum = 0;
		for (int i = 0; i < row.length; i++)
			sum += row[i];
		return sum;
	}
	
	public static double sumRow(double[] row) {
		int sum = 0;
		for (int i = 0; i < row.length; i++)
			sum += row[i];
		return sum;
	}
		
	public static double calculateAdjustedTotalSavings(double mu, double yearsToComplete, double totalYrs, double savingsForYear) {

		double i;
		double adjustedTotalSavings = 0.0; 

		if(mu != 1) {
			for(i= Math.ceil(yearsToComplete); i<totalYrs; i++)
				adjustedTotalSavings += savingsForYear *  Math.pow(mu,i);
		} else {
			for(i= Math.ceil(yearsToComplete); i<totalYrs; i++)
				adjustedTotalSavings += savingsForYear;
		}
		
		return adjustedTotalSavings;
	}
	
	public static double calculateAdjustedDeploymentCost(double mu, double yearsToComplete, double budgetForYear) {

		double i;
		double adjustedDeploymentCost = 0.0; 
		
		if(mu != 1) {
			for(i=1 ; i<= yearsToComplete; i++) {
				adjustedDeploymentCost+= budgetForYear * Math.pow(mu,i - 1);
			}
			adjustedDeploymentCost += (yearsToComplete - Math.floor(yearsToComplete)) * budgetForYear * Math.pow(mu,i - 1);
			
		} else {
			
			for(i=1 ; i<= yearsToComplete; i++)
				adjustedDeploymentCost+= budgetForYear;
				
			adjustedDeploymentCost += (yearsToComplete - Math.floor(yearsToComplete)) * budgetForYear;
		}
		return adjustedDeploymentCost;
	}
	
	public static double[] calculateAdjustedSavingsArr(double mu, double yearsToComplete, int totalYrs, double savingsForYear) {
			
		int i;
		double[] adjustedSavingsArr = new double[totalYrs];
		if(mu != 1) {
			for(i=0; i<totalYrs; i++) {
				if(i < yearsToComplete) {
					adjustedSavingsArr[i] = 0;
					
				}else {
					adjustedSavingsArr[i] = savingsForYear *  Math.pow(mu,i);
				}
			}
		} else {
			for(i=0; i<totalYrs; i++) {
				if(i<yearsToComplete) {
					adjustedSavingsArr[i] = 0;
					
				}else {
					adjustedSavingsArr[i] = savingsForYear;
				}
			}
		}
		return adjustedSavingsArr;
	}
	
	public static double[] calculateAdjustedDeploymentCostArr(double mu, double yearsToComplete, boolean roundYearsUp, int totalYrs, double budgetForYear){
		
		int i;
		double[] adjustedDeploymentCostArr = new double[totalYrs];
		
		if(mu != 1) {
			for(i=0; i<totalYrs; i++) {
				if(i+1 < yearsToComplete) {
					adjustedDeploymentCostArr[i] = budgetForYear * Math.pow(mu,i);
					
				}else if(i<yearsToComplete) {
					if(roundYearsUp) {
						adjustedDeploymentCostArr[i] = (Math.ceil(yearsToComplete) - Math.floor(yearsToComplete)) *  budgetForYear *  Math.pow(mu,i);
					} else {
						adjustedDeploymentCostArr[i] = (yearsToComplete - Math.floor(yearsToComplete)) *  budgetForYear *  Math.pow(mu,i);
					}
				}else {
					adjustedDeploymentCostArr[i] = 0;
				}
			}
		} else {
			for(i=0; i<totalYrs; i++) {
				if(i+1 < yearsToComplete) {
					adjustedDeploymentCostArr[i] = budgetForYear;
					
				}else if(i<yearsToComplete) {
					adjustedDeploymentCostArr[i] = (yearsToComplete - Math.floor(yearsToComplete)) *  budgetForYear;
					
				}else {
					adjustedDeploymentCostArr[i] = 0;
				}
			}
		}
		return adjustedDeploymentCostArr;

	}
	
	public static double[] calculateCummulativeArr(double[] array) {
		double cummulative = 0.0;
		int length = array.length;

		double[] retArray = new double[length];
		for(int i = 0; i<length; i++) {
			cummulative += array[i];
			retArray[i] = cummulative;
		}
		return retArray;
	}
	
}
