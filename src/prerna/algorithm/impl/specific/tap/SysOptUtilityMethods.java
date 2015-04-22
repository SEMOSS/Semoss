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
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;
import java.util.Vector;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.helpers.EntityFiller;

public final class SysOptUtilityMethods {

	
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
	public static ArrayList<String> runListQuery(IEngine engine, String query){
		ArrayList<String> list = new ArrayList<String>();
		if(!query.isEmpty()) {
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

			String[] names = wrapper.getVariables();
			while (wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				list.add((String) sjss.getVar(names[0]));
			}
		}
		return list;
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
	

	public static String createPrintString(ArrayList<String> list)
	{
		String printString = "";
		for(String entry : list) {
			printString += entry+", ";
		}
		return printString;
	}
	

	public static String createMissingInfoPrintString(ArrayList<String> sysList,double[] listToCheck)
	{
		String ret = "";
		for(int i=0;i<listToCheck.length;i++)
			if(listToCheck[i]==0)
				ret+=sysList.get(i)+", ";
		return ret;
	}
	
	public static String createNonZeroPrintString(ArrayList<String> sysList,int[] listToCheck)
	{
		String printString = "";
		for(int i=0;i<listToCheck.length;i++)
			if(listToCheck[i]>0)
				printString +=sysList.get(i)+", ";
		return printString;
	}
	
	public static String createMatrixPrintString(int[][] matrix, ArrayList<String> rowList,ArrayList<String> colList) {
		String matrixString = "";
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			for(int j=0;j<matrix[0].length;j++)
			{
				if(matrix[i][j]>0)
					matrixString += "\n" + rowEntry + " "+colList.get(j);
			}
		}
		return matrixString;
	}
	
	public static String createMatrixPrintString(double[][] matrix, ArrayList<String> rowList,ArrayList<String> colList) {
		String matrixString = "";
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			for(int j=0;j<matrix[0].length;j++)
			{
				if(matrix[i][j]>0.0)
					matrixString += "\n" + rowEntry + " "+colList.get(j)+" "+matrix[i][j];
			}
		}
		return matrixString;
	}
	
	public static String createVectorPrintString(double[] matrix, ArrayList<String> rowList) {
		String vectorString = "";
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			if(matrix[i]>0.0)
				vectorString += "\n" + rowEntry + " "+matrix[i];
		}
		return vectorString;
	}
	
	public static String createVectorPrintString(int[] matrix, ArrayList<String> rowList) {
		String vectorString = "";
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			if(matrix[i]>0.0)
				vectorString += "\n" + rowEntry + " "+matrix[i];
		}
		return vectorString;
	}

	public static String createNumberPrintString(ArrayList<String> dataBLUlist,int[][] numbers)
	{
		String ret = "";
		ArrayList<Integer> numList = new ArrayList<Integer>();
		for(int i=0;i<numbers.length;i++)
			numList.add(numbers[i][0]);
		int numSystemsIndex = 0;
		while(!numList.isEmpty())
		{
			int listLoc = numList.indexOf(numSystemsIndex);
			if(listLoc>-1)
			{
				ret += dataBLUlist.get(listLoc)+": "+numSystemsIndex+", ";
				dataBLUlist.remove(listLoc);
				numList.remove(listLoc);
			}
			else
				numSystemsIndex++;
		}
		
		return ret;
	}
	
	public static String createNumberForRegionPristString(ArrayList<String> dataOrBLUList,ArrayList<String> regionList,int[][] numbers)
	{
		String ret = "";
		for(int regionInd=0;regionInd<numbers[0].length;regionInd++)
		{
			ret += "\nRegion "+regionList.get(regionInd)+": ";
			ArrayList<Integer> numList = new ArrayList<Integer>();
			for(int i=0;i<numbers.length;i++)
				numList.add(numbers[i][regionInd]);
			int numSystemsIndex = 0;
			ArrayList<String> dataOrBLUListCopy = SysOptUtilityMethods.deepCopy(dataOrBLUList);
			while(!numList.isEmpty())
			{
				int listLoc = numList.indexOf(numSystemsIndex);
				if(listLoc>-1)
				{
					ret += dataOrBLUListCopy.get(listLoc)+": "+numSystemsIndex+", ";
					dataOrBLUListCopy.remove(listLoc);
					numList.remove(listLoc);
				}
				else
					numSystemsIndex++;
			}
		}
		return ret;
	}

	public static int[][] createEmptyMatrix(int[][] matrix, int row,int col) {
		matrix = new int[row][col];
		for(int x=0;x<row;x++)
			for(int y=0;y<col;y++)
				matrix[x][y] = 0;
		return matrix;
	}
	
	public static double[][] createEmptyMatrix(double[][] matrix, int row,int col) {
		matrix = new double[row][col];
		for(int x=0;x<row;x++)
			for(int y=0;y<col;y++)
				matrix[x][y] = 0;
		return matrix;
	}

	public static double[] createEmptyVector(double[] matrix, int row) {
		matrix = new double[row];
		for(int x=0;x<row;x++)
			matrix[x] = 0;
		return matrix;
	}
	
	public static String[] createEmptyVector(String[] matrix, int row) {
		matrix = new String[row];
		for(int x=0;x<row;x++)
			matrix[x] = "";
		return matrix;
	}
	
	public static int sumRow(int[] row) {
		int sum = 0;
		for (int i = 0; i < row.length; i++)
			sum += row[i];
		return sum;
	}
	
	/**
	 * Gets the list of all capabilities for a selected functional area
	 * @param sparqlQuery 		String containing the query to get all capabilities for a selected functional area
	 * @return capabilities		Vector<String> containing list of all capabilities for a selected functional area
	 */
	public static Vector<String> getList(IEngine engine, String type, String sparqlQuery)
	{
		Vector<String> retList=new Vector<String>();
		try{
			EntityFiller filler = new EntityFiller();
			filler.engineName = engine.getEngineName();
			filler.type = type;
			filler.setExternalQuery(sparqlQuery);
			filler.run();
			Vector names = filler.nameVector;
			for (int i = 0;i<names.size();i++) {
				retList.add((String) names.get(i));
			}
		}catch(NullPointerException e) {
			System.out.println("Concept does not exist in DB");
		}
		return retList;
		
	}
	
}
