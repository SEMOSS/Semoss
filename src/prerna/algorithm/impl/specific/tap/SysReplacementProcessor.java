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

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lpsolve.LpSolveException;
import prerna.util.ArrayUtilityMethods;

/**
 * This processor is used to set up and process the results of the SysReplacementLPSolver
 * Allows for running the optimization multiple times for different consolidated systems
 * Prints results to a list.
 */
public class SysReplacementProcessor {
	
	private static final Logger LOGGER = LogManager.getLogger(SysReplacementProcessor.class.getName());

	private ArrayList<String> dataList, bluList;
	
	//generated data stores based off of the users selections
	//local systems
	private int[][] sustainedLocalSystemDataMatrix, sustainedLocalSystemBLUMatrix, sustainedLocalSystemSiteResultMatrix;
	private int[] sustainedLocalSystemIsTheaterArr, sustainedLocalSystemIsGarrisonArr;
	private double[] sustainedLocalSystemTotalMaintenanceCostArr;
	private int numLocalSustained;
	
	//central systems
	private int[][] sustainedCentralSystemDataMatrix, sustainedCentralSystemBLUMatrix;
	private int[] sustainedCentralSystemIsTheaterArr, sustainedCentralSystemIsGarrisonArr;
	private double[] sustainedCentralSystemMaintenanceCostArr;
	private int numCentralSustained;
	
	private String[] headers;
	private ArrayList<Object[]> sysReplacementList;
	
	private SysReplacementLPSolver lpSolver;

	public SysReplacementProcessor(ArrayList<String> localSysList,ArrayList<String> centralSysList,ArrayList<String> dataList, ArrayList<String> bluList, int[] localSysSustainedArr, int[] centralSysSustainedArr, int[][] localSystemDataMatrix, int[][] localSystemBLUMatrix, int[][] localSystemSiteResultMatrix, int[] localSystemIsTheaterArr, int[] localSystemIsGarrisonArr, double[] localSystemTotalMaintenanceCostArr, int[][] centralSystemDataMatrix, int[][] centralSystemBLUMatrix, int[] centralSystemIsTheaterArr, int[] centralSystemIsGarrisonArr, double[] centralSystemMaintenanceCostArr) {

		sysReplacementList = new ArrayList<Object []>();
		
		numLocalSustained = localSysSustainedArr.length;
		numCentralSustained = centralSysSustainedArr.length;
		
		this.dataList = dataList;
		this.bluList = bluList;

		createHeaders(localSysList, centralSysList, localSysSustainedArr, centralSysSustainedArr);

		//filtering the data stores to only contain sustained systems
		sustainedLocalSystemDataMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(localSystemDataMatrix, localSysSustainedArr);
		sustainedLocalSystemBLUMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(localSystemBLUMatrix, localSysSustainedArr);
		sustainedLocalSystemSiteResultMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(localSystemSiteResultMatrix, localSysSustainedArr);
		
		sustainedLocalSystemIsTheaterArr= ArrayUtilityMethods.filterArrayByIndicies(localSystemIsTheaterArr, localSysSustainedArr);
		sustainedLocalSystemIsGarrisonArr = ArrayUtilityMethods.filterArrayByIndicies(localSystemIsGarrisonArr, localSysSustainedArr);
		sustainedLocalSystemTotalMaintenanceCostArr = ArrayUtilityMethods.filterArrayByIndicies(localSystemTotalMaintenanceCostArr, localSysSustainedArr);

		sustainedCentralSystemDataMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(centralSystemDataMatrix, centralSysSustainedArr);
		sustainedCentralSystemBLUMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(centralSystemBLUMatrix, centralSysSustainedArr);
		
		sustainedCentralSystemIsTheaterArr= ArrayUtilityMethods.filterArrayByIndicies(centralSystemIsTheaterArr, centralSysSustainedArr);
		sustainedCentralSystemIsGarrisonArr = ArrayUtilityMethods.filterArrayByIndicies(centralSystemIsGarrisonArr, centralSysSustainedArr);
		sustainedCentralSystemMaintenanceCostArr = ArrayUtilityMethods.filterArrayByIndicies(centralSystemMaintenanceCostArr, centralSysSustainedArr);
		
		lpSolver = new SysReplacementLPSolver();
		lpSolver.setSustainedSystemVariables(sustainedLocalSystemDataMatrix, sustainedLocalSystemBLUMatrix, sustainedLocalSystemSiteResultMatrix, sustainedLocalSystemIsTheaterArr, sustainedLocalSystemIsGarrisonArr, sustainedLocalSystemTotalMaintenanceCostArr, sustainedCentralSystemDataMatrix, sustainedCentralSystemBLUMatrix, sustainedCentralSystemIsTheaterArr, sustainedCentralSystemIsGarrisonArr, sustainedCentralSystemMaintenanceCostArr);

	}
	
	//same for local or central so long as you pass in a full sys site
	public void optimizeSysReplacement(String system, int[] consolSysDataArr, int[] consolSysBLUArr, int[] consolSysSiteArr, int consolSysIsTheater, int consolSysIsGarrison) {
		try {
			lpSolver.setConsolidatedSystemVariables(consolSysDataArr, consolSysBLUArr, consolSysSiteArr, consolSysIsTheater, consolSysIsGarrison);
			lpSolver.setupModel();
			lpSolver.execute();
			
			sysReplacementList.addAll(processResults(system, consolSysDataArr, consolSysBLUArr, consolSysSiteArr, consolSysIsTheater, consolSysIsGarrison, lpSolver.getLocalSysReplacementArr(), lpSolver.getCentralSysReplacementArr()));

			lpSolver.deleteModel();
				
		} catch(LpSolveException e) {
			lpSolver.deleteModel();
		}


	}
	
	private ArrayList<Object []> processResults(String system, int[] consolSysDataArr, int[] consolSysBLUArr, int[] consolSysSiteArr, int consolSysIsTheater, int consolSysIsGarrison, int[] localSysReplacementIndicies, int[] centralSysReplacementIndicies) {
		
		ArrayList<Object []> resultList = new ArrayList<Object []>();
		
		//in the case where no replacements are found
		//we are likely missing information about the system OR it does not provide value
		if(localSysReplacementIndicies.length == 0 && centralSysReplacementIndicies.length == 0) {
			String response = "No replacement needed.";
			if(SysOptUtilityMethods.sumRow(consolSysDataArr)+SysOptUtilityMethods.sumRow(consolSysBLUArr) == 0) {
				response += " Provides no data or BLU.";
			}
			if(SysOptUtilityMethods.sumRow(consolSysSiteArr) == 0) {
				response += " At no sites";
			}
			
			Object[] row = new Object[3 + numLocalSustained + numCentralSustained];
			row[0] = system;
			row[1] = response;
			resultList.add(row);
			return resultList;
		}
		
		int[] numDuplicateSitesArr = calculateNumDuplicateSites(consolSysSiteArr);

		
		int numData = dataList.size();
		for(int j=0; j<numData; j++) {
			if(consolSysDataArr[j] == 1) {
				Object[] row = new Object[3 + numLocalSustained + numCentralSustained];
				row[0] = system;
				row[1] = dataList.get(j);
				row[2] = "Data";
				for(int k=0; k<localSysReplacementIndicies.length; k++) {
					if(sustainedLocalSystemDataMatrix[localSysReplacementIndicies[k]][j] == 1) {
						row[3 + localSysReplacementIndicies[k]] = numDuplicateSitesArr[localSysReplacementIndicies[k]];
					}
				}
				for(int k=0; k<centralSysReplacementIndicies.length; k++) {
					if(sustainedCentralSystemDataMatrix[centralSysReplacementIndicies[k]][j] == 1) {
						row[3 + numLocalSustained  + centralSysReplacementIndicies[k]] = numDuplicateSitesArr[numLocalSustained  + centralSysReplacementIndicies[k]];
					}
				}
				resultList.add(row);
			}
		}
		int numBLU = bluList.size();
		for(int j=0; j<numBLU; j++) {
			if(consolSysBLUArr[j] == 1) {
				Object[] row = new Object[3 + numLocalSustained + numCentralSustained];
				row[0] = system;
				row[1] = bluList.get(j);
				row[2] = "BLU";
				for(int k=0; k<localSysReplacementIndicies.length; k++) {
					if(sustainedLocalSystemBLUMatrix[localSysReplacementIndicies[k]][j] == 1) {
						row[3 + localSysReplacementIndicies[k]] = numDuplicateSitesArr[localSysReplacementIndicies[k]];
					}
				}
				for(int k=0; k<centralSysReplacementIndicies.length; k++) {
					if(sustainedCentralSystemBLUMatrix[centralSysReplacementIndicies[k]][j] == 1) {
						row[3 + numLocalSustained  + centralSysReplacementIndicies[k]] = numDuplicateSitesArr[numLocalSustained  + centralSysReplacementIndicies[k]];
					}
				}
				resultList.add(row);
			}
		}			
		return resultList;

	}
	
	private int[] calculateNumDuplicateSites(int[] consolSysSiteArr) {
		int[] numDuplicateSitesArr = new int[numLocalSustained + numCentralSustained];
		int numSites = consolSysSiteArr.length;
		for(int i=0; i<numLocalSustained; i++) {
			int numDuplicateSites = 0;
			for(int j=0; j<numSites; j++) {
				if(consolSysSiteArr[j] == 1 && sustainedLocalSystemSiteResultMatrix[i][j] == 1) {
					numDuplicateSites++;
				}
			}
			numDuplicateSitesArr[i] = numDuplicateSites;
		}
		
		for(int i=0; i<numCentralSustained; i++) {
			numDuplicateSitesArr[numLocalSustained + i] = SysOptUtilityMethods.sumRow(consolSysSiteArr);
		}
		
		return numDuplicateSitesArr;
	}
	
	private void createHeaders(ArrayList<String> localSysList,ArrayList<String> centralSysList, int[] localSysSustainedArr, int[] centralSysSustainedArr) {
		int i;
		headers = new String[3 + numLocalSustained + numCentralSustained];
		headers[0] = "Consolidated System";
		headers[1] = "Functionality";
		headers[2] = "Data or BLU";
		for(i=0; i<numLocalSustained; i++) {
			headers[3 + i] = localSysList.get(localSysSustainedArr[i]);
		}
		for(i=0; i<numCentralSustained; i++) {
			headers[3 + numLocalSustained + i] = centralSysList.get(centralSysSustainedArr[i]);
		}

	}
	
	public String[] getHeaders() {
		return headers;
	}

	public ArrayList<Object[]> getSysReplacementList() {
		return sysReplacementList;
	}
	
}
