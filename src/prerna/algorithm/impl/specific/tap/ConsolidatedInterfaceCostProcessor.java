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
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.engine.api.IDatabase;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.ArrayUtilityMethods;

/**
 * Distributes the cost of building new interfaces to consolidated systems.
 * Used during system site optimization.
 * When sustained system loses an upstream interface because a system was consolidated,
 * the construction cost of new interfaces to replace the lost one is distributed to consolidated system.
 */
public class ConsolidatedInterfaceCostProcessor implements IAlgorithm{
	
	protected static final Logger LOGGER = LogManager.getLogger(ConsolidatedInterfaceCostProcessor.class.getName());

	//Array representing where sustained system is deployed, 1 if at site, 0 if not
	private int[] sustainedSysSiteMatrix;

	//Matricies representing where local systems are currently deployed and will be in future, 1 if at site, 0 if not
	private int[][] currentLocalSysSiteMatrix, futureLocalSysSiteMatrix;
	
	//Matricies representing which systems currently provide which data to the sustained system, 1 if provides data, 0 if does not
	private int[][] localSysDataProviderMatrix, centralSysDataProviderMatrix;
	
	//Array representing the indicies of the central systems that are sustained
	private int[] sustainedCentralSysIndiciesArr;
	
	//Cost allocated to develop interfaces at each site for the sustained system.
	//This will be distributed to the consolidated systems that were at that site and provided data to the sustained system
	private double singleSiteInterfaceCostEstimate;
	
	//total amounts allocated to the sustained system, and ultimately distributed to the consolidated systems.
	private double amountAllocated, amountDistributed;

	//amounts each consolidated system will pay in interface costs. Aggregated across all runs of execute.
	private double[] localSysInterfaceCost, centralSysInterfaceCost;
	
	public ConsolidatedInterfaceCostProcessor(int numLocalSys, int numCentralSys) {
		localSysInterfaceCost = new double[numLocalSys];
		centralSysInterfaceCost = new double[numCentralSys];
	}
	
	/**
	 * Sets the variables needed for processing the cost
	 * @param sysEngine			Engine to pull interface data from
	 * @param sustainedSys		String representing system to be sustained and needing interfaces
	 * @param localSysList		List of local systems to consider
	 * @param centralSysList	List of central systems to consider
	 * @param dataList			List of data to consider
	 * @param sustainedSysSiteMatrix	 	Array of integers representing which sites the sustained system (system of interest) is deployed at
	 * @param currentLocalSysSiteMatrix		Matrix of integers representing which local systems are deployed at which sites currently
	 * @param futureLocalSysSiteMatrix		Matrix of integers representing which local systems are deployed at which sites in the future
	 * @param centralSysKeptIndexArr		Array representing the indicies of the central systems that are sustained	
	 * @param singleSiteInterfaceCostEstimate	Estimated interface cost for a single site
	 */
	public void setVariables(IDatabase sysEngine, String sustainedSys, ArrayList<String> localSysList, ArrayList<String> centralSysList, ArrayList<String> dataList, int[] sustainedSysSiteMatrix, int[][] currentLocalSysSiteMatrix, int[][] futureLocalSysSiteMatrix, int[] sustainedCentralSysIndiciesArr, double singleSiteInterfaceCostEstimate){
		
		this.sustainedSysSiteMatrix = sustainedSysSiteMatrix;
		this.currentLocalSysSiteMatrix = currentLocalSysSiteMatrix;
		this.futureLocalSysSiteMatrix = futureLocalSysSiteMatrix;
		this.sustainedCentralSysIndiciesArr = sustainedCentralSysIndiciesArr;
		this.singleSiteInterfaceCostEstimate = singleSiteInterfaceCostEstimate;
		
		localSysDataProviderMatrix = SysOptUtilityMethods.createEmptyIntMatrix(localSysList.size(),dataList.size());
		centralSysDataProviderMatrix = SysOptUtilityMethods.createEmptyIntMatrix(centralSysList.size(),dataList.size());
		
		String query = "SELECT DISTINCT ?UpstreamSystem ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/System/" + sustainedSys + "> AS ?System) {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?UpstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ICD <http://semoss.org/ontologies/Relation/Consume> ?System} {?UpstreamSystem <http://semoss.org/ontologies/Relation/Provide> ?ICD} {?ICD <http://semoss.org/ontologies/Relation/Payload> ?Data}} BINDINGS ?UpstreamSystem @SYSTEM-BINDINGS@";
		
		String localSysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",localSysList) + "}";
		String localSysQuery = query.replace("@SYSTEM-BINDINGS@",localSysListBindings);
		localSysDataProviderMatrix = SysOptUtilityMethods.fillMatrixFromQuery(sysEngine,localSysQuery,localSysDataProviderMatrix,localSysList,dataList);

		String centralSysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",centralSysList) + "}";
		String centralSysQuery = query.replace("@SYSTEM-BINDINGS@",centralSysListBindings);
		centralSysDataProviderMatrix = SysOptUtilityMethods.fillMatrixFromQuery(sysEngine,centralSysQuery,centralSysDataProviderMatrix,centralSysList,dataList);

	}

	/**
	 * Executes the algorithm.
	 * Calculates which data at each site has lost at least one upstsream interface.
	 * Iterates through the sites the sustained system is deployed at.
	 * For each missing data and each system that used to provide that data
	 * and no longer does, allocates a portion of the cost to that system.
	 */
	@Override
	public void execute(){
		
		if(localSysInterfaceCost == null)
			localSysInterfaceCost = new double[localSysDataProviderMatrix.length];
		if(centralSysInterfaceCost == null)
			centralSysInterfaceCost = new double[centralSysDataProviderMatrix.length];

		int[][] lostUpstreamForDataAtSiteMatrix = generateLostUpstreamForDataAtSiteMatrix();
		
		int i;
		int j;
		int k;
		int numData = localSysDataProviderMatrix[0].length;
		int numSites = sustainedSysSiteMatrix.length;
		
		int numCentralSys = centralSysDataProviderMatrix.length;
		int numLocalSys = localSysDataProviderMatrix.length;

		amountAllocated = SysOptUtilityMethods.sumRow(sustainedSysSiteMatrix) * singleSiteInterfaceCostEstimate;
		amountDistributed = 0.0;
		
		NEWSITE: for(i = 0; i < numSites; i++) {
			
			//if system isn't at this site, on to the next site
			if(sustainedSysSiteMatrix[i] == 0) {
				continue NEWSITE;
			}
			
			//cost for this site is known
			//cost for this site is divided across all missing data			
			//calculate number of missing data at this site
			int numMissingData = 0;
			for(j = 0; j < numData; j++) {
				numMissingData += lostUpstreamForDataAtSiteMatrix[j][i];
			}
			
			//if no data is missing at this site, on to the next site
			if(numMissingData == 0) {
//				numSitesWithAllData++;
				continue NEWSITE;
			}
			
			//cost per data object at site is now known: cost / numMissingData
			//now need to divide across each system that provides the missing data objects
			NEXTDATA: for(j = 0; j < numData; j++) {
				
				//if no interfaces were lost for this data object... continue to next data object
				if(lostUpstreamForDataAtSiteMatrix[j][i] == 0) {
					continue NEXTDATA;
				}
				
				//count the total number of systems that used to provide this data, at this site, and no longer do
				int numSystems = 0;
				
				for(k = 0; k < numLocalSys; k++) {
					if(localSysDataProviderMatrix[k][j] == 1 && currentLocalSysSiteMatrix[k][i] == 1 && futureLocalSysSiteMatrix[k][i] == 0) {
						numSystems++;
					}
				}
				
				for(k = 0; k < numCentralSys; k++) {
					if(!ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, k) && centralSysDataProviderMatrix[k][j] == 1)
						numSystems++;
				}
				
				//divide the interface cost allocated for this site across all data objects and then across all systems that used to provide
				double costPerSiteDataSystem = singleSiteInterfaceCostEstimate / numMissingData / numSystems;
				
				for(k = 0; k < numLocalSys; k++) {
					if(localSysDataProviderMatrix[k][j] == 1 && currentLocalSysSiteMatrix[k][i] == 1 && futureLocalSysSiteMatrix[k][i] == 0) {
						localSysInterfaceCost[k] += costPerSiteDataSystem;
					}
				}
				
				for(k = 0; k < numCentralSys; k++) {
					if(!ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, k) && centralSysDataProviderMatrix[k][j] == 1)
						centralSysInterfaceCost[k] += costPerSiteDataSystem;
				}
				
				amountDistributed += singleSiteInterfaceCostEstimate / numMissingData;
				
			}
		}

	}
	
	/**
	 * Generates a matrix that depicts what data objects have lost interfaces at each site.
	 * @return Matrix of data and sites with a value of 1 if the data used to have 
	 * at least 1 interface at that site and does not as a result of the optimization problem
	 */
	public int[][] generateLostUpstreamForDataAtSiteMatrix() {
		//for each data object
		//	for each central system that was decommissioned.
		//		if decommissioned central system provided data object, then value is 1 for all sites, on to next data
		// 	if no central systems provide, then check the local systems
		//for each local system at each site
		//	if the local system provides data, and was there but now is not, then lost is 1
		
		int i;
		int j;
		int k;
		int numData = localSysDataProviderMatrix[0].length;
		int numSites = sustainedSysSiteMatrix.length;
		
		int numCentralSys = centralSysDataProviderMatrix.length;
		int numLocalSys = localSysDataProviderMatrix.length;

		int[][] lostUpstreamForDataAtSiteMatrix = new int[localSysDataProviderMatrix[0].length][sustainedSysSiteMatrix.length];
		
		OUTER: for(i = 0; i < numData; i++) {
			
			for(j = 0; j < numCentralSys; j++) {
				if(!ArrayUtilityMethods.arrayContainsValue(sustainedCentralSysIndiciesArr, j) && centralSysDataProviderMatrix[j][i] == 1) {
					for(k = 0; k < numSites; k++) {
						Arrays.fill(lostUpstreamForDataAtSiteMatrix[i], 1);			
					}
					continue OUTER;
				}
			}

			//if no central systems were found, set it all to 0 so no null pointers
			Arrays.fill(lostUpstreamForDataAtSiteMatrix[i], 0);
			
			for(j = 0; j < numLocalSys; j++) {
				for(k = 0; k < numSites; k++) {
					if(localSysDataProviderMatrix[j][i] == 1 && currentLocalSysSiteMatrix[j][k] == 1 && futureLocalSysSiteMatrix[j][k] == 0 ) {
						lostUpstreamForDataAtSiteMatrix[i][k] = 1;
					}
				}
			}
			
		}
		
		return lostUpstreamForDataAtSiteMatrix;
	}
	
	public double[] getLocalSysInterfaceCost() {
		return localSysInterfaceCost;
	}

	public double[] getCentralSysInterfaceCost() {
		return centralSysInterfaceCost;
	}
		
	public double getAmountAllocated() {
		return amountAllocated;
	}

	public double getAmountDistributed() {
		return amountDistributed;
	}
	/**
	 * Sets the passed playsheet.
	 * @param 	playSheet	Playsheet to be cast.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		//TODO
		
	}

	/**
	 * Gets variable names.
	 * @return String[] */
	@Override
	public String[] getVariables() {
		//TODO
		return null;
	}


	/**
	 * Gets the name of the algorithm.
	 * @return 	Algorithm name. */
	@Override
	public String getAlgoName() {
		//TODO
		return null;
	}
	
	
}
