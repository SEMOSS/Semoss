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
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.util.Constants;

public class SysSiteOptDataStore{
	
	protected static final Logger logger = LogManager.getLogger(SysSiteOptDataStore.class.getName());

	private double deploymentFactor = 5;
	private double interfacePercOfDeployment = 0.075;
	
	public int[][] systemDataMatrix, systemBLUMatrix, systemSiteMatrix;
	public int[] systemTheaterArr, systemGarrisonArr;
	public int[] systemForceModArr, systemForceDecomArr;
	public int[] systemHasUpstreamInterfaceArr;
	
	public double[] systemCentralMaintenanceCostArr, systemSingleSiteMaintenanceCostArr, systemSingleSiteDeploymentCostArr, systemSingleSiteInterfaceCostArr, systemSingleSiteUserTrainingCostArr;
	public double currentSustainmentCost;
	
	public double[] siteLat, siteLon;	
	private String sysListBindings;

	public void setForceModAndDecomArr( int[] systemForceModArr, int[] systemForceDecomArr){
		this.systemForceModArr = systemForceModArr;
		this.systemForceDecomArr = systemForceDecomArr;
	}
	
	public void fillSysSiteOptDataStores(ArrayList<String> sysList, ArrayList<String> allSysList, ArrayList<String> dataList,ArrayList<String> bluList,ArrayList<String> siteList, IDatabaseEngine systemEngine, IDatabaseEngine siteEngine, double centralPercOfBudget, double trainingPerc, Boolean localSystems) {
				
		sysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + "}";

		fillSystemFunctionality(sysList, dataList, bluList, systemEngine);
		fillSystemTheaterGarrison(sysList, systemEngine, true, true);
		fillSystemHasUpstreamInterface(sysList, allSysList, systemEngine);
		fillSystemSite(sysList, siteList, siteEngine);
		if(localSystems) {
			fillLocalSystemCost(sysList, systemEngine, centralPercOfBudget, deploymentFactor, interfacePercOfDeployment, trainingPerc);
		}else{
			fillCentralSystemCost(sysList, systemEngine, centralPercOfBudget, deploymentFactor, interfacePercOfDeployment, trainingPerc, siteList.size());
		}
	}
	
	//TODO move somewhere else
	public void fillSiteLatLon(ArrayList<String> siteList, IDatabaseEngine siteEngine) {
		String siteListBindings = "{" + SysOptUtilityMethods.makeBindingString("DCSite",siteList) + "}";

		String lonQuery = "SELECT DISTINCT ?DCSite ?lon WHERE { {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DCSite <http://semoss.org/ontologies/Relation/Contains/LONG> ?lon}} BINDINGS ?DCSite "+siteListBindings;
		siteLon = createArrayFromQuery(siteEngine,lonQuery,siteList);
		
		String latQuery = "SELECT DISTINCT ?DCSite ?lat WHERE { {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DCSite <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat}} BINDINGS ?DCSite "+siteListBindings;
		siteLat = createArrayFromQuery(siteEngine,latQuery,siteList);
		
	}

	
	private void fillSystemFunctionality(ArrayList<String> sysList,ArrayList<String> dataList,ArrayList<String> bluList,IDatabaseEngine systemEngine) {
		systemDataMatrix = SysOptUtilityMethods.createEmptyIntMatrix(sysList.size(),dataList.size());
		systemBLUMatrix = SysOptUtilityMethods.createEmptyIntMatrix(sysList.size(),bluList.size());

		DHMSMHelper dhelp = new DHMSMHelper();
		dhelp.setUseDHMSMOnly(false);
		dhelp.runData(systemEngine);
		
		for(int sysInd = 0;sysInd < sysList.size();sysInd++)
		{
			String sys = sysList.get(sysInd);
			ArrayList<String> dataObjects = dhelp.getAllDataFromSys(sys, "C");
			systemDataMatrix=fillSysRow(systemDataMatrix, sysInd, dataList, dataObjects);
		}
		
		String query = "SELECT DISTINCT ?System ?blu WHERE{{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?blu}} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemBLUMatrix = SysOptUtilityMethods.fillMatrixFromQuery(systemEngine,query,systemBLUMatrix,sysList,bluList);

	}
	
	private void fillSystemHasUpstreamInterface(ArrayList<String> sysList, ArrayList<String> allSysList, IDatabaseEngine systemEngine) {
		String query = "SELECT DISTINCT ?System (COUNT(?ICD) AS ?NumUpstream) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?UpstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?ICD <http://semoss.org/ontologies/Relation/Consume> ?System} {?UpstreamSystem <http://semoss.org/ontologies/Relation/Provide> ?ICD} } GROUP BY ?System BINDINGS ?UpstreamSystem @SYSTEM-BINDINGS@";

		String allSysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",allSysList) + "}";

		query = query.replace("@SYSTEM-BINDINGS@",allSysListBindings);
		//ArrayList<Object []> SysOptUtilityMethods.runListQuery(systemEngine, query);
		double[] countArr = createArrayFromQuery(systemEngine, query, sysList);
		
		systemHasUpstreamInterfaceArr = new int[countArr.length];
		for(int i=0; i<countArr.length; i++) {
			if(countArr[i] > 0)
				systemHasUpstreamInterfaceArr[i] = 1;
			else
				systemHasUpstreamInterfaceArr[i] = 0;	
		}
	}

	private void fillSystemSite(ArrayList<String> sysList, ArrayList<String> siteList, IDatabaseEngine siteEngine) {
		systemSiteMatrix = SysOptUtilityMethods.createEmptyIntMatrix(sysList.size(),siteList.size());
		String query = "SELECT DISTINCT ?System ?Site WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;}{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?Site;}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite;} } BINDINGS ?System @SYSTEM-BINDINGS@";

		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemSiteMatrix = SysOptUtilityMethods.fillMatrixFromQuery(siteEngine,query,systemSiteMatrix,sysList,siteList);
	}
	
	private void fillLocalSystemCost(ArrayList<String> sysList, IDatabaseEngine systemEngine, double centralPercOfBudget, double deploymentFactor, double interfacePercOfDeployment, double trainingPerc) {

String query = "SELECT DISTINCT ?sys (COALESCE(?cost,0) AS ?Cost) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?sys <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}}BINDINGS ?sys @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		double[] systemSustainmentBudgetArr = createArrayFromQuery(systemEngine,query,sysList);
		
		int i;
		int numSys = sysList.size();
		systemCentralMaintenanceCostArr = new double[numSys];
		systemSingleSiteMaintenanceCostArr = new double[numSys];
		systemSingleSiteDeploymentCostArr = new double[numSys];
		systemSingleSiteInterfaceCostArr = new double[numSys];
		systemSingleSiteUserTrainingCostArr = new double[numSys];
		currentSustainmentCost = 0.0;
		for(i=0; i<sysList.size(); i++) {
			int numSites = SysOptUtilityMethods.sumRow(systemSiteMatrix[i]);

			if(numSites==0) {
				systemCentralMaintenanceCostArr[i] = centralPercOfBudget * systemSustainmentBudgetArr[i];
				systemSingleSiteMaintenanceCostArr[i] = (1 - centralPercOfBudget) * systemSustainmentBudgetArr[i];
				systemSingleSiteDeploymentCostArr[i] = systemSingleSiteMaintenanceCostArr[i] * deploymentFactor;
				systemSingleSiteInterfaceCostArr[i] = systemSingleSiteDeploymentCostArr[i] * interfacePercOfDeployment * (1 + trainingPerc);
				systemSingleSiteUserTrainingCostArr[i] = systemSingleSiteDeploymentCostArr[i] * trainingPerc;
			} else {
				systemCentralMaintenanceCostArr[i] = centralPercOfBudget * systemSustainmentBudgetArr[i];
				systemSingleSiteMaintenanceCostArr[i] = (1 - centralPercOfBudget) * systemSustainmentBudgetArr[i] / numSites;
				systemSingleSiteDeploymentCostArr[i] = systemSingleSiteMaintenanceCostArr[i] * deploymentFactor;
				systemSingleSiteInterfaceCostArr[i] = systemSingleSiteDeploymentCostArr[i] * interfacePercOfDeployment * (1 + trainingPerc);
				systemSingleSiteUserTrainingCostArr[i] = systemSingleSiteDeploymentCostArr[i] * trainingPerc;
			}

			currentSustainmentCost+=systemSustainmentBudgetArr[i];
		}

		
	}
	
	private void fillCentralSystemCost(ArrayList<String> sysList, IDatabaseEngine systemEngine, double centralPercOfBudget, double deploymentFactor, double interfacePercOfDeployment, double trainingPerc, int numSites) {

		String query = "SELECT DISTINCT ?sys (COALESCE(?cost,0) AS ?Cost) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?sys <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}}BINDINGS ?sys @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCentralMaintenanceCostArr = createArrayFromQuery(systemEngine,query,sysList);
		
		int i;
		int numSys = sysList.size();
		systemSingleSiteInterfaceCostArr = new double[numSys];
		systemSingleSiteUserTrainingCostArr = new double[numSys];
		currentSustainmentCost = 0.0;
		
		for(i=0; i<numSys; i++) {//TODO verify this cost?
			systemSingleSiteInterfaceCostArr[i] = (1 - centralPercOfBudget) * systemCentralMaintenanceCostArr[i] / numSites * deploymentFactor * interfacePercOfDeployment * (1 + trainingPerc);
			systemSingleSiteUserTrainingCostArr[i] = (1 - centralPercOfBudget) * systemCentralMaintenanceCostArr[i] / numSites * deploymentFactor * trainingPerc / numSites;
			
			currentSustainmentCost += systemCentralMaintenanceCostArr[i];
		}
	}

	private void fillSystemTheaterGarrison(ArrayList<String> sysList, IDatabaseEngine systemEngine,boolean includeTheater,boolean includeGarrison)
	{
		String query = "SELECT DISTINCT ?System ?GT WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);

		if(includeTheater) {
			systemTheaterArr = new int[sysList.size()];
			for(int i=0;i<sysList.size();i++)
				systemTheaterArr[i] = 0;
		}
		if(includeGarrison) {
			systemGarrisonArr = new int[sysList.size()];
			for(int i=0;i<sysList.size();i++)
				systemGarrisonArr[i] = 0;
		}

		ISelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getSWrapper(systemEngine, query);
			String[] names = wrapper.getVariables();
			// now get the bindings and generate the data
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				String sysName = (String)sjss.getVar(names[0]);
				int rowIndex = sysList.indexOf(sysName);
				if(rowIndex>-1) {
					String theatGarr = (String)sjss.getVar(names[1]);
					theatGarr = theatGarr.toLowerCase();
					//if(systemTheater!=null && (theatGarr.contains("both")||theatGarr.contains("theater")))
					if(includeTheater && (!theatGarr.contains("garrison") || theatGarr.contains("and")))
						systemTheaterArr[rowIndex] = 1;

					//if(systemGarrison!=null && (theatGarr.contains("both")||theatGarr.contains("garrison")))
					if(includeGarrison && (!theatGarr.contains("theater") || theatGarr.contains("and")))
						systemGarrisonArr[rowIndex] = 1;
				}
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
	
	private int[][] fillSysRow(int[][] matrixToFill,int rowInd, ArrayList<String> colList, ArrayList<String> colToPopulate) {
		for(int ind = 0;ind<colToPopulate.size();ind++)
		{
			String colName = colToPopulate.get(ind);
			int matrixColInd = colList.indexOf(colName);
			if(matrixColInd>-1)
				matrixToFill[rowInd][matrixColInd] = 1;
		}
		return matrixToFill;
	}	
	
	private double[] createArrayFromQuery(IDatabaseEngine engine, String query, ArrayList<String> rowNames) {
		double[] arr = new double[rowNames.size()];
		Arrays.fill(arr, 0);

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
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex>-1)
				{
					Object val = sjss.getVar(names[1]);
					if(val instanceof Double)
						arr[rowIndex]= (Double)val;
					if(val instanceof Integer)
						arr[rowIndex]= ((Integer)val) * 1.0;
					else if(val instanceof String) {
						try {
							arr[rowIndex]= Double.parseDouble((String)val);
						}catch(NumberFormatException e){
							logger.info("Could not obtain value for " + rowName + " for value " + names[1]);
						}
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

		return arr;
	}
	
}
