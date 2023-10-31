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

public class ResidualSystemOptFillData{
	
	protected static final Logger logger = LogManager.getLogger(ResidualSystemOptFillData.class.getName());
	public ArrayList<String> sysList, dataList, bluList;
	public ArrayList<String> regionList;
	
	private ArrayList<String> systemMustModernize, systemMustDecommission;
	
	private boolean includeRegionalization = false;
	
	//a_ip, b_iq, c_ip
	public int[][] systemDataMatrix, systemBLUMatrix, systemRegionMatrix;
	
	public double[][] systemCostOfDataMatrix;
	public double[] systemCostOfMaintenance, systemCostOfDB;

	public double[] systemNumOfSites;
	public String[] systemDisposition, systemCapability, systemCapabilityGroup, systemCONOPS, systemMHSSpecific;
	public double[] systemModernize, systemDecommission;
	
	//Ap, Bq
	public int[][] dataRegionSORSystemCount, bluRegionProviderCount;
	public int[][] dataRegionSORSystemCountReduced, bluRegionProviderCountReduced;
	private ArrayList<Integer> dataReducedIndex;
	private ArrayList<Integer> bluReducedIndex;
	
	//if a theater/garrison break down

	public int[] systemTheater, systemGarrison;
	
	private int[][] dataRegionSORSystemTheaterCount, dataRegionSORSystemGarrisonCount;
	private int[][] bluRegionProviderTheaterCount, bluRegionProviderGarrisonCount;
	
	public int[][] dataRegionSORSystemTheaterCountReduced, dataRegionSORSystemGarrisonCountReduced;
	public int[][] bluRegionProviderTheaterCountReduced, bluRegionProviderGarrisonCountReduced;
	
	private ArrayList<Integer> dataReducedTheaterIndex, dataReducedGarrisonIndex;
	private ArrayList<Integer> bluReducedTheaterIndex, bluReducedGarrisonIndex;
	
	private IDatabaseEngine systemEngine;
	private IDatabaseEngine costEngine;// = "TAP_Cost_Data";
	private IDatabaseEngine siteEngine;// = "TAP_Site_Data";
	
	private String sysListBindings;//, capListBindings;
	private double hourlyRate = 150.0;
	
	private boolean reducedFunctionality = false;
	
	public void setSysDataBLULists(ArrayList<String> sysList,ArrayList<String> dataList,ArrayList<String> bluList,ArrayList<String> regionList,ArrayList<String> systemMustModernize,ArrayList<String> systemMustDecommission)
	{
		this.sysList = sysList;
		this.dataList = dataList;
		this.bluList = bluList;
		this.regionList = regionList;
		this.systemMustModernize = systemMustModernize;
		this.systemMustDecommission = systemMustDecommission;	
	}

	public void setHourlyRate(double hourlyRate) {
		this.hourlyRate = hourlyRate;
	}
	
	public void setEngines(IDatabaseEngine systemEngine, IDatabaseEngine costEngine, IDatabaseEngine siteEngine) {
		this.systemEngine = systemEngine;
		this.costEngine = costEngine;
		this.siteEngine = siteEngine;
	}
	
	public boolean fillDataStores() {
		sysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + "}";		
		fillSystemFunctionality();

		fillSystemCostOfData();
		fillSystemRegion();
		
		fillSystemCost();
		fillSystemNumOfSites();
		fillSystemDisposition();
		fillSystemCapability();
		fillSystemMHSSpecific();
		fillSystemRequired(); //requires the MHS Specific	
		fillSystemModAndDecomm();
		
		dataReducedIndex = new ArrayList<Integer>();
		bluReducedIndex = new ArrayList<Integer>();
	
		dataRegionSORSystemCount = calculateIfProviderExistsWithRegion(systemDataMatrix,true);
		bluRegionProviderCount = calculateIfProviderExistsWithRegion(systemBLUMatrix,false);
		
		dataRegionSORSystemCountReduced = removeReducedData(dataRegionSORSystemCount,dataReducedIndex);
		bluRegionProviderCountReduced = removeReducedData(bluRegionProviderCount,bluReducedIndex);

		return reducedFunctionality;
	}
	
	public boolean fillTheaterGarrisonDataStores(boolean includeTheater,boolean includeGarrison)
	{
		sysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + "}";		
		fillSystemFunctionality();

		fillSystemCostOfData();
		fillSystemRegion();
		
		fillSystemCost();
		fillSystemNumOfSites();
		fillSystemDisposition();
		fillSystemCapability();
		fillSystemMHSSpecific();
		fillSystemRequired(); //requires the MHS Specific	
		fillSystemModAndDecomm();

		fillSystemTheaterGarrison(includeTheater,includeGarrison);
	
		dataReducedTheaterIndex = new ArrayList<Integer>();
		dataReducedGarrisonIndex = new ArrayList<Integer>();
		bluReducedTheaterIndex = new ArrayList<Integer>();
		bluReducedGarrisonIndex = new ArrayList<Integer>();
		
		calculateIfProviderExistsWithRegionGT(systemDataMatrix,true);
		calculateIfProviderExistsWithRegionGT(systemBLUMatrix,false);
		
		dataRegionSORSystemTheaterCountReduced = removeReducedData(dataRegionSORSystemTheaterCount,dataReducedTheaterIndex);
		dataRegionSORSystemGarrisonCountReduced = removeReducedData(dataRegionSORSystemGarrisonCount,dataReducedGarrisonIndex);
		bluRegionProviderTheaterCountReduced = removeReducedData(bluRegionProviderTheaterCount,bluReducedTheaterIndex);
		bluRegionProviderGarrisonCountReduced = removeReducedData(bluRegionProviderGarrisonCount,bluReducedGarrisonIndex);

		return reducedFunctionality;
	}
	
	private void fillSystemFunctionality() {
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
		systemBLUMatrix = fillMatrixFromQuery(systemEngine,query,systemBLUMatrix,sysList,bluList);

	}
	
	private void fillSystemCostOfData() {
		systemCostOfDataMatrix = SysOptUtilityMethods.createEmptyDoubleMatrix(sysList.size(),dataList.size());
		String query = "SELECT DISTINCT ?sys ?data (SUM(?loe)*" + hourlyRate + " AS ?cost) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?sys ?data BINDINGS ?sys @SYSTEM-BINDINGS@";
	
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		
		systemCostOfDataMatrix = fillMatrixFromQuery(costEngine,query,systemCostOfDataMatrix,sysList,dataList);
	}
		
	private void fillSystemRegion() 	{
		if(regionList!=null) {
			includeRegionalization = true;
			systemRegionMatrix = SysOptUtilityMethods.createEmptyIntMatrix(sysList.size(),regionList.size());

			String query = "SELECT DISTINCT ?System ?Region WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?DeployedAt2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>} {?Includes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Includes>} {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HealthServiceRegion>} {?Located <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Located>} {?System ?DeployedAt1 ?SystemDCSite} {?SystemDCSite ?DeployedAt2 ?DCSite} {?DCSite ?Includes ?MTF} {?MTF ?Located ?Region} } BINDINGS ?System @SYSTEM-BINDINGS@";
			sysListBindings = "{" + SysOptUtilityMethods.makeBindingString("System",sysList) + "}";		
			query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
			systemRegionMatrix = fillMatrixFromQuery(siteEngine,query,systemRegionMatrix,sysList,regionList);
		} else {
			systemRegionMatrix = SysOptUtilityMethods.createEmptyIntMatrix(sysList.size(),1);
			for(int i = 0;i<systemRegionMatrix.length;i++)
				systemRegionMatrix[i][0] = 1;
		}
	}
	
	private void fillSystemCost() {
		systemCostOfMaintenance = new double[sysList.size()];
		Arrays.fill(systemCostOfMaintenance, 0);
		String query = "SELECT DISTINCT ?sys (COALESCE(?cost,0) AS ?Cost) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?sys <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}}BINDINGS ?sys @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCostOfMaintenance = fillVectorFromQuery(systemEngine,query,systemCostOfMaintenance,sysList,false);
		
		systemCostOfDB = new double[sysList.size()];
		Arrays.fill(systemCostOfDB, 0);
		query = "SELECT DISTINCT ?sys (COALESCE(?cost/10,0) AS ?Cost) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?sys <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}}BINDINGS ?sys @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCostOfDB = fillVectorFromQuery(systemEngine,query,systemCostOfDB,sysList,false);
	}
	
	private void fillSystemNumOfSites() {
		systemNumOfSites = new double[sysList.size()];
		Arrays.fill(systemNumOfSites, 0);

		String query = "SELECT DISTINCT ?System (COUNT(DISTINCT(?DCSite)) as ?Num_Of_Deployment_Sites) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite ?DeployedAt ?DCSite;} {?System ?DeployedAt1 ?SystemDCSite;} } GROUP BY ?System BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemNumOfSites = fillVectorFromQuery(siteEngine,query,systemNumOfSites,sysList,false);
	}
	private void fillSystemDisposition() {
		systemDisposition = new String[sysList.size()];
		Arrays.fill(systemDisposition, "");
		
		String query = "SELECT DISTINCT ?System ?Disposition WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Disposition> ?Disposition}{?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemDisposition = fillVectorFromQuery(systemEngine,query,systemDisposition,sysList,false);
	}
	private void fillSystemCapability() {
		systemCapability = new String[sysList.size()];
		Arrays.fill(systemCapability, "");
		
		systemCapabilityGroup = new String[sysList.size()];
		Arrays.fill(systemCapabilityGroup, "");
		
		systemCONOPS = new String[sysList.size()];
		Arrays.fill(systemCONOPS, "");
		
		String capQuery = "SELECT DISTINCT ?System ?Capability WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?System ?Supports ?Capability}} BINDINGS ?System @SYSTEM-BINDINGS@";
		capQuery = capQuery.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCapability = fillVectorFromQuery(systemEngine,capQuery,systemCapability,sysList,false);
		
		String capGroupQuery = "SELECT DISTINCT ?System ?CapabilityGroup WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?System ?Supports ?CapabilityGroup}} BINDINGS ?System @SYSTEM-BINDINGS@";
		capGroupQuery = capGroupQuery.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCapabilityGroup = fillVectorFromQuery(systemEngine,capGroupQuery,systemCapabilityGroup,sysList,false);
		
		String conopsQuery = "SELECT DISTINCT ?System ?CONOPS WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?CONOPS <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?System ?Supports ?CONOPS}} BINDINGS ?System @SYSTEM-BINDINGS@";
		conopsQuery = conopsQuery.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCONOPS = fillVectorFromQuery(systemEngine,conopsQuery,systemCONOPS,sysList,false);
	}
	private void fillSystemMHSSpecific() {

		systemMHSSpecific = new String[sysList.size()];
		Arrays.fill(systemMHSSpecific, "");
		
		String query = "SELECT DISTINCT ?System (IF(?MHS_Specific='Y','Yes','No') AS ?Specific) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> ?MHS_Specific}} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemMHSSpecific = fillVectorFromQuery(systemEngine,query,systemMHSSpecific,sysList,false);
	}
	
	private void fillSystemRequired() {
		systemModernize = new double[sysList.size()];
		Arrays.fill(systemModernize, 0);

		String query = "SELECT DISTINCT ?System ('Y' AS ?Required) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'Y'}} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemModernize = fillVectorFromQuery(systemEngine,query,systemModernize,sysList,true);
	}
	
	private void fillSystemModAndDecomm() {

		systemDecommission = new double[sysList.size()];
		Arrays.fill(systemDecommission, 0);

		for(String sys:systemMustModernize) {
			int sysIndex = sysList.indexOf(sys);
			if(sysIndex>-1)
				systemModernize[sysIndex] = 1;
		}
		for(String sys:systemMustDecommission) {
			int sysIndex = sysList.indexOf(sys);
			if(sysIndex>-1&&systemModernize[sysIndex]<1.0)
				systemDecommission[sysIndex] = 1;
		}
	}	
	
	public void fillSystemTheaterGarrison(boolean includeTheater,boolean includeGarrison)
	{
		String query = "SELECT DISTINCT ?System ?GT WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);

		if(includeTheater) {
			systemTheater = new int[sysList.size()];
			for(int i=0;i<sysList.size();i++)
				systemTheater[i] = 0;
		}
		if(includeGarrison) {
			systemGarrison = new int[sysList.size()];
			for(int i=0;i<sysList.size();i++)
				systemGarrison[i] = 0;
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
						systemTheater[rowIndex] = 1;

					//if(systemGarrison!=null && (theatGarr.contains("both")||theatGarr.contains("garrison")))
					if(includeGarrison && (!theatGarr.contains("theater") || theatGarr.contains("and")))
						systemGarrison[rowIndex] = 1;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					e.printStackTrace();
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
	
	private int[][] fillMatrixFromQuery(IDatabaseEngine engine, String query,int[][] matrix,ArrayList<String> rowNames,ArrayList<String> colNames) {
		ISelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			// get the bindings from it
			String[] names = wrapper.getVariables();
			// now get the bindings and generate the data
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				Object rowName = sjss.getVar(names[0]);
				Object colName = sjss.getVar(names[1]);

				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex > -1) {
					int colIndex = colNames.indexOf(colName);
					if(colIndex>-1) {
						matrix[rowIndex][colIndex] = 1;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return matrix;
	}
	
	private double[][] fillMatrixFromQuery(IDatabaseEngine engine, String query,double[][] matrix,ArrayList<String> rowNames,ArrayList<String> colNames) {
		ISelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			// get the bindings from it
			String[] names = wrapper.getVariables();
			// now get the bindings and generate the data
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				Object rowName = sjss.getVar(names[0]);
				Object colName = sjss.getVar(names[1]);
				
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex > -1) {
					int colIndex = colNames.indexOf(colName);
					if(colIndex > -1) {
						if(names.length > 2) {
							Object val = sjss.getVar(names[2]);
							if(val instanceof Double)
								matrix[rowIndex][colIndex] = (Double)val;
							if(val instanceof Integer)
								matrix[rowIndex][colIndex] = ((Integer)val) * 1.0;
							else if(val instanceof String) {
								try {
									matrix[rowIndex][colIndex] = Double.parseDouble((String)val);
								}catch(NumberFormatException e){
									matrix[rowIndex][colIndex] = 0.0;
								}
							}
							matrix[rowIndex][colIndex] = (Double)sjss.getVar(names[2]);
						} else {
							matrix[rowIndex][colIndex] = 1.0;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return matrix;
	}
	
	private double[] fillVectorFromQuery(IDatabaseEngine engine, String query,double[] matrix,ArrayList<String> rowNames, boolean needsConversion) {
		ISelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			// get the bindings from it
			String[] names = wrapper.getVariables();
			// now get the bindings and generate the data
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				Object rowName = sjss.getVar(names[0]);
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex > -1) {
					if(!needsConversion) {
						Object val = sjss.getVar(names[1]);
						if(val instanceof Double)
							matrix[rowIndex]= (Double)val;
						if(val instanceof Integer)
							matrix[rowIndex]= ((Integer)val) * 1.0;
						else if(val instanceof String) {
							try {
								matrix[rowIndex]= Double.parseDouble((String)val);
							}catch(NumberFormatException e){
								matrix[rowIndex]= 0.0;
							}
						}
					}
					else
					{
						String requiredVal = (String)sjss.getVar(names[1]);
						if(requiredVal.toUpperCase().contains("Y")) {
							matrix[rowIndex]= 1.0;
						} else {
							matrix[rowIndex] = 0.0;
						}
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return matrix;
	}
	
	private String[] fillVectorFromQuery(IDatabaseEngine engine, String query,String[] matrix,ArrayList<String> rowNames, boolean valIsOne) {
		ISelectWrapper wrapper;
		try {
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			// get the bindings from it
			String[] names = wrapper.getVariables();
			// now get the bindings and generate the data
			try {
				while(wrapper.hasNext())
				{
					ISelectStatement sjss = wrapper.next();
					Object rowName = sjss.getVar(names[0]);
					int rowIndex = rowNames.indexOf(rowName);
					if(rowIndex>-1)
					{
						matrix[rowIndex]= (String)sjss.getVar(names[1]);
					}
				}
			} catch (RuntimeException e) {
				logger.fatal(e);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return matrix;
	}
	
	private int[][] calculateIfProviderExistsWithRegion(int[][] sysMatrix,boolean isData) {
		int[][] retVector = new int[sysMatrix[0].length][1];
		if(includeRegionalization)
			retVector = new int[sysMatrix[0].length][regionList.size()];
		//for every system in the system matrix
		for(int col=0;col<sysMatrix[0].length;col++)
		{
			for(int regionInd=0;regionInd<retVector[0].length;regionInd++)
			{
				int numProviders = 0;
				//checks to see if only systems that are on the "decomm" list provide this data.
				//if so, then we need to remove the data object from analysis and set reducedFunctionality to true.
				boolean decommOnly=true;
				for(int row=0;row<sysMatrix.length;row++)
				{
					//check to see if that system is in the region we're currently looking at
					if(systemRegionMatrix[row][regionInd]>=1.0) {
						numProviders+=sysMatrix[row][col];
						if(sysMatrix[row][col]>=1 && systemDecommission[row]==0.0)
							decommOnly=false;
					}
				}
				if(numProviders==0)
					reducedFunctionality = true;
				if(numProviders!=0&&decommOnly) {
					reducedFunctionality = true;
					if(isData)
						dataReducedIndex.add(col);
					else
						bluReducedIndex.add(col);
				}
				retVector[col][regionInd] =numProviders;
			}
		}
		return retVector;
	}
	
	private int[][] removeReducedData(int[][] matrixToReduce, ArrayList<Integer> indicesToReduce) {
		for(int index : indicesToReduce ) {
			for(int col=0;col<matrixToReduce[index].length;col++) {
				matrixToReduce[index][col]=0;
			}
		}
		return matrixToReduce;		
	}
	
	public double percentDataBLUReplacedReg(int modIndex,int decommIndex) {
		double overlapCount = 0.0;
		double totalCount = 0.0;
		for(int dataInd=0;dataInd<systemDataMatrix[modIndex].length;dataInd++) {
			if(systemDataMatrix[decommIndex][dataInd]>0) {
				totalCount++;
			}
			if(systemDataMatrix[decommIndex][dataInd]>0&&systemDataMatrix[modIndex][dataInd]>0) {
				overlapCount++;
			}
		}
		for(int bluInd=0;bluInd<systemBLUMatrix[modIndex].length;bluInd++) {
			if(systemBLUMatrix[decommIndex][bluInd]>0) {
				totalCount++;
			}
			if(systemBLUMatrix[decommIndex][bluInd]>0&&systemBLUMatrix[modIndex][bluInd]>0) {
				overlapCount++;
			}
		}
		if(totalCount==0.0)
			return 0.0;
		return overlapCount/totalCount;
	}
	
	public double percentDataBLUReplacedTG(int modIndex,int decommIndex) {
		//if the modernized system cannot act in the environments the deommisioned system can, then 0%
		//if decomm system in theater (systemTheater=1), then mod must be in theater return false if mod is not in theater(systemTheater=0)
		if(systemTheater!=null&&systemTheater[decommIndex]>systemTheater[modIndex])
			return 0.0;
		if(systemGarrison!=null&&systemGarrison[decommIndex]>systemGarrison[modIndex])
			return 0.0;
		return percentDataBLUReplacedReg(modIndex,decommIndex);
	}
	
	public boolean didSystemProvideReducedDataBLUReg(int decommIndex) {
		if(didSysProvideReduced(systemDataMatrix,decommIndex,dataReducedIndex))
			return true;
		if(didSysProvideReduced(systemBLUMatrix,decommIndex,bluReducedIndex))
			return true;
		return false;
	}
	
	public boolean didSystemProvideReducedDataBLUTG(int decommIndex) {
		if(systemTheater!=null&&systemTheater[decommIndex]>0.0) {
			if(didSysProvideReduced(systemDataMatrix,decommIndex,dataReducedTheaterIndex))
				return true;
			if(didSysProvideReduced(systemBLUMatrix,decommIndex,bluReducedTheaterIndex))
				return true;
		}
		if(systemGarrison!=null&&systemGarrison[decommIndex]>0.0) {
			if(didSysProvideReduced(systemDataMatrix,decommIndex,dataReducedGarrisonIndex))
				return true;
			if(didSysProvideReduced(systemBLUMatrix,decommIndex,bluReducedGarrisonIndex))
				return true;
		}
		return false;
	}
	
	private boolean didSysProvideReduced(int[][] systemMatrix, int decommIndex, ArrayList<Integer> reducedIndex) {
		for(Integer dataInd : reducedIndex) {
			if(systemMatrix[decommIndex][dataInd]>0.0) {
				return true;
			}
		}
		return false;
	}
	
	private void calculateIfProviderExistsWithRegionGT(int[][] sysMatrix,boolean isData)
	{
		int[][] theaterProviderCount = new int[sysMatrix[0].length][1];
		int[][] garrisonProviderCount = new int[sysMatrix[0].length][1];
		int regions = 1;
		if(includeRegionalization) {
			regions = regionList.size();
			theaterProviderCount = new int[sysMatrix[0].length][regionList.size()];
			garrisonProviderCount = new int[sysMatrix[0].length][regionList.size()];
		}
		//for every system in the system matrix
		for(int col=0;col<sysMatrix[0].length;col++)
		{
			for(int regionInd=0;regionInd<regions;regionInd++)
			{
				int numTheaterProviders = 0;
				int numGarrisonProviders = 0;
				boolean decommTheaterOnly=true;
				boolean decommGarrisonOnly=true;
				for(int row=0;row<sysMatrix.length;row++) {
					//check to see if that system is in the region we're currently looking at
					if(systemRegionMatrix[row][regionInd]>=1.0) {
						if(systemTheater!=null&&systemRegionMatrix[row][regionInd]>=1.0&&systemTheater[row]>=1) {
							numTheaterProviders+=sysMatrix[row][col];
							if(sysMatrix[row][col]>=1.0&&systemDecommission[row]==0.0)
								decommTheaterOnly = false;
						}
						if(systemGarrison!=null&&systemRegionMatrix[row][regionInd]>=1.0&&systemGarrison[row]>=1) {
							numGarrisonProviders+=sysMatrix[row][col];
							if(sysMatrix[row][col]>=1.0&&systemDecommission[row]==0.0)
								decommGarrisonOnly = false;
						}
					}
				}
				if(numTheaterProviders==0&&numGarrisonProviders==0)
					reducedFunctionality = true;
				if(numTheaterProviders!=0&&decommTheaterOnly) {
					reducedFunctionality=true;
					if(isData)
						dataReducedTheaterIndex.add(col);
					else
						bluReducedTheaterIndex.add(col);
				}
				if(numGarrisonProviders!=0&&decommGarrisonOnly) {
					reducedFunctionality=true;
					if(isData)
						dataReducedGarrisonIndex.add(col);
					else
						bluReducedGarrisonIndex.add(col);
				}	
				if(systemTheater!=null)
					theaterProviderCount[col][regionInd] =numTheaterProviders;
				if(systemGarrison!=null)
					garrisonProviderCount[col][regionInd] =numGarrisonProviders;
			}
		}
		if(isData) {
			if(systemTheater!=null)
				dataRegionSORSystemTheaterCount = theaterProviderCount;
			if(systemGarrison!=null)
				dataRegionSORSystemGarrisonCount = garrisonProviderCount;
		} else {
			if(systemTheater!=null)
				bluRegionProviderTheaterCount = theaterProviderCount;
			if(systemGarrison!=null)
				bluRegionProviderGarrisonCount = garrisonProviderCount;
		}
	}

}