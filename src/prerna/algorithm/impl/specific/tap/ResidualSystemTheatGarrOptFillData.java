/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

public class ResidualSystemTheatGarrOptFillData extends ResidualSystemOptFillData{
	protected static final Logger logger = LogManager.getLogger(ResidualSystemTheatGarrOptFillData.class.getName());
	
	public int[] systemTheater;
	public int[] systemGarrison;
	
	private int[][] dataRegionSORSystemTheaterCount;
	private int[][] dataRegionSORSystemGarrisonCount;
	private int[][] bluRegionProviderTheaterCount;
	private int[][] bluRegionProviderGarrisonCount;
	
	public int[][] dataRegionSORSystemTheaterCountReduced;
	public int[][] dataRegionSORSystemGarrisonCountReduced;
	public int[][] bluRegionProviderTheaterCountReduced;
	public int[][] bluRegionProviderGarrisonCountReduced;
	
	private ArrayList<Integer> dataReducedTheaterIndex;
	private ArrayList<Integer> dataReducedGarrisonIndex;
	private ArrayList<Integer> bluReducedTheaterIndex;
	private ArrayList<Integer> bluReducedGarrisonIndex;
	
	public boolean fillDataStores(boolean includeTheater,boolean includeGarrison)
	{
		instantiate();
		runQueries();
		fillSystemTheaterGarrison(includeTheater,includeGarrison);
	
		dataRegionSORSystemCount = calculateIfProviderExistsWithRegion(systemDataMatrix,true);
		bluRegionProviderCount = calculateIfProviderExistsWithRegion(systemBLUMatrix,false);
	
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
		
		if(playSheet!=null)
			printToConsole();
		else
			printAll();

		return reducedFunctionality;
	}
	
	private void fillSystemTheaterGarrison(boolean includeTheater,boolean includeGarrison)
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

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(systemEngine);

		/*SesameJenaSelectWrapper	wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		wrapper.executeQuery();	
		*/
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			String sysName = (String)getVariable(names[0], sjss);
			int rowIndex = sysList.indexOf(sysName);
			if(rowIndex>-1) {
				String theatGarr = (String)getVariable(names[1], sjss);
				theatGarr = theatGarr.toLowerCase();
				//if(systemTheater!=null && (theatGarr.contains("both")||theatGarr.contains("theater")))
				if(includeTheater && !theatGarr.contains("garrison"))
					systemTheater[rowIndex] = 1;

				//if(systemGarrison!=null && (theatGarr.contains("both")||theatGarr.contains("garrison")))
				if(includeGarrison && !theatGarr.contains("theater"))
					systemGarrison[rowIndex] = 1;
			}
		}
	}
	protected void calculateIfProviderExistsWithRegionGT(int[][] sysMatrix,boolean isData)
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
	private void printAll()
	{
		logger.info("System Provides Data:");
		printMatrix(systemDataMatrix,sysList,dataList);

		logger.info("System Cost to Provide Data:");
		printMatrix(systemCostOfDataMatrix,sysList,dataList);
		
		logger.info("System Provides BLU:");
		printMatrix(systemBLUMatrix,sysList,bluList);
		
		logger.info("System Cost to Maintain:");
		printVector(systemCostOfMaintenance,sysList);
		logger.info("System Cost to Maintain DB:");
		printVector(systemCostOfDB,sysList);
		logger.info("System Site List:");
		printVector(systemNumOfSites,sysList);
		
		logger.info("New site list must provide data in theater:");
		printMatrix(dataRegionSORSystemTheaterCount,dataList,regionList);
		logger.info("New site list must provide data in garrison:");
		printMatrix(dataRegionSORSystemGarrisonCount,dataList,regionList);
		
		logger.info("New site list must provide blu in theater:");
		printMatrix(bluRegionProviderTheaterCount,bluList,regionList);		
		logger.info("New site list must provide blu in garrison:");
		printMatrix(bluRegionProviderGarrisonCount,bluList,regionList);
	}
	protected void printToConsole()
	{
		super.printToConsole();
		if(systemTheater!=null) {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nTheater Systems: ");
			printNonZerosToConsoleList(deepCopy(sysList),systemTheater);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Theater Systems that are SOR of Data Object...");
			printNumberToConsoleList(deepCopy(dataList),dataRegionSORSystemTheaterCount);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Theater Systems that provide BLU by region...");
			printNumberToConsoleList(deepCopy(bluList),bluRegionProviderTheaterCount);
		}
		
		if(systemGarrison!=null) {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nGarrison Systems: ");
			printNonZerosToConsoleList(deepCopy(sysList),systemGarrison);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Garrison Systems that are SOR of Data Object...");
			printNumberToConsoleList(deepCopy(dataList),dataRegionSORSystemGarrisonCount);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Garrison Systems that provide BLU by region...");
			printNumberToConsoleList(deepCopy(bluList),bluRegionProviderGarrisonCount);
		}
	}
	private void printNonZerosToConsoleList(ArrayList<String> sysList,int[] listToCheck)
	{
		for(int i=0;i<listToCheck.length;i++)
			if(listToCheck[i]>0)
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+sysList.get(i)+", ");
	}
	@Override
	public double percentDataBLUReplaced(int modIndex,int decommIndex) {
		//if the modernized system cannot act in the environments the deommisioned system can, then 0%
		//if decomm system in theater (systemTheater=1), then mod must be in theater return false if mod is not in theater(systemTheater=0)
		if(systemTheater!=null&&systemTheater[decommIndex]>systemTheater[modIndex])
			return 0.0;
		if(systemGarrison!=null&&systemGarrison[decommIndex]>systemGarrison[modIndex])
			return 0.0;
		return super.percentDataBLUReplaced(modIndex,decommIndex);
	}
	
	@Override
	public boolean didSystemProvideReducedDataBLU(int decommIndex) {
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
	@Override
	protected boolean didSysProvideReduced(int[][] systemMatrix, int decommIndex, ArrayList<Integer> reducedIndex) {
		for(Integer dataInd : reducedIndex) {
			if(systemMatrix[decommIndex][dataInd]>0.0) {
				return true;
			}
		}
		return false;
	}
}
