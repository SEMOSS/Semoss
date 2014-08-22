/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.util.DIHelper;

/**
 * Heat Map that shows how much a System Supports a Capability
 */
public class SysCapSimHeatMapSheet extends SimilarityHeatMapSheet {

	private final String hrCoreDB = "HR_Core";
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	private final String capabilityQuery = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}}";
	private final String businessProcessQuery = "SELECT DISTINCT ?BusinessProcess WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}}";	
	
	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public SysCapSimHeatMapSheet() {
		super();	
	}	
	
	public void createData() {
		addPanel();
		SimilarityFunctions sdf = new SimilarityFunctions();	
		SysBPCapInsertProcessor processor = new SysBPCapInsertProcessor(0.0, 0.0, "AND");
		String comparisonType = this.query;
		logger.info("Creating " + comparisonType + " to System Heat Map.");
		
			if (comparisonType.equals("Capability")) {
				allHash.put("xAxisTitle", "Capability");
				updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
				comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, capabilityQuery);
				setComparisonObjectTypes("Capability", "System");
				updateProgressBar("35%...Querying Data", 35);
				processor.genStorageInformation(coreDB, comparisonType);
			}
			else if(comparisonType.equals("BusinessProcess")) {
				allHash.put("xAxisTitle", "BusinessProcess");
				updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
				comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, businessProcessQuery);
				setComparisonObjectTypes("BusinessProcess", "System");
				updateProgressBar("35%...Querying Data", 35);
				processor.genStorageInformation(coreDB, comparisonType);
			}		
		sdf.setComparisonObjectList(comparisonObjectList);
		HashMap getData = new HashMap();
		
		Hashtable resultHash = new Hashtable();
		resultHash.putAll(processor.storageHash);		
		
		updateProgressBar("50%...Evaluating Data Objects Created for a " + comparisonType, 50);
		getData = (HashMap) resultHash.get(processor.DATAC);
		Hashtable dataCScoreHash = convertMapToTable(getData);
		logger.info("Finished Data Objects Created Processing.");
		dataCScoreHash = processHashForCharting(dataCScoreHash);		
		
		/*updateProgressBar("60%...Evaluating Data Objects Read for a Capability", 60);
		Hashtable dataRScoreHash = (Hashtable) resultHash.get(processor.DATAR);
		logger.info("Finished Data Objects Read Processing.");
		dataRScoreHash = processHashForCharting(dataRScoreHash);*/		
		
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + comparisonType, 70);
		getData.clear();
		getData = (HashMap) resultHash.get(processor.BLU);
		Hashtable bluScoreHash = convertMapToTable(getData);
		logger.info("Finished Business Logic Provided Processing.");
		bluScoreHash = processHashForCharting(bluScoreHash);		
			
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
	
		paramDataHash.put("Data_Objects_Created", dataCScoreHash);
		//paramDataHash.put("Data_Objects_Read", dataRScoreHash);
		paramDataHash.put("Business_Logic_Provided", bluScoreHash);
		
		allHash.put("title",  "Systems Support " + comparisonType);		
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
	}
	
	private Hashtable convertMapToTable(HashMap entry) {
		Hashtable returnHash = new Hashtable();
		for(Object key: entry.keySet()){
			HashMap innerMap = new HashMap();
			Hashtable returnInnerTable = new Hashtable();
			innerMap = (HashMap) entry.get(key);
			for(Object innerKey : innerMap.keySet()) {
				returnInnerTable.put(innerKey, innerMap.get(innerKey));
			}
			returnHash.put(key, returnInnerTable);
		}
		
		return returnHash;
	}
}
