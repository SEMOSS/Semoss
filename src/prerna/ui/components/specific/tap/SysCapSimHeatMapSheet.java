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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

/**
 * Heat Map that shows how much a System Supports a Capability
 */
public class SysCapSimHeatMapSheet extends SimilarityHeatMapSheet {

	String hrCoreDB = "HR_Core";
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	String capabilityQuery = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}}";
	String businessProcessQuery = "SELECT DISTINCT ?BusinessProcess WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}}";	
	
	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public SysCapSimHeatMapSheet() {
		super();
		setComparisonObjectTypes("Capability", "System");	
	}	
	
	public void createData() {
		addPanel();
		SimilarityFunctions sdf = new SimilarityFunctions();	
		SysBPCapInsertProcessor processor = new SysBPCapInsertProcessor(0.0, 0.0, "AND");
		logger.info("Creating " + this.query + " to System Heat Map.");
		
		updateProgressBar("10%...Getting " + this.query + " list for evaluation", 10);
		comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, capabilityQuery);
		if (this.query.equals("Capability")) {
			comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, capabilityQuery);
		}
		else if(this.query.equals("BusinessProcess")) {
			comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, businessProcessQuery);
		}
		sdf.setComparisonObjectList(comparisonObjectList);
		
		updateProgressBar("20%...Getting all Systems for evaluation", 20);		
		Hashtable systemDataHash = processor.getQueryResultHash(coreDB, processor.SYSTEM_DATA_QUERY);
		Hashtable systemBLUHash = processor.getQueryResultHash(coreDB, processor.SYSTEM_BLU_QUERY);	
		
		updateProgressBar("35%...Querying Data", 35);
		if (this.query.equals("Capability")) {
			Hashtable capDataHash = processor.getQueryResultHash(coreDB, processor.CAPABILITY_DATA_QUERY);
			Hashtable capBLUHash = processor.getQueryResultHash(coreDB, processor.CAPABILITY_BLU_QUERY);		
			processor.genRelationsForStorage(capDataHash, capBLUHash, systemDataHash, systemBLUHash);
		}
		else if(this.query.equals("BusinessProcess")) {
			Hashtable bpDataHash = processor.getQueryResultHash(coreDB, processor.BUSINESS_PROCESSES_DATA_QUERY);
			Hashtable bpBLUHash = processor.getQueryResultHash(coreDB, processor.BUSINESS_PROCESSES_BLU_QUERY);		
			processor.genRelationsForStorage(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash);
		}		
		Hashtable resultHash = processor.storageHash;		
		
		updateProgressBar("50%...Evaluating Data Objects Created for a " + this.query, 50);
		Hashtable dataCScoreHash = (Hashtable) resultHash.get(processor.DATAC);
		logger.info("Finished Data Objects Created Processing.");
		dataCScoreHash = processHashForCharting(dataCScoreHash);		
		
		/*updateProgressBar("60%...Evaluating Data Objects Read for a Capability", 60);
		Hashtable dataRScoreHash = (Hashtable) resultHash.get(processor.DATAR);
		logger.info("Finished Data Objects Read Processing.");
		dataRScoreHash = processHashForCharting(dataRScoreHash);*/		
		
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + this.query, 70);
		Hashtable bluScoreHash = (Hashtable) resultHash.get(processor.BLU);
		logger.info("Finished Business Logic Provided Processing.");
		bluScoreHash = processHashForCharting(bluScoreHash);		
			
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
	
		paramDataHash.put("Data_Objects_Created", dataCScoreHash);
		//paramDataHash.put("Data_Objects_Read", dataRScoreHash);
		paramDataHash.put("Business_Logic_Provided", bluScoreHash);
		
		allHash.put("title",  "Systems Support " + this.query);
		allHash.put("xAxisTitle", "Capability");
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
	}
}
