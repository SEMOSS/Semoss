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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

/**
 * Heat Map that shows how much a System Supports a Capability
 */
public class BLUSysComparison extends SimilarityHeatMapSheet{

	String hrCoreDB = "HR_Core";
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	public ArrayList<String> BLUList = new ArrayList<String>();
	public ArrayList<String> systemNamesList = new ArrayList<String>();

	String masterQuery = "SELECT DISTINCT ?System ?BLU ?Data (IF (BOUND (?Provide), 'Needed and Present', IF( BOUND(?ICD), 'Needed and Present', 'Needed but not Present')) AS ?Status) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'} OPTIONAL{ { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} {?Consume <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?ICD ?Consume ?System} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?ICD ?Payload ?Data} } UNION { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?Provide ?Data} } } { SELECT DISTINCT ?System ?BLU ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?ProbabilityNeeded} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Provide2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?Provide2 ?BLU} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Requires <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Requires>;} {?BLU ?Requires ?Data} FILTER(?ProbabilityNeeded != 'High') } } } BINDINGS ?Probability {('Medium')('Low')('Medium-High')}";
	String BLUListQuery = "SELECT DISTINCT ?BLU WHERE {{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}}";
	String systemKey = "System";
	String BLUKey = "BLU";
	String dataKey = "Data";
	String statusKey = "Status";
	String statusTrueKey = "Needed and Present";
	final String valueString = "Score";
	final String keyString = "key";
	
	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public BLUSysComparison() {
		super();	
	}	
	
	public void createData() {

		addPanel();
		SimilarityFunctions simfns = new SimilarityFunctions();	
		String comparisonType = "System-BLU Comparison";
		logger.info("Creating " + comparisonType + " Heat Map.");
		updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
		
		//Creating ArrayList of BLUs
		BLUList = simfns.createComparisonObjectList(hrCoreDB, BLUListQuery);
		updateProgressBar("35%...Querying Data", 35);
		
		//Creating hashtable from main query 
		SesameJenaSelectWrapper mainWrapper = new SesameJenaSelectWrapper();
		mainWrapper.setQuery(masterQuery);
		mainWrapper.setEngine(coreDB);
		mainWrapper.executeQuery();
		names = mainWrapper.getVariables();
		processWrapper(mainWrapper, names);
		averageAdder();
		updateProgressBar("50%...Evaluating Data Objects Created for a " + comparisonType, 50);
		
		//Creating keyHash from paramDataHash
		keyHashExtractor();
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + comparisonType, 70);		
		

		logger.info(systemNamesList);

		allHash.put("xAxisTitle", "BLU");
		allHash.put("title",  "Systems Support " + comparisonType);
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
		
	}
	
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names){
		// now get the bindings and generate the data
		
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();


		try {
			while(sjw.hasNext()) //while still row beneath (while data left)
			{
				SesameJenaSelectStatement sjss = sjw.next(); //go to new row
				
				Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();
				Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();
				String systemTemp = "";
				String BLUTemp = "";
				String dataTemp = "";
				String statusTemp = "";
				String sysBLUTemp = "";
				Double dataValueTemp = 0.0;
				
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(names[colIndex].contains(systemKey)){
						systemTemp = sjss.getVar(names[colIndex]) + "";
					}
					else if(names[colIndex].contains(BLUKey)){
						BLUTemp = sjss.getVar(names[colIndex]) + "";
					}
					else if(names[colIndex].contains(dataKey)){
						dataTemp = sjss.getVar(names[colIndex]) + "";
					}
					else if(names[colIndex].contains(statusKey)){
						statusTemp = sjss.getVar(names[colIndex]) + "";
					}
					else{
						logger.info("failed to identify expected key in query");
					}
				}
				if(statusTemp.equals(statusTrueKey)){
					dataValueTemp = 100.0;
				}
				
				sysBLUTemp = systemTemp + "-" + BLUTemp;
				
				if(!BLUDataHash.containsKey(sysBLUTemp)){
					innerDataHash.put(dataTemp, dataValueTemp);
					systemBLUHash.put("System", systemTemp);
					systemBLUHash.put("BLU", BLUTemp);
					systemBLUHash.put("Data", innerDataHash);
					BLUDataHash.put(sysBLUTemp, systemBLUHash);
					
				}
				else{
					systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(sysBLUTemp);
					innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
					innerDataHash.put(dataTemp, dataValueTemp);
				}
			}
			paramDataHash.put("BLU-Data", BLUDataHash);
		} 
		catch (Exception e) {
			logger.fatal(e);
		}	
	}
	
	private void averageAdder(){
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		
		BLUDataHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("BLU-Data"); 
		
		Set<String> sysBLUKeys = BLUDataHash.keySet();
		try {		
			for(String sysBLUKey: sysBLUKeys){
				Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();
				Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();	
				
				systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(sysBLUKey);
				innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
	
				Double numerator = 0.0;
				Double denominator = 0.0;
				Set<String> dataKeys = innerDataHash.keySet();
				for(String dataKey: dataKeys){
					numerator += innerDataHash.get(dataKey);
					denominator++;
				}
				Double average = (double) (numerator/denominator);
				systemBLUHash.put("Score", average);	
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}
		
	private void keyHashExtractor(){
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		
		BLUDataHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("BLU-Data"); 
		
		Set<String> sysBLUKeys = BLUDataHash.keySet();
		try {		
			for(String sysBLUKey: sysBLUKeys){
				Hashtable<String, Object> paramSysBLUHashTemp = new Hashtable<String, Object>();
				Hashtable<String, String> sysBLUHashTemp = new Hashtable<String, String>();
				String systemTemp = "";
				String bluTemp = "";
				
				paramSysBLUHashTemp = (Hashtable<String, Object>) BLUDataHash.get(sysBLUKey);
				systemTemp = (String) paramSysBLUHashTemp.get("System");
				bluTemp = (String) paramSysBLUHashTemp.get("BLU");
				sysBLUHashTemp.put("System", systemTemp);
				sysBLUHashTemp.put("BLU", bluTemp);
				
				keyHash.put(sysBLUKey, sysBLUHashTemp);
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}
	
	public ArrayList retrieveValues(ArrayList<String> selectedVars, Hashtable<String, Double>minimumWeights, String key){
		ArrayList<Hashtable> retHash = new ArrayList<Hashtable>();
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		BLUDataHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("BLU-Data");
		Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();		
		systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(key);
		Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();	
		innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
		Set<String> innerDataKeys = innerDataHash.keySet();
		for(String innerDataKey: innerDataKeys){
			Hashtable newHash = new Hashtable();
			newHash.put(keyString, innerDataKey);
			newHash.put(valueString, innerDataHash.get(innerDataKey));
			retHash.add(newHash);
		}			
		return retHash;
	}
}
	





