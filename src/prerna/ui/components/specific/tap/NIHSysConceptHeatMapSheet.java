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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;

/**
 * Heat Map that shows what gaps in data exist for systems to suppor their BLUs
 */
public class NIHSysConceptHeatMapSheet extends SimilarityHeatMapSheet{

	public ArrayList<String> systemNamesList = new ArrayList<String>();
	String systemKey = "BUSINESS_AREA";
	String conceptKey = "Concept";
	String columnNameKey = "COLUMN_NAME";
	String tableCountKey = "TableCount";
	final String valueString = "Score";
	final String keyString = "key";
	Hashtable<String, Hashtable<String, Double>> fullConceptColumnNameMapping = new Hashtable<String, Hashtable<String, Double>>();//concept -> columnName -> 0.
	String fullConceptColumnNameQuery = "SELECT DISTINCT ?Concept ?COLUMN_NAME WHERE { {?COLUMN_NAME <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/COLUMN_NAME>} {?Concept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Concept>} {?COLUMN_NAME <http://semoss.org/ontologies/Relation/Represents> ?Concept} }";
	ArrayList<String> conceptNames = new ArrayList<String>();
	ArrayList<String> sysNames = new ArrayList<String>();


	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public NIHSysConceptHeatMapSheet() {
		super();
	}	

	@Override
	public void createData() {

		addPanel();	
		String comparisonType = "System-Concept Comparison";
		logger.info("Creating " + comparisonType + " Heat Map.");
		updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
		fillFullHash();
		
		//Creating hashtable from main query 
		SesameJenaSelectWrapper mainWrapper = new SesameJenaSelectWrapper();
		mainWrapper.setQuery(this.query);
		mainWrapper.setEngine(this.engine);
		mainWrapper.executeQuery();
		names = mainWrapper.getVariables();
		processWrapper(mainWrapper, names);
		scoreAdder();
		
		//Creating keyHash from paramDataHash
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + comparisonType, 70);		
		
		logger.info(systemNamesList);

		allHash.put("xAxisTitle", "Concept");
		allHash.put("title",  "Systems Support " + comparisonType);
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
		
	}
	
	private void fillFullHash(){
		SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
		sjw.setQuery(this.fullConceptColumnNameQuery);
		sjw.setEngine(this.engine);
		sjw.executeQuery();
		String[] mappingNames = sjw.getVariables();
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();
				String conceptTemp = sjss.getVar(conceptKey) + "";
				String colTemp = sjss.getVar(columnNameKey) + "";
				Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
				if(fullConceptColumnNameMapping.containsKey(conceptTemp)){
					innerHash = fullConceptColumnNameMapping.get(conceptTemp);
				}
				innerHash.put(colTemp, 0.);
				fullConceptColumnNameMapping.put(conceptTemp, innerHash);
			}
		}catch(RuntimeException e){
			e.printStackTrace();
		}
				
	}
	
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names){
		
		Hashtable<String, Hashtable<String, Object>> conceptTableHash = new Hashtable<String, Hashtable<String, Object>>();
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();
				
				Hashtable<String, Object> systemConceptHash = new Hashtable<String, Object>();
				Hashtable<String, Object> keySystemConceptHash = new Hashtable<String, Object>();
				Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();
				String systemTemp = sjss.getVar(systemKey) + "";
				String conceptTemp = sjss.getVar(conceptKey) + "";
				String columnTemp = sjss.getVar(columnNameKey) + "";
				Double tableCountTemp = (Double) sjss.getVar(tableCountKey);

				String sysConceptTemp = systemTemp + "-" + conceptTemp;
				
				if(!conceptTableHash.containsKey(sysConceptTemp)){
					innerDataHash.putAll(fullConceptColumnNameMapping.get(conceptTemp));
					innerDataHash.put(columnTemp, tableCountTemp);
					systemConceptHash.put("System", systemTemp);
					systemConceptHash.put("Concept", conceptTemp);
					systemConceptHash.put("Column", innerDataHash);
					conceptTableHash.put(sysConceptTemp, systemConceptHash);
					
					//for keyHash
					keySystemConceptHash.put("System", systemTemp);
					keySystemConceptHash.put("Concept", conceptTemp);
					keyHash.put(sysConceptTemp, keySystemConceptHash);

				}
				else{
					systemConceptHash = (Hashtable<String, Object>) conceptTableHash.get(sysConceptTemp);
					innerDataHash = (Hashtable<String, Double>) systemConceptHash.get("Column");
					innerDataHash.put(columnTemp, tableCountTemp);
				}
			}
			paramDataHash.put("Concept-Column", conceptTableHash);

		} 
		catch (RuntimeException e) {
			logger.fatal(e);
		}	
	}
	
	private void scoreAdder(){
		Hashtable<String, Hashtable<String, Object>> systemConHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("Concept-Column"); 
		
		Set<String> sysConceptKeys = systemConHash.keySet();
		try {		
			for(String sysConKey: sysConceptKeys){
				Hashtable<String, Object> innerSysConHash = new Hashtable<String, Object>();
				Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();	
				
				innerSysConHash = (Hashtable<String, Object>) systemConHash.get(sysConKey);
				innerDataHash = (Hashtable<String, Double>) innerSysConHash.get("Column");
	
				Set<String> colKeys = innerDataHash.keySet();
				Double total = 0.;
				for(String colKey: colKeys){
					total += innerDataHash.get(colKey);
				}
				innerSysConHash.put(valueString, total);	
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}

	public ArrayList retrieveValues(ArrayList<String> selectedVars, Hashtable<String, Double>minimumWeights, String key){
		ArrayList<Hashtable> retHash = new ArrayList<Hashtable>();
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		BLUDataHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("Concept-Column");
		Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();		
		systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(key);
		Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();	
		innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Column");
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


