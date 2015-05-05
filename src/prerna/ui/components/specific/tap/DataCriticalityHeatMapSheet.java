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
package prerna.ui.components.specific.tap;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

/** Heat map that dynamically updates with critical data objects given a certain threshold.
 */

public class DataCriticalityHeatMapSheet extends SimilarityHeatMapSheet {
	
	String hrCoreDB = "HR_Core";
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	String capabilityQuery = "SELECT DISTINCT ?DHMSMCapability WHERE { {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>} {?DHMSMCapability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM ?TaggedBy ?DHMSMCapability} }";
	Hashtable<String, Hashtable<String, Double>> dataHash = new Hashtable<String, Hashtable<String, Double>>();
	String dhmsmCapDataQuery = "SELECT DISTINCT ?DHMSMCapability ?DataObject ((sample(?countData) * 100) / (sample(?countTask)) as ?weightPrint) WHERE { {SELECT DISTINCT ?DHMSMCapability (COUNT(?Task)/2 as ?countTask) WHERE { {?DHMSMCapability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?DHMSMCapability ?Consists ?Task} } GROUP BY ?DHMSMCapability } {SELECT DISTINCT ?DHMSMCapability ?DataObject (COUNT(?Task)/4 as ?countData) WHERE { {?DHMSMCapability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?DHMSMCapability ?Consists ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Task ?Needs ?DataObject} } GROUP BY ?DHMSMCapability ?DataObject } {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>} {?DHMSMCapability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM ?TaggedBy ?DHMSMCapability} {?Consists1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?DHMSMCapability ?Consists1 ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Task ?Needs ?DataObject} } GROUP BY ?DHMSMCapability ?DataObject";
	
	public DataCriticalityHeatMapSheet() {
		super();	
	}	
		
	public void createData() {
		SimilarityFunctions sdf = new SimilarityFunctions();
		ISelectWrapper sjsw = Utility.processQuery(coreDB, dhmsmCapDataQuery);		
		Hashtable<String, Set<String>> aggregatedData = new Hashtable<String, Set<String>>();
		String[] vars = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();			
			String sub = sjss.getVar(vars[0]).toString();
			Set<String> pred = new HashSet<String>();
			String concat = sjss.getVar(vars[1]).toString()+"@"+sjss.getVar(vars[2]).toString();
			pred.add(concat);
			if (!aggregatedData.containsKey(sub))
				{aggregatedData.put(sub, pred);}
			else {aggregatedData.get(sub).add(concat);}				
		}	
		for (String key : aggregatedData.keySet()) 
		{
			key = key.replaceAll("\"", "");
			Hashtable dataScoreHash = new Hashtable();
			for (String data : aggregatedData.get(key)) 
			{
				double weight = Double.parseDouble(data.substring(data.indexOf("@")+1, data.length()));	
				weight = weight / 100;
				data = data.substring(0, data.indexOf("@"));
				dataScoreHash.put(data, weight);
			}
			dataHash.put(key, dataScoreHash);
		}
		
		addPanel();
		String comparisonType = "Data";
		logger.info("Creating " + comparisonType + " to System Heat Map.");
	
		allHash.put("xAxisTitle", "Capability");
		updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
		comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, capabilityQuery);
		setComparisonObjectTypes("Capability", "Data");
		updateProgressBar("35%...Querying Data", 35);
	
		sdf.setComparisonObjectList(comparisonObjectList);	

		updateProgressBar("50%...Evaluating Data Objects Created for a " + comparisonType, 50);
		logger.info("Finished Data Objects Created Processing.");		
		
		Hashtable dataScoreHash = processHashForCharting(dataHash);		
		
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
	
		paramDataHash.put("Data_Objects_Created", dataScoreHash);
		
		allHash.put("title",  "Systems Support " + comparisonType);		
		allHash.put("yAxisTitle", "Data");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
	}
}
