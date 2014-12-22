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
package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;

public interface IAggregationHelper {

	String semossConceptBaseURI = "http://semoss.org/ontologies/Concept/";
	String semossRelationBaseURI = "http://semoss.org/ontologies/Relation/";
	String semossPropertyBaseURI = "http://semoss.org/ontologies/Relation/Contains/";
	
	HashMap<String, HashMap<String, Object>> dataHash = new HashMap<String, HashMap<String, Object>>();
	HashMap<String, HashMap<String, Object>> removeDataHash = new HashMap<String, HashMap<String, Object>>();

	HashMap<String, Set<String>> allRelations = new HashMap<String, Set<String>>();
	HashMap<String, Set<String>> allConcepts = new HashMap<String, Set<String>>();
	
	HashMap<String, String> allLabels = new HashMap<String, String>();
	
	// processing and aggregating methods
	
	void processData(IEngine engine, HashMap<String, HashMap<String, Object>> data);
	
	void deleteData(IEngine engine, HashMap<String, HashMap<String, Object>> data);
	
	void processAllRelationshipSubpropTriples(IEngine engine);
	
	void processAllConceptTypeTriples(IEngine engine);
	
	void processNewConcepts(IEngine engine, String newConceptType);
	
	void processNewRelationships(IEngine engine, String newRelationshipType);
	
	void processNewConceptsAtInstanceLevel(IEngine engine, String subject, String object);
	
	void processNewRelationshipsAtInstanceLevel(IEngine engine, String subject, String object); 
	
	void addToDataHash(Object[] returnTriple);
	
	void addToDeleteHash(Object[] returnTriple);
	
	void addToAllConcepts(String uri);
	
	void addToAllRelationships(String uri);
	
	void writeToOWL(IEngine engine);
	
	// utility methods
	Object[] processSumValues(String sub, String prop, Object value);
	
	Object[] processConcatString(String sub, String prop, Object value); 
	
	Object[] processMaxMinDouble(String sub, String prop, Object value, boolean max);
	
	Object[] processMinMaxDate(String sub, String prop, Object value, Boolean latest);
	



	

}
