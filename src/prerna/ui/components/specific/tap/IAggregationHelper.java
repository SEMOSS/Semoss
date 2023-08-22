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
package prerna.ui.components.specific.tap;

import java.util.HashMap;

import prerna.engine.api.IDatabaseEngine;

public interface IAggregationHelper {

	String semossConceptBaseURI = "http://semoss.org/ontologies/Concept/";
	String semossRelationBaseURI = "http://semoss.org/ontologies/Relation/";
	String semossPropertyBaseURI = "http://semoss.org/ontologies/Relation/Contains/";
	
	// processing and aggregating methods
	
	void processData(IDatabaseEngine engine, HashMap<String, HashMap<String, Object>> data);
	
	void deleteData(IDatabaseEngine engine, HashMap<String, HashMap<String, Object>> data);
	
	void processAllRelationshipSubpropTriples(IDatabaseEngine engine);
	
	void processAllConceptTypeTriples(IDatabaseEngine engine);
	
	void processNewConcepts(IDatabaseEngine engine, String newConceptType);
	
	void processNewRelationships(IDatabaseEngine engine, String newRelationshipType);
	
	void processNewConceptsAtInstanceLevel(IDatabaseEngine engine, String subject, String object);
	
	void processNewRelationshipsAtInstanceLevel(IDatabaseEngine engine, String subject, String object); 
	
	void addToDataHash(Object[] returnTriple);
	
	void addToDeleteHash(Object[] returnTriple);
	
	void addToAllConcepts(String uri);
	
	void addToAllRelationships(String uri);
	
	void writeToOWL(IDatabaseEngine engine);
	
	// utility methods
	Object[] processSumValues(String sub, String prop, Object value);
	
	Object[] processConcatString(String sub, String prop, Object value); 
	
	Object[] processMaxMinDouble(String sub, String prop, Object value, boolean max);
	
	Object[] processMinMaxDate(String sub, String prop, Object value, Boolean latest);
	



	

}
