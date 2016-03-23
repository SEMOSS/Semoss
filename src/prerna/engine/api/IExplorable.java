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
package prerna.engine.api;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.query.builder.IQueryBuilder;

public interface IExplorable {
	
	// gets the perspectives for this engine
	Vector<String> getPerspectives();
	
	// gets the questions for a given perspective
	Vector<String> getInsights(String perspective);
	
	// get all the insights irrespective of perspective
	Vector<String> getInsights();

	// get the insight for a given question description
	Vector<Insight> getInsight(String... id);
	
	// gets insights for a given type of entity
	Vector<String> getInsight4Type(String type);
	
	// get insights for a tag
	Vector<String> getInsight4Tag(String tag) ;
	
	// gets the from neighborhood for a given node
	Vector<String> getFromNeighbors(String nodeType, int neighborHood);
	
	// gets the to nodes
	Vector<String> getToNeighbors(String nodeType, int neighborHood);
	
	// gets the from and to nodes
	Vector<String> getNeighbors(String nodeType, int neighborHood);
	
	// gets all the params
	Vector<SEMOSSParam> getParams(String insightName);

	// sets the dreamer properties file
	void setDreamer(String dreamer);
	
	// sets the ontology file
//	void setOntology(String ontologyFile);
	
	// sets the owl
	void setOWL(String owl);

	// gets the owl definitions
	String getOWLDefinition();

	// commits the OWL
	void commitOWL();
	
	// adds property to be associated with explorable
	void addProperty(String key, String value);
	
	// get property
	String getProperty(String key);
	
	// gets the param values for a parameter
	Vector<Object> getParamOptions(String parameterURI);

	// gets the query builder
	IQueryBuilder getQueryBuilder();
	
	// gets a vector of all concepts that exist
	Vector<String> getConcepts();
	
	// gets all of the properties for a given concept
	List<String> getProperties4Concept(String concept, Boolean logicalNames);
	
	// executes a query on the ontology engine
	Object execOntoSelectQuery(String query);
	
	// executes an insert query on the ontology engine
	void ontoInsertData(String query);
	
	// executes a remove query on the onotology engine
	void ontoRemoveData(String query);

	IEngine getInsightDatabase();

	List<Map<String, Object>> getAllInsightsMetaData();
	
	String getInsightDefinition();
	
	Vector<SEMOSSParam> getParams(String... paramIds);
	
	// gets the display or physical for a given concept
	String getTransformedNodeName(String concept, boolean getDisplayName);
	
	//loads the logical and physical names
	void loadTransformedNodeNames();

	Vector<String> executeInsightQuery(String sparqlQuery, boolean isDbQuery);
	
	String getNodeBaseUri();
}
