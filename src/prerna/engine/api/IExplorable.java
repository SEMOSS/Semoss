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
import java.util.Set;
import java.util.Vector;

import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;

public interface IExplorable {
	
	// gets the perspectives for this engine
	// REFAC: Not sure we need this anymore
	Vector<String> getPerspectives();
	
	// gets the questions for a given perspective
	// REFAC: Not sure we need this anymore
	Vector<String> getInsights(String perspective);
	
	// get all the insights irrespective of perspective
	// REFAC: Not sure we need this anymore
	Vector<String> getInsights();

	// get the insight for a given question description
	// REFAC: Not sure we need this anymore - we can do this where id is null
	Vector<Insight> getInsight(String... id);
	
	// gets the from neighborhood for a given node
	Vector<String> getFromNeighbors(String nodeType, int neighborHood);
	
	// gets the to nodes
	Vector<String> getToNeighbors(String nodeType, int neighborHood);
	
	// gets the from and to nodes
	Vector<String> getNeighbors(String nodeType, int neighborHood);
	
	// sets the owl
	void setOWL(String owl);
	
	String getOWL();
	
	boolean isBasic();

	void setBasic(boolean isBasic);
	
	// gets the owl definitions
	String getOWLDefinition();

	/**
	 * Get the OWL engine
	 * @return
	 */
	RDFFileSesameEngine getBaseDataEngine();
	
	void setBaseDataEngine(RDFFileSesameEngine baseDataEngine);
	
	// commits the OWL
	void commitOWL();
	
	// adds property to be associated with explorable
	// REFAC: Check
	void addProperty(String key, String value);
	
	// get property
	String getProperty(String key);
	
	IQueryInterpreter getQueryInterpreter();
	
	/**
	 * Returns the list of concepts as defined by the OWL file
	 * @param conceptualNames 	boolean to return the conceptual URI or physical URI
	 * @return
	 */
	Vector<String> getConcepts(boolean conceptualNames);

	Vector<String[]> getRelationships(boolean conceptualNames);
	
	/**
	 * Returns the set of properties for a given concept
	 * @param concept					The concept URI
	 * 									Assumes the concept URI is the conceptual URI
	 * @param conceptualNames			Boolean to determine if the return should be the properties
	 * 									conceptual names or physical names
	 * @return							List containing the property URIs for the given concept
	 */
	List<String> getProperties4Concept(String conceptPhysicalUri, Boolean conceptualNames);

	// executes a query on the ontology engine
	// REFAC: Change this to engine
	Object execOntoSelectQuery(String query);
	
	IEngine getInsightDatabase();

	void setInsightDatabase(IEngine insightDatabase);
	
	String getInsightDefinition();
	
	/**
	 * Get the physical URI from the conceptual URI
	 * @param conceptualURI			The conceptual URI
	 * 								If it is not a valid URI, we will assume it is the instance_name and create the URI
	 * @return						Return the physical URI 					
	 */
	// REFAC: Change this to engine - this should be local master
	String getConceptPhysicalUriFromConceptualUri(String conceptualURI);
	
	// REFAC: Change this to engine - this should be local master
	String getPropertyPhysicalUriFromConceptualUri(String conceptualURI, String parentConceptualUri);
	
	/**
	 * Get the conceptual URI from the physical URI
	 * @param physicalURI			The physical URI
	 * 								If it is not a valid URI, we will assume it is the instance_name and create the URI
	 * @return						Return the conceptual URI 					
	 */
	// REFAC: Change this to engine - this should be local master
	String getConceptualUriFromPhysicalUri(String physicalURI);

	// WHAT IS THIS ?
	Vector<String> executeInsightQuery(String sparqlQuery, boolean isDbQuery);
	
	String getNodeBaseUri();
	
	/**
	 * Get the datatypes for the uri from the associated owl file
	 * @param uris
	 * @return
	 */
	// REFAC: Change this to engine - this should be local master
	String getDataTypes(String uri);
	
	/**
	 * Get the datatypes for the uris from the associated owl file
	 * If varargs param is empty, it will return all data types
	 * @param uris
	 * @return
	 */
	// REFAC: Change this to engine - this should be local master
	Map<String, String> getDataTypes(String... uris);
	
	String getAdtlDataTypes(String uri);
	
	Map<String, String> getAdtlDataTypes(String... uris);

	// REFAC: this has no meaning.. sorry
	String getParentOfProperty(String property);
	
	SelectQueryStruct getDatabaseQueryStruct();
	
	// REFAC: Change this to engine - this should be local master
	Map<String, Object[]> getMetamodel();

	Set<String> getLogicalNames(String physicalURI);

	Set<String> getDescriptions(String physicalURI);

}
