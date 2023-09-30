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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.query.interpreters.IQueryInterpreter;

public interface IExplorable {
	
	// gets the from neighborhood for a given node
	Vector<String> getFromNeighbors(String nodeType, int neighborHood);
	
	// gets the to nodes
	Vector<String> getToNeighbors(String nodeType, int neighborHood);
	
	// gets the from and to nodes
	Vector<String> getNeighbors(String nodeType, int neighborHood);
	
	String getOwlFilePath();
	
	// sets the owl
	void setOwlFilePath(String owlFilePath);
	
	// get the position file used to paint the metamodel
	File getOwlPositionFile();
	
	boolean isBasic();

	void setBasic(boolean isBasic);
	
	// gets the owl definitions
	String getOWLDefinition();

	/**
	 * Get the OWL engine
	 * @return
	 */
	RDFFileSesameEngine getBaseDataEngine();
	
	/**
	 * Set the owl engine
	 * @param baseDataEngine
	 */
	void setBaseDataEngine(RDFFileSesameEngine baseDataEngine);
	
	/**
	 * Commit the owl engine and write to disk
	 */
	void commitOWL();
	
	// adds property to be associated with explorable
	// REFAC: Check
	void addProperty(String key, String value);
	
	// get property
	String getProperty(String key);
	
	/**
	 * Get the query struct associated with the engine
	 * @return
	 */
	IQueryInterpreter getQueryInterpreter();
	
	/**
	 * Returns the set of properties for a given concept
	 * @param concept					The concept URI
	 * 									Assumes the concept URI is the conceptual URI
	 * @param conceptualNames			Boolean to determine if the return should be the properties
	 * 									conceptual names or physical names
	 * @return							List containing the property URIs for the given concept
	 */
//	List<String> getProperties4Concept(String conceptPhysicalUri, Boolean conceptualNames);

	// executes a query on the ontology engine
	// REFAC: Change this to engine
	Object execOntoSelectQuery(String query);
	
	/**
	 * Generate an audit database
	 */
	AuditDatabase generateAudit();
	
//	/**
//	 * Get the physical URI from the conceptual URI
//	 * @param conceptualURI			The conceptual URI
//	 * 								If it is not a valid URI, we will assume it is the instance_name and create the URI
//	 * @return						Return the physical URI 					
//	 */
//	// REFAC: Change this to engine - this should be local master
//	String getConceptPhysicalUriFromConceptualUri(String conceptualURI);
//	
//	// REFAC: Change this to engine - this should be local master
//	String getPropertyPhysicalUriFromConceptualUri(String conceptualURI, String parentConceptualUri);
//	
//	/**
//	 * Get the conceptual URI from the physical URI
//	 * @param physicalURI			The physical URI
//	 * 								If it is not a valid URI, we will assume it is the instance_name and create the URI
//	 * @return						Return the conceptual URI 					
//	 */
//	// REFAC: Change this to engine - this should be local master
//	String getConceptualUriFromPhysicalUri(String physicalURI);

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
//	String getParentOfProperty(String property);
	
	// REFAC: Change this to engine - this should be local master
	Map<String, Object[]> getMetamodel();
	
	//////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Okay, trying to make a new set of functions that should hopefully replace a lot of the ones 
	 * that are currently required
	 * Note that the physical URI and the Pixel URI are always unique
	 * within an app
	 */
	
	/**
	 * Get the list of concepts/tables in a given engine
	 * @return
	 */
	List<String> getPixelConcepts();
	
	/**
	 * Get the list of selectors for a given concept/table in TABLE__COLUMN format 
	 * This will include the TABLE if it contains data (i.e. RDF/Graph but not RDBMS)
	 * If you only want the properties, please refer to {@link #getPropertyPixelSelectors(String)}
	 * @param conceptPixelName
	 * @return
	 */
	List<String> getPixelSelectors(String conceptPixelName);

	/**
	 * Get the list of property selectors for a given concept/table in TABLE format
	 * If you want the selectors including the concept (assuming it has data),
	 * please refer to {@link #getPixelSelectors(String)}
	 * @param conceptPixelName
	 * @return
	 */
	List<String> getPropertyPixelSelectors(String conceptPixelName);
	
	/**
	 * Returns the list of physical concept URIs
	 * @return
	 */
	List<String> getPhysicalConcepts();
	
	/**
	 * Get the list of relationships
	 * @return
	 */
	List<String[]> getPhysicalRelationships();
	
	/**
	 * Get the property URIs for a physical concept URI
	 * @param physicalUri
	 * @return
	 */
	List<String> getPropertyUris4PhysicalUri(String physicalUri);

	/**
	 * Get the physical URI based on the pixel selector
	 * The pixel selector input will be in TABLE__COLUMN format
	 * @param pixelSelector
	 * @return
	 */
	String getPhysicalUriFromPixelSelector(String pixelSelector);
	
	/**
	 * Get the pixel URI from the physical URI
	 * 
	 * 
	 * We cannot use this cause of the fact that we have not updated the OWL triples
	 * for a RDF engine for the properties to contain the Concept in the URL (which would make it unique)
	 * Example: Right now we have http://semoss.org/ontologies/Relation/Contains/Description as a 
	 * property which could point to multiple concepts
	 * 
	 * @param physicalUri
	 * @return
	 */
	@Deprecated
	String getPixelUriFromPhysicalUri(String physicalUri);
	
	/**
	 * Get the pixel URI from the concept physical URI
	 * @param conceptPysicalUri
	 * @return
	 */
	String getConceptPixelUriFromPhysicalUri(String conceptPhysicalUri);
	
	/**
	 * Get the pixel URI from the concept and property physical URIs
	 * @param conceptPhysicalUri
	 * @param propertyPhysicalUri
	 * @return
	 */
	String getPropertyPixelUriFromPhysicalUri(String conceptPhysicalUri, String propertyPhysicalUri);
	
	/**
	 * Get the pixel selector in TABLE__COLUMN format from the physical URI
	 * @param physicalUri
	 */
	String getPixelSelectorFromPhysicalUri(String physicalUri);

	/**
	 * Get the conceptual name for the physical URI
	 * @param physicalUri
	 * @return
	 */
	String getConceptualName(String physicalUri);

	/**
	 * Get the logical names for the physical URI
	 * @param physicalUri
	 * @return
	 */
	Set<String> getLogicalNames(String physicalUri);

	/**
	 * Get the description for the physical URI
	 * @param physicalUri
	 * @return
	 */
	String getDescription(String physicalUri);
	
	/**
	 * Get the primary key for table
	 * This is for legacy pixels where we use TABLE without specifying the column
	 * in RDBMS engines
	 * @param physicalUri
	 * @return
	 */
	@Deprecated
	String getLegacyPrimKey4Table(String physicalUri);
	
}
