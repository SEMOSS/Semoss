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

import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.query.interpreters.IQueryInterpreter;

/**
 * Interface for engines that support exploration and metadata operations.
 * 
 * <p>This interface provides comprehensive functionality for exploring database
 * schemas, relationships, and metadata. It enables engines to expose their
 * structure and relationships in a standardized way, supporting operations
 * such as schema discovery, relationship mapping, and semantic data exploration.</p>
 * 
 * <p>Key capabilities include:</p>
 * <ul>
 *   <li><strong>Schema Exploration:</strong> Discover concepts, properties, and relationships</li>
 *   <li><strong>Metadata Management:</strong> Access and manage semantic metadata via OWL</li>
 *   <li><strong>Query Support:</strong> Execute ontology and data queries</li>
 *   <li><strong>URI Management:</strong> Convert between physical, conceptual, and pixel URIs</li>
 *   <li><strong>Relationship Discovery:</strong> Navigate graph relationships and neighborhoods</li>
 * </ul>
 * 
 * <p>The interface supports multiple URI namespaces:</p>
 * <ul>
 *   <li><strong>Physical URI:</strong> Direct database identifiers (tables, columns)</li>
 *   <li><strong>Conceptual URI:</strong> Semantic identifiers from ontology</li>
 *   <li><strong>Pixel URI:</strong> User-friendly display names</li>
 * </ul>
 * 
 * @see {@link IDatabaseEngine} for database operations
 * @see {@link RDFFileSesameEngine} for RDF/OWL metadata management
 * @see {@link IQueryInterpreter} for query processing
 * @author SEMOSS
 */
public interface IExplorable {
	
	/**
	 * Gets the incoming neighbors for a given node type within a specified distance.
	 * 
	 * <p>This method discovers all node types that have outgoing relationships
	 * pointing to the specified node type, within the given neighborhood distance.
	 * In graph terms, these are the source nodes of edges pointing to the target.</p>
	 * 
	 * @param nodeType The target node type to find neighbors for
	 * @param neighborHood The maximum distance to search for neighbors
	 * @return Vector of node type names that connect to the specified node type
	 */
	Vector<String> getFromNeighbors(String nodeType, int neighborHood);
	
	/**
	 * Gets the outgoing neighbors for a given node type within a specified distance.
	 * 
	 * <p>This method discovers all node types that the specified node type has
	 * outgoing relationships to, within the given neighborhood distance.
	 * In graph terms, these are the target nodes of edges originating from the source.</p>
	 * 
	 * @param nodeType The source node type to find neighbors for
	 * @param neighborHood The maximum distance to search for neighbors
	 * @return Vector of node type names that the specified node type connects to
	 */
	Vector<String> getToNeighbors(String nodeType, int neighborHood);
	
	/**
	 * Gets all neighbors (both incoming and outgoing) for a given node type.
	 * 
	 * <p>This method combines the results of both {@link #getFromNeighbors(String, int)}
	 * and {@link #getToNeighbors(String, int)} to provide a complete view of all
	 * connected node types within the specified neighborhood distance.</p>
	 * 
	 * @param nodeType The node type to find all neighbors for
	 * @param neighborHood The maximum distance to search for neighbors
	 * @return Vector of all connected node type names (both incoming and outgoing)
	 */
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
	
	/**
	 * 
	 * @return
	 */
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
	
	/**
	 * 
	 * @param uri
	 * @return
	 */
	String getAdtlDataTypes(String uri);
	
	/**
	 * 
	 * @param uris
	 * @return
	 */
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
