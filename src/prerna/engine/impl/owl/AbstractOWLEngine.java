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
package prerna.engine.impl.owl;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.jena.vocabulary.OWL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IExplorable;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.masterdatabase.utility.MetamodelVertex;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractOWLEngine implements IExplorable {

	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";

	// kept as a reference for the from/to neighbor traversal queries
//	private static final String FROM_SPARQL = "SELECT DISTINCT ?entity WHERE { "
//			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//			+ "{?x ?rel  ?y} " + "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
//			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}" + "}";
//
//	private static final String TO_SPARQL = "SELECT DISTINCT ?entity WHERE { "
//			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//			+ "{?x ?rel ?y} " + "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}" + "}";

	private static final Logger classLogger = LogManager.getLogger(AbstractOWLEngine.class);

	// predefined URIs
	public static final String SEMOSS_URI_PREFIX = "http://semoss.org/ontologies/";
	public static final String DEFAULT_NODE_CLASS = "Concept";
	public static final String DEFAULT_RELATION_CLASS = "Relation";
	public static final String DEFAULT_PROP_CLASS = "Relation/Contains";
	public static final String CONCEPTUAL_RELATION_NAME = "Conceptual";
	public static final String PIXEL_RELATION_NAME = "Pixel";
	public static final String ADDITIONAL_DATATYPE_NAME = "AdtlDataType";

	// since we keep making these URIs often
	public static final String BASE_NODE_URI = SEMOSS_URI_PREFIX + DEFAULT_NODE_CLASS;
	public static final String BASE_RELATION_URI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
	public static final String BASE_PROPERTY_URI = SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS;
	public static final String CONCEPTUAL_RELATION_URI = BASE_RELATION_URI + "/" + CONCEPTUAL_RELATION_NAME;
	public static final String PIXEL_RELATION_URI = BASE_RELATION_URI + "/" + PIXEL_RELATION_NAME;
	public static final String ADDITIONAL_DATATYPE_RELATION_URI = BASE_RELATION_URI + "/" + ADDITIONAL_DATATYPE_NAME;

	public static final String TIME_KEY = "ENGINE:TIME";
	public static final String TIME_URL = "http://semoss.org/ontologies/Concept/TimeStamp";

	// pre-bracketed <URI> fragments for embedding directly in SPARQL (typo-safe)
	protected static final String Q_SUBCLASS_OF = "<" + RDFS.SUBCLASSOF + ">";
	protected static final String Q_RDFS_DOMAIN = "<" + RDFS.DOMAIN + ">";
	protected static final String Q_RDF_TYPE = "<" + RDF.TYPE + ">";
	protected static final String Q_DATATYPE_PROPERTY = "<" + OWL.DatatypeProperty + ">";
	protected static final String Q_CONCEPT = "<" + BASE_NODE_URI + ">";
	protected static final String Q_RELATION = "<" + BASE_RELATION_URI + ">";
	protected static final String Q_CONTAINS = "<" + BASE_PROPERTY_URI + ">";
	protected static final String Q_PIXEL = "<" + PIXEL_RELATION_URI + ">";
	protected static final String Q_CONCEPTUAL = "<" + CONCEPTUAL_RELATION_URI + ">";

	@Deprecated
	public static final String LEGACY_PRIM_KEY_URI = BASE_RELATION_URI + "/" + "LEGACY_PRIM_KEY";

	protected RDFFileSesameEngine baseDataEngine = null;
	protected String engineId = null;
	protected String engineName = null;

	public AbstractOWLEngine(RDFFileSesameEngine baseDataEngine, String engineId, String engineName) {
		this.baseDataEngine = baseDataEngine;
		this.engineId = engineId;
		this.engineName = engineName;
	}

	/**
	 * 
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public IRawSelectWrapper query(String query) throws Exception {
		return WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, query);
	}

	/**
	 * 
	 * @param query
	 * @return
	 */
	public IConstructWrapper queryCW(String query) {
		return WrapperManager.getInstance().getCWrapper(this.baseDataEngine, query);
	}

	@Override
	public Vector<String> getFromNeighbors(String physicalNodeType, int neighborHood) {
		// this is where this node is the from node
		// String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI +
		// Utility.getInstanceName(nodeType), false);
		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <"
				+ RDFS.SUBPROPERTYOF + "> " + Q_RELATION + "} " + "{?node ?rel ?start}}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	public Vector<String[]> getFromNeighborsWithRelation(String physicalNodeType, int neighborHood) {
		// this is where this node is the from node
		// String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI +
		// Utility.getInstanceName(nodeType), false);
		String query = "SELECT DISTINCT ?node ?rel WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <"
				+ RDFS.SUBPROPERTYOF + "> " + Q_RELATION + "} " + "{?node ?rel ?start}}";
		return Utility.getVectorArrayOfReturn(query, baseDataEngine, true);
	}

	// gets the to nodes
	@Override
	public Vector<String> getToNeighbors(String physicalNodeType, int neighborHood) {
		// this is where this node is the to node
		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <"
				+ RDFS.SUBPROPERTYOF + "> " + Q_RELATION + "} " + "{?start ?rel ?node}}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	// gets the from and to nodes
	@Override
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		Vector<String> from = getFromNeighbors(nodeType, 0);
		Vector<String> to = getToNeighbors(nodeType, 0);
		from.addAll(to);
		return from;
	}

	@Override
	public String getOWLDefinition() {
		StringWriter output = new StringWriter();
		try {
			baseDataEngine.getRc().export(new RDFXMLWriter(output));
		} catch (RepositoryException re) {
			classLogger.error("Failed to export OWL definition from the base data engine", re);
		} catch (RDFHandlerException e) {
			classLogger.error("Failed to serialize OWL definition to RDF/XML", e);
		}
		return output.toString();
	}

	@Override
	public String getOwlFilePath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setOwlFilePath(String owlFilePath) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitOWL() {
		// TODO Auto-generated method stub

	}

	@Override
	public void addProperty(String key, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getProperty(String key) {
		return null;
	}

	@Override
	public Vector<String> executeInsightQuery(String sparqlQuery, boolean isDbQuery) {
		return null;
	}

	@Override
	public String getNodeBaseUri() {
		String baseUri = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine,
				GET_BASE_URI_FROM_OWL)) {
			if (wrapper.hasNext()) {
				IHeadersDataRow data = wrapper.next();
				baseUri = data.getRawValues()[0] + "";
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		if (baseUri == null) {
			baseUri = Constants.CONCEPT_URI;
		}
		return baseUri;
	}

	public Vector<String> getConcepts() {
		String query = "SELECT ?concept WHERE { {?concept " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }"
				+ " Filter(?concept != " + Q_CONCEPT + ") }";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	/**
	 * Runs a select query on the base data engine of this engine
	 */
	@Override
	public Object execOntoSelectQuery(String query) {
		classLogger.debug("Running select query on base data engine of {}", this.engineName);
		classLogger.debug("Query is {}", query);
		return this.baseDataEngine.execQuery(query);
	}

	/**
	 * Runs insert query on base data engine of this engine
	 */
	public void ontoInsertData(String query) {
		classLogger.debug("Running insert query on base data engine of {}", this.engineName);
		classLogger.debug("Query is {}", query);
		baseDataEngine.insertData(query);
	}

	/**
	 * This method runs an update query on the base data engine which contains all
	 * owl and metamodel information
	 */
	public void ontoRemoveData(String query) {
		classLogger.debug("Running update query on base data engine of {}", this.engineName);
		classLogger.debug("Query is {}", query);
		baseDataEngine.removeData(query);
	}

	@Override
	public String getDataTypes(String uri) {
		String query = "SELECT DISTINCT ?TYPE WHERE { {<" + uri + "> <" + RDFS.CLASS.toString() + "> ?TYPE} }";
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return null;
	}

	@Override
	public Map<String, String> getDataTypes(String... uris) {
		StringBuilder bindBuilder = new StringBuilder();
		for (String uri : uris) {
			bindBuilder.append("(<").append(uri).append(">)");
		}
		String query = "SELECT DISTINCT ?NODE ?TYPE WHERE { {?NODE <" + RDFS.CLASS.toString() + "> ?TYPE} } ";
		String bindings = bindBuilder.toString();
		if (!bindings.isEmpty()) {
			query += "BINDINGS ?NODE {" + bindings + "}";
		}
		// results to be stored
		Map<String, String> retMap = new HashMap<>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String node = row[0].toString();
				String type = row[1].toString();
				retMap.put(node, type);
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return retMap;
	}

	@Override
	public String getAdtlDataTypes(String uri) {
		String query = "SELECT DISTINCT ?ADTLTYPE WHERE { {<" + uri + "> <" + ADDITIONAL_DATATYPE_RELATION_URI
				+ "> ?ADTLTYPE} }";

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			if (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String adtlType = row.getValues()[0].toString().replace("ADTLTYPE:", "");
				return adtlType;
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return null;
	}

	@Override
	public Map<String, String> getAdtlDataTypes(String... uris) {
		StringBuilder bindBuilder = new StringBuilder();
		for (String uri : uris) {
			bindBuilder.append("(<").append(uri).append(">)");
		}
		String query = "SELECT DISTINCT ?NODE ?ADTLTYPE WHERE { {?NODE <" + ADDITIONAL_DATATYPE_RELATION_URI
				+ "> ?ADTLTYPE} } ";
		String bindings = bindBuilder.toString();
		if (!bindings.isEmpty()) {
			query += "BINDINGS ?NODE {" + bindings + "}";
		}
		// results to be stored
		Map<String, String> retMap = new HashMap<>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String node = row[0].toString();
				String type = row[1].toString();
				if (type != null && !type.equals("")) {
					type = type.replace("ADTLTYPE:", "");
					retMap.put(node, type);
				}
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return retMap;
	}

	@Override
	public Map<String, Object[]> getMetamodel() {
		// create this from the query struct
		Map<String, MetamodelVertex> tableToVert = new TreeMap<>();

		String getSelectorsInformation = "SELECT DISTINCT ?concept ?property WHERE { " + "{?concept " + Q_SUBCLASS_OF
				+ " " + Q_CONCEPT + " }" + "OPTIONAL {" + "{?property " + Q_RDF_TYPE + " <" + CONTAINS_BASE_URI + "> } "
				+ "{?concept <" + OWL.DatatypeProperty.toString() + "> ?property } " + "}" // END OPTIONAL
				+ "}"; // END WHERE

		// execute the query and loop through and add the nodes and props
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine,
				getSelectorsInformation)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow hrow = wrapper.next();
				Object[] raw = hrow.getRawValues();
				if (raw[0].toString().equals(BASE_NODE_URI)) {
					continue;
				}

				String concept = Utility.getInstanceName(raw[0].toString());
				Object property = raw[1];

				if (!tableToVert.containsKey(concept)) {
					MetamodelVertex vert = new MetamodelVertex(concept);
					tableToVert.put(concept, vert);
				}

				if (property != null && !property.toString().isEmpty()) {
					tableToVert.get(concept).addProperty(Utility.getClassName(property.toString()));
				}
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		List<Map<String, String>> relationships = new ArrayList<>();

		// query to get all the relationships
		String getRelationshipsInformation = "SELECT DISTINCT ?fromConceptualConcept ?rel ?toConceptualConcept WHERE { "
				+ "{?fromConcept " + Q_SUBCLASS_OF + " " + Q_CONCEPT + "} " + "{?toConcept " + Q_SUBCLASS_OF + " "
				+ Q_CONCEPT + "} " + "{?rel <" + RDFS.SUBPROPERTYOF.toString() + "> " + Q_RELATION + "} "
				+ "{?fromConcept ?rel ?toConcept} " + "{?fromConcept " + Q_CONCEPTUAL + " ?fromConceptualConcept }"
				+ "{?toConcept " + Q_CONCEPTUAL + " ?toConceptualConcept }" + "}"; // END
																					// WHERE

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine,
				getRelationshipsInformation)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow hrow = wrapper.next();
				Object[] row = hrow.getValues();

				if (hrow.getRawValues()[1].toString().equals(BASE_RELATION_URI)) {
					continue;
				}

				String fromConcept = row[0].toString();
				String rel = row[1].toString();
				String toConcept = row[2].toString();

				Map<String, String> edgeMap = new TreeMap<>();
				edgeMap.put("source", fromConcept);
				edgeMap.put("target", toConcept + "");
				edgeMap.put("rel", rel);
				relationships.add(edgeMap);
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		Map<String, Object[]> retObj = new HashMap<>();
		retObj.put("nodes", tableToVert.values().toArray());
		retObj.put("edges", relationships.toArray());
		return retObj;
	}

	@Override
	public File getOwlPositionFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return null;
	}

	@Override
	public boolean isBasic() {
		return true;
	}

	@Override
	public void setBasic(boolean isBasic) {
		// this is always basic - used only for OWL
	}

	/*
	 * NEW PIXEL TO REPLACE CONCEPTUAL NAMES
	 */

	@Override
	public List<String> getPixelConcepts() {
		String query = "SELECT ?pixelName WHERE {" + " {?concept " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }"
				+ " {?concept " + Q_PIXEL + " ?pixelName }" + " }";
		return Utility.getVectorOfReturn(query, baseDataEngine, false);
	}

	@Override
	public List<String> getPixelSelectors(String conceptPixelName) {
		// first grab the concept if it has data
		String query = "SELECT DISTINCT ?pixelName WHERE { " + " BIND(<" + BASE_NODE_URI + "/" + conceptPixelName
				+ "> as ?concept) " + " {?concept " + Q_PIXEL + " ?pixelName }" + " FILTER NOT EXISTS {?concept "
				+ Q_RDFS_DOMAIN + " \"noData\" }" + " }";
		// then grab the properties of the concept which always have data
		Vector<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, false);
		query = "SELECT DISTINCT ?pixelName WHERE { " + " BIND(<" + BASE_NODE_URI + "/" + conceptPixelName
				+ "> as ?concept) " + " {?concept " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " } " + " {?property "
				+ Q_RDF_TYPE + " " + Q_CONTAINS + "} " + " {?concept " + Q_DATATYPE_PROPERTY + " ?property} "
				+ " {?property " + Q_PIXEL + " ?pixelName}" + " }";
		Vector<String> pArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
		for (String p : pArr) {
			retArr.add(conceptPixelName + "__" + Utility.getClassName(p));
		}

		return retArr;
	}

	@Override
	public List<String> getPropertyPixelSelectors(String conceptPixelName) {
		// then grab the properties of the concept which always have data
		String query = "SELECT DISTINCT ?pixelName WHERE { " + " BIND(<" + BASE_NODE_URI + "/" + conceptPixelName
				+ "> as ?concept) " + " {?concept " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " } " + " {?property "
				+ Q_RDF_TYPE + " " + Q_CONTAINS + "} " + " {?concept " + Q_DATATYPE_PROPERTY + " ?property} "
				+ " {?property " + Q_PIXEL + " ?pixelName}" + " }";
		List<String> retArr = new ArrayList<>();
		Vector<String> pArr = Utility.getVectorOfReturn(query, baseDataEngine, false);
		for (String p : pArr) {
			retArr.add(conceptPixelName + "__" + p);
		}

		return retArr;
	}

	@Override
	public List<String> getPhysicalConcepts() {
		String query = "SELECT ?concept WHERE {" + "{?concept " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }"
				+ "Filter(?concept != " + Q_CONCEPT + ")" + "}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	@Override
	public List<String[]> getPhysicalRelationships() {
		String query = "SELECT DISTINCT ?start ?end ?rel WHERE { " + "{?start " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }"
				+ "{?end " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }" + "{?rel <" + RDFS.SUBPROPERTYOF + "> " + Q_RELATION
				+ "} " + "{?start ?rel ?end}" + "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)" + "Filter(?rel != "
				+ Q_RELATION + ")" + "}";
		return Utility.getVectorArrayOfReturn(query, baseDataEngine, true);
	}

	@Override
	public List<String> getPropertyUris4PhysicalUri(String physicalUri) {
		String query = "SELECT DISTINCT ?property WHERE { " + "BIND(<" + physicalUri + "> AS ?concept) " + "{?concept "
				+ Q_SUBCLASS_OF + " " + Q_CONCEPT + " } "
//					+ "{?property " + Q_RDF_TYPE + " " + Q_CONTAINS + "} "
				+ "{?concept " + Q_DATATYPE_PROPERTY + " ?property} " + "}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	@Override
	public String getPhysicalUriFromPixelSelector(String pixelSelector) {
		String semossConceptName = pixelSelector;
		String semossPropertyName = null;
		if (semossConceptName.contains("__")) {
			String[] split = pixelSelector.split("__");
			semossConceptName = split[0];
			semossPropertyName = split[1];

			// accounting if we are using the prim key placeholder
			if (semossPropertyName.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				semossPropertyName = null;
			}
		}

		String query = null;
		if (semossPropertyName == null) {
			// this is just a concept
			query = "SELECT DISTINCT ?concept WHERE { " + " BIND(<" + BASE_NODE_URI + "/" + semossConceptName
					+ "> as ?pixelName) " + " {?concept " + Q_PIXEL + " ?pixelName } " + " }";
		} else {
			// this is a property
			query = "SELECT DISTINCT ?property WHERE { " + " BIND(<" + BASE_PROPERTY_URI + "/" + semossPropertyName
					+ "/" + semossConceptName + "> as ?pixelName) " + " {?property " + Q_PIXEL + " ?pixelName } "
					+ " }";
		}

		List<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
		if (!retArr.isEmpty()) {
			return retArr.get(0);
		}
		return null;
	}

	@Override
	@Deprecated
	/**
	 * We cannot use this cause of the fact that we have not updated the OWL triples
	 * for a RDF engine for the properties to contain the Concept in the URL (which
	 * would make it unique) Example: Right now we have
	 * http://semoss.org/ontologies/Relation/Contains/Description as a property
	 * which could point to multiple concepts
	 */
	public String getPixelUriFromPhysicalUri(String physicalUri) {
		String query = "SELECT DISTINCT ?pixel WHERE { " + " BIND(<" + physicalUri + "> as ?physicalUri) "
				+ " {?physicalUri " + Q_PIXEL + " ?pixel } " + " }";
		List<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
		if (!retArr.isEmpty()) {
			if (retArr.size() > 1) {
				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! {}", physicalUri);
			}
			return retArr.get(0);
		}
		return null;
	}

	@Override
	public String getConceptPixelUriFromPhysicalUri(String conceptPhysicalUri) {
		String query = "SELECT DISTINCT ?pixel WHERE { " + " BIND(<" + conceptPhysicalUri + "> as ?physicalUri) "
				+ " {?physicalUri " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }" + " {?physicalUri " + Q_PIXEL
				+ " ?pixel } " + " }";
		List<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
		if (!retArr.isEmpty()) {
			if (retArr.size() > 1) {
				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! {}", conceptPhysicalUri);
			}
			return retArr.get(0);
		}
		return null;
	}

	@Override
	/**
	 * This is so annoying... no simple work around for the issue with
	 * {@link #getPixelUriFromPhysicalUri(String)}
	 */
	public String getPropertyPixelUriFromPhysicalUri(String conceptPhysicalUri, String propertyPhysicalUri) {
		String query = "SELECT DISTINCT ?pixel ?parentPixel WHERE { " + " BIND(<" + propertyPhysicalUri
				+ "> as ?propertyPhysicalUri) " + " BIND(<" + conceptPhysicalUri + "> as ?conceptPhysicalUri) "
				+ " {?conceptPhysicalUri " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }" + " {?conceptPhysicalUri " + Q_PIXEL
				+ " ?parentPixel } " + " {?propertyPhysicalUri " + Q_RDF_TYPE + " " + Q_CONTAINS + "} "
				+ "	{?conceptPhysicalUri " + Q_DATATYPE_PROPERTY + " ?propertyPhysicalUri} " + " {?propertyPhysicalUri "
				+ Q_PIXEL + " ?pixel } " + " }";

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			String conceptName = null;
			if (wrapper.hasNext()) {
				Object[] raw = wrapper.next().getRawValues();
				String propPixel = raw[0].toString();
				String parentPixel = raw[1].toString();
				conceptName = Utility.getInstanceName(parentPixel);
				if (Utility.getInstanceName(propPixel).equals(conceptName)) {
					return propPixel;
				}
			}
			classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! {} ::: {}", conceptPhysicalUri,
					propertyPhysicalUri);
			while (wrapper.hasNext()) {
				Object[] raw = wrapper.next().getRawValues();
				String propPixel = raw[0].toString();
				if (Utility.getInstanceName(propPixel).equals(conceptName)) {
					return propPixel;
				}
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}
		return null;
	}

	@Override
	public String getPixelSelectorFromPhysicalUri(String physicalUri) {
		String query = "SELECT DISTINCT ?pixel ?type WHERE { " + " {" + " BIND(\"concept\" as ?type) " + " BIND(<"
				+ physicalUri + "> as ?physicalUri) " + " {?physicalUri " + Q_SUBCLASS_OF + " " + Q_CONCEPT + " }"
				+ " {?physicalUri " + Q_PIXEL + " ?pixel } " + " }" + " UNION " + "	{" + " BIND(\"property\" as ?type) "
				+ " BIND(<" + physicalUri + "> as ?physicalUri) " + " {?physicalUri " + Q_RDF_TYPE + " " + Q_CONTAINS
				+ " }" + " {?physicalUri " + Q_PIXEL + " ?pixel } " + "	}" + " }";

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			while (wrapper.hasNext()) {
				Object[] raw = wrapper.next().getRawValues();
				if (raw[1].toString().contains("concept")) {
					return Utility.getInstanceName(raw[0].toString());
				} else {
					String parent = Utility.getInstanceName(raw[0].toString());
					String child = Utility.getClassName(raw[0].toString());
					return parent + "__" + child;
				}
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return null;
	}

	@Override
	public String getConceptualName(String physicalUri) {
		String query = "SELECT DISTINCT ?conceptual WHERE { " + "BIND(<" + physicalUri + "> AS ?uri) " + "{?uri <"
				+ CONCEPTUAL_RELATION_URI + "> ?conceptual } " + "}";

		String conceptualName = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			if (wrapper.hasNext()) {
				conceptualName = wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}
		return conceptualName;
	}

	@Override
	public Set<String> getLogicalNames(String physicalUri) {
		String query = "SELECT DISTINCT ?logical WHERE { " + "BIND(<" + physicalUri + "> AS ?uri) " + "{?uri <"
				+ OWL.sameAs.toString() + "> ?logical } " + "}";

		Set<String> logicals = new TreeSet<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			while (wrapper.hasNext()) {
				logicals.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return logicals;
	}

	@Override
	public String getDescription(String physicalUri) {
		String query = "SELECT DISTINCT ?description WHERE { " + "BIND(<" + physicalUri + "> AS ?uri) " + "{?uri <"
				+ RDFS.COMMENT.toString() + "> ?description } " + "}";

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return null;
	}

	@Override
	@Deprecated
	public String getLegacyPrimKey4Table(String physicalUri) {
		String query = "SELECT DISTINCT ?value WHERE { " + "BIND(<" + physicalUri + "> AS ?uri) " + "{?uri <"
				+ LEGACY_PRIM_KEY_URI + "> ?value } " + "}";

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query)) {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			classLogger.error("Error executing OWL query on engine {}", this.engineName, e);
		}

		return null;
	}
}