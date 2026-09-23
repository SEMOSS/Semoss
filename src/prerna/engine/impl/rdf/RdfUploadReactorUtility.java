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
package prerna.engine.impl.rdf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.Dataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;

import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.engine.api.ISesameRDFEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class RdfUploadReactorUtility {

	private static final Logger classLogger = LogManager.getLogger(RdfUploadReactorUtility.class.getName());

	private RdfUploadReactorUtility() {

	}

	/**
	 * 
	 * @param engine
	 * @param owlEngine
	 */
	public static void loadMetadataIntoEngine(IRDFDatabase engine, WriteOWLEngine owlEngine) {
		List<Object[]> allStatementInserts = new ArrayList<>();

		Map<String, String> hash = owlEngine.getConceptHash();
		String object = AbstractOWLEngine.SEMOSS_URI_PREFIX + AbstractOWLEngine.DEFAULT_NODE_CLASS;
		for (String concept : hash.keySet()) {
			allStatementInserts.add(new Object[] { hash.get(concept), RDFS.SUBCLASSOF + "", object, true });
		}
		hash = owlEngine.getRelationHash();
		object = AbstractOWLEngine.SEMOSS_URI_PREFIX + AbstractOWLEngine.DEFAULT_RELATION_CLASS;
		for (String relation : hash.keySet()) {
			allStatementInserts.add(new Object[] { hash.get(relation), RDFS.SUBPROPERTYOF + "", object, true });
		}
		hash = owlEngine.getPropHash();
		object = AbstractOWLEngine.SEMOSS_URI_PREFIX + AbstractOWLEngine.DEFAULT_PROP_CLASS;
		for (String prop : hash.keySet()) {
			allStatementInserts.add(new Object[] { hash.get(prop), RDF.TYPE + "", object, true });
		}

		engine.bulkInsert(allStatementInserts);
	}

	/**
	 * Create and add all triples associated with relationship tabs
	 * 
	 * @param owlEngine
	 * @param baseUri
	 * @param subjectNodeType     String containing the subject node type
	 * @param objectNodeType      String containing the object node type
	 * @param instanceSubjectName String containing the name of the subject instance
	 * @param instanceObjectName  String containing the name of the object instance
	 * @param relName             String containing the name of the relationship
	 *                            between the subject and object
	 * @param propHash            Hashtable that contains all properties
	 * @param allStatementInserts Store all the triples in this list
	 */
	public static void createRelationship(WriteOWLEngine owlEngine, String baseUri, String subjectNodeType,
			String objectNodeType, String instanceSubjectName, String instanceObjectName, String relName,
			Hashtable<String, Object> propHash, List<Object[]> allStatementInserts) {
		subjectNodeType = Utility.cleanString(subjectNodeType, true);
		objectNodeType = Utility.cleanString(objectNodeType, true);

		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);
		instanceObjectName = Utility.cleanString(instanceObjectName, true);

		// get base URIs for subject node at instance and semoss level
		String subjectSemossBaseURI = owlEngine.addConcept(subjectNodeType, "STRING");
		String subjectInstanceBaseURI = getInstanceURI(baseUri, subjectNodeType);

		// get base URIs for object node at instance and semoss level
		String objectSemossBaseURI = owlEngine.addConcept(objectNodeType, "STRING");
		String objectInstanceBaseURI = getInstanceURI(baseUri, objectNodeType);

		// create the full URI for the subject instance
		// add type and label triples to database
		String subjectNodeURI = subjectInstanceBaseURI + "/" + instanceSubjectName;
		allStatementInserts.add(new Object[] { subjectNodeURI, RDF.TYPE, subjectSemossBaseURI, true });
		allStatementInserts.add(new Object[] { subjectNodeURI, RDFS.LABEL, instanceSubjectName, false });

		// create the full URI for the object instance
		// add type and label triples to database
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName;
		allStatementInserts.add(new Object[] { objectNodeURI, RDF.TYPE, objectSemossBaseURI, true });
		allStatementInserts.add(new Object[] { objectNodeURI, RDFS.LABEL, instanceObjectName, false });

		// generate URIs for the relationship
		relName = Utility.cleanPredicateString(relName);
		String relSemossBaseURI = owlEngine.addRelation(subjectNodeType, objectNodeType, relName);
		String relInstanceBaseURI = getRelBaseURI(baseUri, relName);

		// create instance value of relationship and add instance relationship,
		// subproperty, and label triples
		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR
				+ instanceObjectName;
		allStatementInserts.add(new Object[] { instanceRelURI, RDFS.SUBPROPERTYOF, relSemossBaseURI, true });
		allStatementInserts.add(new Object[] { instanceRelURI, RDFS.LABEL,
				instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName, false });
		allStatementInserts.add(new Object[] { subjectNodeURI, instanceRelURI, objectNodeURI, true });

		addProperties(owlEngine, "", instanceRelURI, propHash, allStatementInserts);
	}

	/**
	 * 
	 * @param owlEngine
	 * @param baseUri
	 * @param nodeType
	 * @param instanceName
	 * @param propHash
	 * @param allStatementInserts
	 */
	public static void addNodeProperties(WriteOWLEngine owlEngine, String baseUri, String nodeType, String instanceName,
			Hashtable<String, Object> propHash, List<Object[]> allStatementInserts) {
		// create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		nodeType = Utility.cleanString(nodeType, true);
		String semossBaseURI = owlEngine.addConcept(nodeType);
		String instanceBaseURI = getInstanceURI(baseUri, nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		allStatementInserts.add(new Object[] { subjectNodeURI, RDF.TYPE, semossBaseURI, true });
		allStatementInserts.add(new Object[] { subjectNodeURI, RDFS.LABEL, instanceName, false });
		addProperties(owlEngine, nodeType, subjectNodeURI, propHash, allStatementInserts);
	}

	/**
	 * 
	 * @param owlEngine
	 * @param subjectNodeType
	 * @param instanceURI
	 * @param propHash
	 * @param allStatementInserts
	 */
	private static void addProperties(WriteOWLEngine owlEngine, String subjectNodeType, String instanceURI,
			Hashtable<String, Object> propHash, List<Object[]> allStatementInserts) {
		// add all properties
		Enumeration<String> propKeys = propHash.keys();

		String basePropURI = getBasePropURI();
		// add property triple based on data type of property
		while (propKeys.hasMoreElements()) {
			String key = propKeys.nextElement().toString();
			String propURI = basePropURI + "/" + Utility.cleanString(key, true);
			// logger.info("Processing Property " + key + " for " + instanceURI);
			allStatementInserts.add(new Object[] { propURI, RDF.TYPE, basePropURI, true });
			if (propHash.get(key) instanceof Number) {
				Double value = ((Number) propHash.get(key)).doubleValue();
				// logger.info("Processing Double value " + value);
				allStatementInserts.add(new Object[] { instanceURI, propURI, value.doubleValue(), false });
				if (subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "DOUBLE");
				}
			} else if (propHash.get(key) instanceof SemossDate) {
				ZonedDateTime value = ((SemossDate) propHash.get(key)).getZonedDateTime();
				Date date = new Date(value.toInstant().toEpochMilli());
				allStatementInserts.add(new Object[] { instanceURI, propURI, date, false });
				if (subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "DATE");
				}
			} else if (propHash.get(key) instanceof Boolean) {
				Boolean value = (Boolean) propHash.get(key);
				// logger.info("Processing Boolean value " + value);
				allStatementInserts.add(new Object[] { instanceURI, propURI, value.booleanValue(), false });
				if (subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "BOOLEAN");
				}
			} else {
				String value = propHash.get(key).toString();
				if (value.equals(Constants.PROCESS_CURRENT_DATE)) {
					// logger.info("Processing Current Date Property");
					insertCurrentDate(propURI, basePropURI, instanceURI, allStatementInserts);
				} else if (value.equals(Constants.PROCESS_CURRENT_USER)) {
					// logger.info("Processing Current User Property");
					insertCurrentUser(propURI, basePropURI, instanceURI, allStatementInserts);
				} else {
					String cleanValue = Utility.cleanString(value, true, false, true);
					// logger.info("Processing String value " + cleanValue);
					allStatementInserts.add(new Object[] { instanceURI, propURI, cleanValue, false });
				}
				if (subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "STRING");
				}
			}
		}
	}

	/**
	 * Insert the current date as a property onto a node if property is
	 * "PROCESS_CURRENT_DATE"
	 * 
	 * @param propInstanceURI     String containing the URI of the property at the
	 *                            instance level
	 * @param basePropURI         String containing the base URI of the property at
	 *                            SEMOSS level
	 * @param subjectNodeURI      String containing the URI of the subject at the
	 *                            instance level
	 * @param allStatementInserts
	 */
	private static void insertCurrentDate(String propInstanceURI, String basePropURI, String subjectNodeURI,
			List<Object[]> allStatementInserts) {
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		Date dateFormatted;
		try {
			dateFormatted = df.parse(date);
			allStatementInserts.add(new Object[] { propInstanceURI, RDF.TYPE, basePropURI, true });
			allStatementInserts.add(new Object[] { subjectNodeURI, propInstanceURI, dateFormatted, false });
		} catch (ParseException e) {
//			logger.error("ERROR: could not parse date: " + date);
		}
	}

	/**
	 * 
	 * @param propURI             String containing the URI of the property at the
	 *                            instance level
	 * @param basePropURI         String containing the base URI of the property at
	 *                            SEMOSS level
	 * @param subjectNodeURI      String containing the URI of the subject at the
	 *                            instance level
	 * @param allStatementInserts
	 */
	private static void insertCurrentUser(String propURI, String basePropURI, String subjectNodeURI,
			List<Object[]> allStatementInserts) {
		String cleanValue = System.getProperty("user.name");
		allStatementInserts.add(new Object[] { propURI, RDF.TYPE, basePropURI, true });
		allStatementInserts.add(new Object[] { subjectNodeURI, propURI, cleanValue, false });
	}

	/**
	 * Delete all the triples from the database
	 * 
	 * @param engine
	 */
	public static void deleteAllTriples(IDatabaseEngine engine) {
		long start = System.currentTimeMillis();
		classLogger.info(
				"Starting to delete all triples from database " + engine.getEngineName() + "_" + engine.getEngineId());
		// null is equiv. to a wildcard for removeStatements method
		// so it matches any subject, predicate, object
		if (engine instanceof ISesameRDFEngine) {
			try {
				((ISesameRDFEngine) engine).getRc().clear();
			} catch (RepositoryException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else if (engine instanceof RDFFileJenaEngine) {
			((RDFFileJenaEngine) engine).getJenaModel().removeAll();
		} else if (engine instanceof RDFJenaTDBEngine) {
			Dataset dataset = ((RDFJenaTDBEngine) engine).getDataset();
			dataset.getDefaultModel().removeAll();
		} else {
			throw new IllegalArgumentException("Engine is not a valid type to remove triples from");
		}
		long end = System.currentTimeMillis();
		classLogger
				.info("Done deleting all triples from database " + engine.getEngineName() + "_" + engine.getEngineId());
		classLogger.debug("Deleting triples in " + (end - start) + "ms");
	}

	/**
	 * 
	 * @param baseUri
	 * @param nodeType
	 * @return
	 */
	public static String getInstanceURI(String baseUri, String nodeType) {
		return baseUri + "/" + Constants.DEFAULT_NODE_CLASS + "/" + nodeType;
	}

	/**
	 * 
	 * @param baseUri
	 * @param relName
	 * @return
	 */
	public static String getRelBaseURI(String baseUri, String relName) {
		return baseUri + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
	}

	/**
	 * 
	 * @return
	 */
	public static String getBasePropURI() {
		// TODO this does not use custom base input
		String semossURI = Utility.getDIHelperProperty(Constants.SEMOSS_URI);
		return semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + "Contains";
	}
}
