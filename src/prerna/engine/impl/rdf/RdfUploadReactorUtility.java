package prerna.engine.impl.rdf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.SailException;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RdfUploadReactorUtility {
	
	private static final Logger classLogger = LogManager.getLogger(RdfUploadReactorUtility.class.getName());

	private RdfUploadReactorUtility() {
		
	}

	public static void loadMetadataIntoEngine(IDatabaseEngine engine, WriteOWLEngine owlEngine) {
		Hashtable<String, String> hash = owlEngine.getConceptHash();
		String object = AbstractOWLEngine.SEMOSS_URI_PREFIX + AbstractOWLEngine.DEFAULT_NODE_CLASS;
		for(String concept : hash.keySet()) {
			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(concept), RDFS.SUBCLASSOF + "", object, true});
		}
		hash = owlEngine.getRelationHash();
		object = AbstractOWLEngine.SEMOSS_URI_PREFIX + AbstractOWLEngine.DEFAULT_RELATION_CLASS;
		for(String relation : hash.keySet()) {
			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(relation), RDFS.SUBPROPERTYOF + "", object, true});
		}
		hash = owlEngine.getPropHash();
		object = AbstractOWLEngine.SEMOSS_URI_PREFIX + AbstractOWLEngine.DEFAULT_PROP_CLASS;
		for(String prop : hash.keySet()) {
			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(prop), RDF.TYPE + "", object, true});
		}
	}
	
	/**
	 * Create and add all triples associated with relationship tabs
	 * @param subjectNodeType					String containing the subject node type
	 * @param objectNodeType					String containing the object node type
	 * @param instanceSubjectName				String containing the name of the subject instance
	 * @param instanceObjectName				String containing the name of the object instance
	 * @param relName							String containing the name of the relationship between the subject and object
	 * @param propHash							Hashtable that contains all properties
	 */
	public static void createRelationship(IDatabaseEngine engine, WriteOWLEngine owlEngine, String baseUri, String subjectNodeType, String objectNodeType, String instanceSubjectName, String instanceObjectName, String relName, Hashtable<String, Object> propHash) {
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
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, RDF.TYPE, subjectSemossBaseURI, true });
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, RDFS.LABEL, instanceSubjectName, false });

		// create the full URI for the object instance
		// add type and label triples to database
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName; 
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { objectNodeURI, RDF.TYPE, objectSemossBaseURI, true });
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT,new Object[] { objectNodeURI, RDFS.LABEL, instanceObjectName, false });

		// generate URIs for the relationship
		relName = Utility.cleanPredicateString(relName);
		String relSemossBaseURI = owlEngine.addRelation(subjectNodeType, objectNodeType, relName);
		String relInstanceBaseURI = getRelBaseURI(baseUri, relName);


		// create instance value of relationship and add instance relationship,
		// subproperty, and label triples
		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName;
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.SUBPROPERTYOF, relSemossBaseURI, true });
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.LABEL, 
				instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName, false });
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, instanceRelURI, objectNodeURI, true });

		addProperties(engine, owlEngine, "", instanceRelURI, propHash);
	}
	
	public static void addNodeProperties(IDatabaseEngine engine, WriteOWLEngine owlEngine, String baseUri, String nodeType, String instanceName, Hashtable<String, Object> propHash) {
		//create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		nodeType = Utility.cleanString(nodeType, true); 
		String semossBaseURI = owlEngine.addConcept(nodeType);
		String instanceBaseURI = getInstanceURI(baseUri, nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDF.TYPE, semossBaseURI, true});
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDFS.LABEL, instanceName, false});
		addProperties(engine, owlEngine, nodeType, subjectNodeURI, propHash);
	}
	
	public static void addProperties(IDatabaseEngine engine, WriteOWLEngine owlEngine, String subjectNodeType, String instanceURI, Hashtable<String, Object> propHash) {
		// add all properties
		Enumeration<String> propKeys = propHash.keys();

		String basePropURI  = getBasePropURI();
		
		// add property triple based on data type of property
		while (propKeys.hasMoreElements()) {
			String key = propKeys.nextElement().toString();
			String propURI = basePropURI + "/" + Utility.cleanString(key, true);
			// logger.info("Processing Property " + key + " for " + instanceURI);
			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { propURI, RDF.TYPE, basePropURI, true });
			if (propHash.get(key) instanceof Number) {
				Double value = ((Number) propHash.get(key)).doubleValue();
				// logger.info("Processing Double value " + value);
				engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.doubleValue(), false });
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "DOUBLE");
				}
			} else if (propHash.get(key) instanceof Date) {
				Date value = (Date) propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				Date dateFormatted;
				try {
					dateFormatted = df.parse(date);
				} catch (ParseException e) {
//					logger.error("ERROR: could not parse date: " + date);
					continue;
				}
				// logger.info("Processing Date value " + dateFormatted);
				engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, dateFormatted, false });
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "DATE");
				}
			} else if (propHash.get(key) instanceof Boolean) {
				Boolean value = (Boolean) propHash.get(key);
				// logger.info("Processing Boolean value " + value);
				engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.booleanValue(), false });
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "BOOLEAN");
				}
			} else {
				String value = propHash.get(key).toString();
				if (value.equals(Constants.PROCESS_CURRENT_DATE)) {
					// logger.info("Processing Current Date Property");
					insertCurrentDate(engine, propURI, basePropURI, instanceURI);
				} else if (value.equals(Constants.PROCESS_CURRENT_USER)) {
					// logger.info("Processing Current User Property");
					insertCurrentUser(engine, propURI, basePropURI, instanceURI);
				} else {
					String cleanValue = Utility.cleanString(value, true, false, true);
					// logger.info("Processing String value " + cleanValue);
					engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, cleanValue, false });
				}
				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
					owlEngine.addProp(subjectNodeType, key, "STRING");
				}
			}
		}
	}
	/**
	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	public static void insertCurrentDate(IDatabaseEngine engine, String propInstanceURI, String basePropURI, String subjectNodeURI) {
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		Date dateFormatted;
		try {
			dateFormatted = df.parse(date);
			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propInstanceURI, RDF.TYPE, basePropURI, true});
			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propInstanceURI, dateFormatted, false});
		} catch (ParseException e) {
//			logger.error("ERROR: could not parse date: " + date);
		}
	}
	
	public static String getInstanceURI(String baseUri, String nodeType) {
		return baseUri + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
	}
	
	public static String getRelBaseURI(String baseUri, String relName) {
		return 	baseUri + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
	}
	
	public static String getBasePropURI() {
		// TODO this does not use custom base input
		String semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		return semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + "Contains";
	}
	
	/**
	 * Insert the current user as a property onto a node if property is "PROCESS_CURRENT_USER"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	public static void insertCurrentUser(IDatabaseEngine engine, String propURI, String basePropURI, String subjectNodeURI) {
		String cleanValue = System.getProperty("user.name");
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propURI, cleanValue, false});
	}
	
	/**
	 * Delete all the triples from the database
	 * @param engine
	 */
	public static void deleteAllTriples(IDatabaseEngine engine) {
		long start = System.currentTimeMillis();
		classLogger.info("Starting to delete all triples from database " + engine.getEngineName() + "_" + engine.getEngineId());
		// null is equiv. to a wildcard for removeStatements method
		// so it matches any subject, predicate, object
		if(engine instanceof BigDataEngine) {
			try {
				((BigDataEngine) engine).getSc().removeStatements(null, null, null);
			} catch (SailException e) {
				e.printStackTrace();
			}
		} else if(engine instanceof RDFFileSesameEngine) {
			try {
				((RDFFileSesameEngine) engine).getSc().removeStatements(null, null, null);
			} catch (SailException e) {
				e.printStackTrace();
			}
		} else {
			throw new IllegalArgumentException("Engine is not a valid type to remove triples from");
		}
		long end = System.currentTimeMillis();
		classLogger.info("Done deleting all triples from database " + engine.getEngineName() + "_" + engine.getEngineId());
		classLogger.debug("Deleting triples in " + (end-start) + "ms");
	}
}
