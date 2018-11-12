package prerna.forms;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.semoss.web.form.FormResource;
import prerna.util.Constants;
import prerna.util.Utility;

public final class FormBuilder {

	public static final String FORM_BUILDER_ENGINE_NAME = "form_builder_engine";
	public static final String AUDIT_FORM_SUFFIX = "_FORM_LOG";
	
	private static final DateFormat DATE_DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS");
	private static final DateFormat SIMPLE_DATE_DF = new SimpleDateFormat("yyyy-MM-dd");
	private static final DateFormat GENERIC_DF = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSSSS'Z'");
	private static final String UPSTREAM = "upstream";
	private static final String DOWNSTREAM = "downstream";
	private static final String OVERRIDE = "override";
	private static final String OVERRIDE_TYPE = "overrideType";
	private static final String DELETE_UNCONNECTED_CONCEPTS = "deleteUnconnected";
	private static final String REMOVE_NODE = "removeNode";
	private static final String ADD = "Added";
	private static final String REMOVE = "Removed";

	private FormBuilder() {

	}

	/**
	 * 
	 * @param form
	 * @throws IOException 
	 */
	public static void commitFormData(IEngine engine, Map<String, Object> engineHash, String user) throws IOException {
		if(engine == null) {
			throw new IOException("Engine cannot be found");
		}
		
		String auditLogTableName = RdbmsQueryBuilder.escapeForSQLStatement(RDBMSEngineCreationHelper.cleanTableName(engine.getEngineId())).toUpperCase() + AUDIT_FORM_SUFFIX;
		IEngine formEng = Utility.getEngine(FORM_BUILDER_ENGINE_NAME);
		// create audit table if doesn't exist
		String checkTableQuery = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='" + auditLogTableName + "'";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(formEng, checkTableQuery);
		boolean auditTableExists = false;
		if(wrapper.hasNext()) {
			auditTableExists = true;
		}
		if(!auditTableExists) {
			String createAuditTable = "CREATE TABLE " + auditLogTableName + " (ID IDENTITY, USER VARCHAR(255), ACTION VARCHAR(100), START_NODE VARCHAR(255), REL_NAME VARCHAR(255), END_NODE VARCHAR(255), PROP_NAME VARCHAR(255), PROP_VALUE CLOB, TIME TIMESTAMP)";
			try {
				formEng.insertData(createAuditTable);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		String semossBaseURI = "http://semoss.org/ontologies";
		String baseURI = engine.getNodeBaseUri();
		if(baseURI != null && !baseURI.isEmpty()) {
			baseURI = baseURI.replace("/Concept/", "");
		} else {
			baseURI = semossBaseURI;
		}

		String relationBaseURI = semossBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		String conceptBaseURI = semossBaseURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String propertyBaseURI = semossBaseURI + "/" + Constants.DEFAULT_PROPERTY_CLASS;

		List<HashMap<String, Object>> nodes = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("nodes")) {
			nodes = (List<HashMap<String, Object>>) engineHash.get("nodes"); 
		}
		List<HashMap<String, Object>> relationships = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("relationships")) {
			relationships = (List<HashMap<String, Object>>)engineHash.get("relationships");
		}
		List<HashMap<String, Object>> removeNodes = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("removeNodes")) {
			removeNodes = (List<HashMap<String, Object>>) engineHash.get("removeNodes"); 
		}
		List<HashMap<String, Object>> removeRelationships = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("removeRelationships")) {
			removeRelationships = (List<HashMap<String, Object>>)engineHash.get("removeRelationships");
		}

		if(engine.getEngineType() == IEngine.ENGINE_TYPE.JENA || engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME) {
			saveRDFFormData(engine, baseURI, relationBaseURI, propertyBaseURI, nodes, relationships, removeNodes, removeRelationships, formEng, auditLogTableName, user);
		} else if(engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS) {
			saveRDBMSFormData(engine, baseURI, relationBaseURI, conceptBaseURI, propertyBaseURI, nodes, relationships, formEng, auditLogTableName, user);
		} else {
			throw new IOException("Engine type cannot be found");
		}

		//commit information to db
		formEng.commit();
		engine.commit();
	}
	
	/////////////////////////////////////////////RDF CODE/////////////////////////////////////////////

	/**
	 * 
	 * @param engine
	 * @param baseURI
	 * @param relationBaseURI
	 * @param conceptBaseURI
	 * @param propertyBaseURI
	 * @param nodes
	 * @param relationships
	 * 
	 * Save data from the form to a RDF Database
	 * @param auditLogTableName 
	 * @param formEng 
	 * @param user 
	 */
	private static void saveRDFFormData(IEngine engine, String baseURI, String relationBaseURI, String propertyBaseURI, List<HashMap<String, Object>> nodes, List<HashMap<String, Object>> relationships, List<HashMap<String, Object>> removeNodes, List<HashMap<String, Object>> removeRelationships, IEngine formEng, String auditLogTableName, String user) {
		String startNode;
		String endNode;
		String subject;
		String instanceSubjectURI;
		String object;
		String instanceObjectURI;
		String relationType;
		String baseRelationshipURI;
		String instanceRel;
		String instanceRelationshipURI;
		String conceptType;
		String conceptValue;
		String instanceConceptURI;
		Object propertyValue;
		String propertyURI;

		// for deleting existing relationships
		for(int i = 0; i < removeRelationships.size(); i++) {
			Map<String, Object> deleteRelationships = removeRelationships.get(i);
			startNode = Utility.cleanString(deleteRelationships.get("startNodeVal").toString(), true);
			endNode = Utility.cleanString(deleteRelationships.get("endNodeVal").toString(), true);
			subject = deleteRelationships.get("startNodeType").toString();
			object =  deleteRelationships.get("endNodeType").toString(); 
			instanceSubjectURI = baseURI + "/Concept/" + Utility.getInstanceName(subject) + "/" + startNode;
			instanceObjectURI = baseURI + "/Concept/" + Utility.getInstanceName(object) + "/" +endNode;

			relationType = Utility.getInstanceName(deleteRelationships.get("relType").toString());
			baseRelationshipURI = relationBaseURI + "/" + relationType;
			instanceRel = startNode + ":" + endNode;
			instanceRelationshipURI =  baseURI + "/Relation/" + relationType + "/" + instanceRel;
			
			boolean deleteUnconnectedConcepts = false;
			boolean removeNode = false;
			overrideRDFRelationship(engine, instanceSubjectURI, subject, instanceObjectURI, object, baseRelationshipURI, true, deleteUnconnectedConcepts, removeNode, formEng, auditLogTableName, user);
		}
		
		// for deleting existing concepts
		for(int i = 0; i < removeNodes.size(); i++) {
			Map<String, Object> deleteConcept = removeNodes.get(i);
			conceptType = deleteConcept.get("conceptName").toString();
			conceptValue = Utility.cleanString(deleteConcept.get("conceptValue").toString(), true);
			instanceConceptURI = baseURI + "/Concept/" + Utility.getInstanceName(conceptType) + "/" + conceptValue;
			
			boolean removeConcept = false;
			if(deleteConcept.get(OVERRIDE) != null) {
				removeConcept = Boolean.parseBoolean(deleteConcept.get(OVERRIDE).toString());
			}
			
			if(removeConcept){
				// need to delete all properties before deleting concept
				Set<String> uriBindingList = new HashSet<String>();
				uriBindingList.add(instanceConceptURI);
				deleteAllRDFConnectionsToConcept(engine, uriBindingList, formEng, auditLogTableName, user);
				removeRDFNodeAndAllProps(engine, uriBindingList, formEng, auditLogTableName, user);
			} else if(deleteConcept.containsKey("properties")) {
				List<HashMap<String, Object>> properties = (List<HashMap<String, Object>>) deleteConcept.get("properties");

				for(int j = 0; j < properties.size(); j++) {
					Map<String, Object> property = properties.get(j);
					propertyValue = property.get("propertyValue");
					if(propertyValue instanceof String) {
						// check if string val is a date
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
							try {
								dateFormat.setLenient(false);
								propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
							} catch (ParseException e) {
								propertyValue = propertyValue.toString();
							}
					}
					propertyURI = property.get("propertyName").toString();

					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propertyValue, false});
					// ugh... we need to push forms
					// values being passed are not properly keeping track of things that have underscores and things that don't
					// just going to try both versions
					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, Utility.cleanString(propertyValue.toString(), true, false, true), false});

					// add audit log statement
					Calendar cal = Calendar.getInstance();
					String currTime = DATE_DF.format(cal.getTime());
					addAuditLog(formEng, auditLogTableName, user, REMOVE, instanceConceptURI, "", "", propertyURI, propertyValue + "", currTime);
					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{propertyURI, RDF.TYPE, propertyBaseURI, true});
				}
			}
		}
		
		// for adding new relationships
		for(int i = 0; i < relationships.size(); i++) {
			Map<String, Object> relationship = relationships.get(i);
			startNode = Utility.cleanString(relationship.get("startNodeVal").toString(), true);
			endNode = Utility.cleanString(relationship.get("endNodeVal").toString(), true);
			subject = relationship.get("startNodeType").toString();
			object =  relationship.get("endNodeType").toString(); 
			instanceSubjectURI = baseURI + "/Concept/" + Utility.getInstanceName(subject) + "/" + startNode;
			instanceObjectURI = baseURI + "/Concept/" + Utility.getInstanceName(object) + "/" +endNode;

			relationType = Utility.getInstanceName(relationship.get("relType").toString());
			baseRelationshipURI = relationBaseURI + "/" + relationType;
			instanceRel = startNode + ":" + endNode;
			instanceRelationshipURI =  baseURI + "/Relation/" + relationType + "/" + instanceRel;

			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, RDF.TYPE, subject, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceObjectURI, RDF.TYPE, object, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, relationBaseURI, instanceObjectURI, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, baseRelationshipURI, instanceObjectURI, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, instanceRelationshipURI, instanceObjectURI, true});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(formEng, auditLogTableName, user, ADD, instanceSubjectURI, baseRelationshipURI, instanceObjectURI, "", "", currTime);
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDFS.SUBPROPERTYOF, baseRelationshipURI, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDFS.SUBPROPERTYOF, relationBaseURI, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDF.TYPE, RDF.PROPERTY, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDFS.LABEL, instanceRel, false});
		}
		
		//for adding concepts and properties of nodes
		for(int i = 0; i < nodes.size(); i++) {
			Map<String, Object> concept = nodes.get(i);
			conceptType = concept.get("conceptName").toString();
			conceptValue = Utility.cleanString(concept.get("conceptValue").toString(), true);

			instanceConceptURI = baseURI + "/Concept/" + Utility.getInstanceName(conceptType) + "/" + conceptValue;
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceConceptURI, RDF.TYPE, conceptType, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceConceptURI, RDFS.LABEL, conceptValue, false});
			if(concept.containsKey("properties")) {
				List<HashMap<String, Object>> properties = (List<HashMap<String, Object>>) concept.get("properties");

				for(int j = 0; j < properties.size(); j++) {
					Map<String, Object> property = properties.get(j);
					propertyValue = property.get("propertyValue");
					if(propertyValue instanceof String) {
						// check if string val is a date
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
						try {
							dateFormat.setLenient(true);
							propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
						} catch (ParseException e) {
							propertyValue = propertyValue.toString();
						}
					}
					propertyURI = property.get("propertyName").toString();

					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propertyValue, false});
					// add audit log statement
					Calendar cal = Calendar.getInstance();
					String currTime = DATE_DF.format(cal.getTime());
					addAuditLog(formEng, auditLogTableName, user, ADD, instanceConceptURI, "", "", propertyURI, propertyValue + "", currTime);
					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyURI, RDF.TYPE, propertyBaseURI, true});
				}
			}
		}
	}
	
//	private static void removeRDFNodeProp(IEngine engine, String instanceConceptURI, String propertyURI, IEngine formEng, String auditLogTableName, String user) {
//		String getOldNodePropValuesQuery = "SELECT DISTINCT ?propVal WHERE { BIND(<" + instanceConceptURI + "> AS ?instance) {?instance <" + propertyURI + "> ?propVal} }";
//
//		List<Object> propVals = new ArrayList<Object>();
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, getOldNodePropValuesQuery);
//		String[] names = wrapper.getVariables();
//		while(wrapper.hasNext()) {
//			ISelectStatement ss = wrapper.next();
//			propVals.add(ss.getVar(names[0]));
//		}
//
//		for(Object propertyValue : propVals) {
//			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propertyValue, false});
//			// add audit log statement
//			Calendar cal = Calendar.getInstance();
//			String currTime = DATE_DF.format(cal.getTime());
//			addAuditLog(formEng, auditLogTableName, user, REMOVE, instanceConceptURI, "", "", propertyURI, propertyValue + "", currTime);
//		}
//	}
	
	/**
	 * Deletes the relationship and relationship properties that exist between an instance and all other instances of a specified type
	 * @param engine
	 * @param instanceSubjectURI
	 * @param subjectTypeURI
	 * @param instanceObjectURI
	 * @param objectTypeURI
	 * @param baseRelationshipURI
	 * @param deleteDownstream
	 * @param removeNode 
	 * @param deleteUnconnectedConcepts 
	 * @param removeNode 
	 * @param deleteUnconnectedConcepts 
	 */
	private static void overrideRDFRelationship(IEngine engine, String instanceSubjectURI, String subjectTypeURI, String instanceObjectURI, String objectTypeURI, String baseRelationshipURI, boolean deleteDownstream, boolean deleteUnconnectedConcepts, boolean removeNode, IEngine formEng, String auditLogTableName, String user) {
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?SUB ?PRED ?OBJ ?LABEL ?PROP ?VAL WHERE { ");
//		if(deleteDownstream) {
			query.append("BIND(<" + instanceSubjectURI + "> AS ?SUB) ");
//		} else {
			query.append("BIND(<" + instanceObjectURI + "> AS ?OBJ) ");
//		}
		query.append("{?SUB <").append(RDF.TYPE).append("> <" + subjectTypeURI + ">} ");
		query.append("{?OBJ <").append(RDF.TYPE).append("> <" + objectTypeURI + ">} ");
		query.append("{ ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <" + baseRelationshipURI + ">} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("} UNION { ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <" + baseRelationshipURI + ">} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
		query.append("{?PRED ?PROP ?VAL} ");
		query.append("} }");

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
		String[] names = wrapper.getVariables();
		Set<String> uriBindingList = new HashSet<String>();
		String baseRelationURI = "http://semoss.org/ontologies/Relation";
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String subURI = ss.getRawVar(names[0]) + "";
			String predURI = ss.getRawVar(names[1]) + "";
			String objURI = ss.getRawVar(names[2]) + "";
			Object label = ss.getVar(names[3]);
			Object propURI = ss.getRawVar(names[4]);
			Object propVal = ss.getVar(names[5]);

			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, predURI, objURI, true});
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, baseRelationshipURI, objURI, true});
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, baseRelationURI, objURI, true});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(formEng, auditLogTableName, user, REMOVE, subURI, baseRelationshipURI, objURI, "", "", currTime);
						
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRelationshipURI, true});
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRelationURI, true});
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDF.TYPE, RDF.PROPERTY, true});
			if(label != null && !label.toString().isEmpty()) {
				engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.LABEL, label.toString(), false});
			}
			if(propURI != null && !propURI.toString().isEmpty()) {
				engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, propURI.toString(), propVal, false});
				// add audit log statement
				currTime = DATE_DF.format(cal.getTime());
				addAuditLog(formEng, auditLogTableName, user, REMOVE, "", predURI, "", propURI.toString(), propVal + "", currTime);
			}
			
			if(deleteDownstream) {
				uriBindingList.add(objURI);
//				if(removeNode) {
//					deleteAllRDFConnectionsToConcept(engine, objURI);
//				} else if(deleteUnconnectedConcepts) {
//					removeUnconnectedRDFNodes(engine, objURI);
//				}
			} else {
				uriBindingList.add(subURI);
//				if(removeNode) {
//					deleteAllRDFConnectionsToConcept(engine, subURI);
//				} else if(deleteUnconnectedConcepts) {
//					removeUnconnectedRDFNodes(engine, subURI);
//				}
			}
		}
		
		if(removeNode) {
			deleteAllRDFConnectionsToConcept(engine, uriBindingList, formEng, auditLogTableName, user);
		} else if(deleteUnconnectedConcepts) {
			removeUnconnectedRDFNodes(engine, uriBindingList, formEng, auditLogTableName, user);
		}
		
	}
	
	private static void deleteAllRDFConnectionsToConcept(IEngine engine, Set<String> uriBindingList, IEngine formEng, String auditLogTableName, String user) {
		String[] queries = new String[]{
				generateDeleteAllRDFConnectionsToConceptQuery(uriBindingList, true),
				generateDeleteAllRDFConnectionsToConceptQuery(uriBindingList, false)};
		
		for(String query : queries) {
			if(query == null) {
				continue;
			}
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext()) {
				ISelectStatement ss = wrapper.next();
				String subURI = ss.getRawVar(names[0]) + "";
				String predURI = ss.getRawVar(names[1]) + "";
				String objURI = ss.getRawVar(names[2]) + "";
				Object label = ss.getVar(names[3]);
				Object propURI = ss.getRawVar(names[4]);
				Object propVal = ss.getVar(names[5]);
	
				engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, predURI, objURI, true});
				// add audit log statement
				Calendar cal = Calendar.getInstance();
				String currTime = DATE_DF.format(cal.getTime());
				addAuditLog(formEng, auditLogTableName, user, REMOVE, subURI, predURI, objURI, "", "", currTime);
				if(label != null && label.toString().isEmpty()) {
					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.LABEL, label, false});
				}
				if(propURI != null && !propURI.toString().isEmpty()) {
					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, propURI, propVal, false});
					// add audit log statement
					cal = Calendar.getInstance();
					currTime = DATE_DF.format(cal.getTime());
					addAuditLog(formEng, auditLogTableName, user, REMOVE, "", predURI, "", propURI.toString(), propVal + "", currTime);
				}
			}
		}
		
		// lastly, remove the node and all its props
		removeRDFNodeAndAllProps(engine, uriBindingList, formEng, auditLogTableName, user);
	}

//	private static void deleteAllRDFConnectionsToConcept(IEngine engine, String conceptURI) {
//		// generate queries to delete all upstream/downstream nodes to concept uri
//		String[] queries = new String[]{
//				generateDeleteAllRDFConnectionsToConceptQuery(conceptURI, true),
//				generateDeleteAllRDFConnectionsToConceptQuery(conceptURI, false)};
//		
//		for(String query : queries) {
//			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
//			String[] names = wrapper.getVariables();
//			while(wrapper.hasNext()) {
//				ISelectStatement ss = wrapper.next();
//				String subURI = ss.getRawVar(names[0]) + "";
//				String predURI = ss.getRawVar(names[1]) + "";
//				String objURI = ss.getRawVar(names[2]) + "";
//				String label = ss.getVar(names[3]) + "";
//				String propURI = ss.getRawVar(names[4]) + "";
//				Object propVal = ss.getVar(names[5]);
//	
//				engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, predURI, objURI, true});
//				if(!label.isEmpty()) {
//					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.LABEL, label, false});
//				}
//				if(!propURI.isEmpty()) {
//					engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, propURI, propVal, false});
//				}
//			}
//		}
//		
//		// lastly, remove the node and all its props
//		removeRDFNodeAndAllProps(engine, conceptURI);
//	}
	
	private static String generateDeleteAllRDFConnectionsToConceptQuery(Set<String> conceptURI, boolean downstream) {
		if(conceptURI.isEmpty()) {
			return null;
		}
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?SUB ?PRED ?OBJ ?LABEL ?PROP ?VAL WHERE { ");
		query.append("{ ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <http://semoss.org/ontologies/Relation>} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("} UNION { ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <http://semoss.org/ontologies/Relation>} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
		query.append("{?PRED ?PROP ?VAL} ");
		query.append("} }");
		if(downstream) {
			query.append("BINDINGS ?SUB {");
		} else {
			query.append("BINDINGS ?OBJ {");
		}
		for(String concept : conceptURI) {
			query.append("(<");
			query.append(concept);
			query.append(">)");
		}
		query.append("}");

		return query.toString();
	}
	
//	private static String generateDeleteAllRDFConnectionsToConceptQuery(String conceptURI, boolean downstream) {
//		StringBuilder query = new StringBuilder("SELECT DISTINCT ?SUB ?PRED ?OBJ ?LABEL ?PROP ?VAL WHERE { ");
//		if(downstream) {
//			query.append("BIND(<" + conceptURI + "> AS ?SUB) ");
//		} else {
//			query.append("BIND(<" + conceptURI + "> AS ?OBJ) ");
//		}
//		query.append("{ ");
//		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <http://semoss.org/ontologies/Relation>} ");
//		query.append("{?SUB ?PRED ?OBJ} ");
//		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
//		query.append("} UNION { ");
//		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <http://semoss.org/ontologies/Relation>} ");
//		query.append("{?SUB ?PRED ?OBJ} ");
//		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
//		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
//		query.append("{?PRED ?PROP ?VAL} ");
//		query.append("} }");
//		
//		return query.toString();
//	}
	
	private static void removeUnconnectedRDFNodes(IEngine engine, Set<String> uriBindingList, IEngine formEng, String auditLogTableName, String user) {
		if(uriBindingList.isEmpty()) {
			return;
		}
		
		// check relationships in one direction
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?CONCEPT (COUNT(?REL) AS ?C_RELS) WHERE {");
		query.append("{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "); 
		query.append("{?CONCEPT a <http://semoss.org/ontologies/Concept>} ");
		query.append("{?NODE ?REL ?CONCEPT} } ");
		query.append("GROUP BY ?CONCEPT BINDINGS ?NODE {");
		for(String concept : uriBindingList) {
			query.append("(<");
			query.append(concept);
			query.append(">)");
		}
		query.append("}");
		
		Set<String> allNodes = new HashSet<String>();
		Set<String> connectedNodes = new HashSet<String>();
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String nodeURI = ss.getRawVar(names[0]) + "";
			int count = ((Number) ss.getVar(names[1])).intValue();
			
			allNodes.add(nodeURI);
			if(count > 0) {
				connectedNodes.add(nodeURI);
			}
		}
		
		// make sure to check relationships in the other direction
		query = new StringBuilder("SELECT DISTINCT ?CONCEPT (COUNT(?REL) AS ?C_RELS) WHERE {");
		query.append("{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "); 
		query.append("{?CONCEPT a <http://semoss.org/ontologies/Concept>} ");
		query.append("{?CONCEPT ?REL ?NODE} } ");
		query.append("GROUP BY ?CONCEPT BINDINGS ?NODE {");
		for(String concept : uriBindingList) {
			query.append("(<");
			query.append(concept);
			query.append(">)");
		}
		query.append("} ");

		wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
		names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String nodeURI = ss.getRawVar(names[0]) + "";
			int count = ((Number) ss.getVar(names[1])).intValue();
			
			allNodes.add(nodeURI);
			if(count > 0) {
				connectedNodes.add(nodeURI);
			}
		}
		
		allNodes.removeAll(connectedNodes);
		if(allNodes.size() > 0) {
			// made sure has no upstream or downstream, so delete it and all its properties
			removeRDFNodeAndAllProps(engine, allNodes, formEng, auditLogTableName, user);
		}
	}
	
//	public static void removeUnconnectedRDFNodes(IEngine engine, String conceptURI) {
//		// check relationships in one direction
//		StringBuilder query = new StringBuilder("SELECT DISTINCT (COUNT(?REL) AS ?C_RELS) WHERE {");
//		query.append("BIND(<" + conceptURI + "> AS ?NODE) ");
//		query.append("{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "); 
//		query.append("{?CONCEPT a <http://semoss.org/ontologies/Concept>} ");
//		query.append("{?NODE ?REL ?CONCEPT} }");
//
//		boolean isConnected = false;
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
//		String[] names = wrapper.getVariables();
//		while(wrapper.hasNext()) {
//			ISelectStatement ss = wrapper.next();
//			Integer count = (Integer) ss.getVar(names[0]);
//			
//			if(count > 0) {
//				isConnected = true;
//			}
//		}
//		
//		if(isConnected) {
//			return;
//		} else {
//			// make sure to check relationships in the other direction
//			query = new StringBuilder("SELECT DISTINCT (COUNT(?REL) AS ?C_RELS) WHERE {");
//			query.append("BIND(<" + conceptURI + "> AS ?NODE) ");
//			query.append("{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "); 
//			query.append("{?CONCEPT a <http://semoss.org/ontologies/Concept>} ");
//			query.append("{?CONCEPT ?REL ?NODE} }");
//
//			wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
//			names = wrapper.getVariables();
//			while(wrapper.hasNext()) {
//				ISelectStatement ss = wrapper.next();
//				Integer count = (Integer) ss.getVar(names[0]);
//				
//				if(count > 0) {
//					isConnected = true;
//				}
//			}
//			
//			if(!isConnected) {
//				// made sure has no upstream or downstream, so delete it and all its properties
//				removeRDFNodeAndAllProps(engine, conceptURI);
//			}
//		}
//	}
	
	private static void removeRDFNodeAndAllProps(IEngine engine, Set<String> uriBindingList, IEngine formEng, String auditLogTableName, String user) {
		if(uriBindingList.isEmpty()) {
			return;
		}
		
		// delete the properties for the instances
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?NODE ?PROP ?VAL WHERE { ");
		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
		query.append("{?NODE ?PROP ?VAL} } ");
		query.append("BINDINGS ?NODE {");
		for(String concept : uriBindingList) {
			query.append("(<");
			query.append(concept);
			query.append(">)");
		}
		query.append("}");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String nodeURI = ss.getRawVar(names[0]) + "";
			String propURI = ss.getRawVar(names[1]) + "";
			Object propVal = ss.getVar(names[2]);
			
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, propURI, propVal, false});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(formEng, auditLogTableName, user, REMOVE, nodeURI, "", "", propURI, propVal + "", currTime);
		}
		
		// deletes the instances
		String semossBaseConcept = "http://semoss.org/ontologies/Concept";
		for(String nodeURI : uriBindingList) {
			String typeURI = semossBaseConcept + "/" + Utility.getClassName(nodeURI);
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, RDF.TYPE, typeURI, true});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(formEng, auditLogTableName, user, REMOVE, nodeURI, "", "", "", "", currTime);
			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, RDFS.LABEL, Utility.getInstanceName(nodeURI), false});
		}
		
	}

//	private static void removeRDFNodeAndAllProps(IEngine engine, String conceptURI) {
//		StringBuilder query = new StringBuilder("SELECT DISTINCT ?NODE ?PROP ?VAL WHERE { ");
//		query.append("BIND(<" + conceptURI + "> AS ?NODE) ");
//		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
//		query.append("{?NODE ?PROP ?VAL} ");
//		
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query.toString());
//		String[] names = wrapper.getVariables();
//		while(wrapper.hasNext()) {
//			ISelectStatement ss = wrapper.next();
//			String nodeURI = ss.getRawVar(names[0]) + "";
//			String propURI = ss.getRawVar(names[1]) + "";
//			Object propVal = ss.getVar(names[2]);
//			
//			engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, propURI, propVal, false});
//		}
//		String typeURI = "http://semoss.org/ontologies/Concept/" + Utility.getClassName(conceptURI);
//		engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{conceptURI, RDF.TYPE, typeURI, true});
//		engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{conceptURI, RDFS.LABEL, Utility.getInstanceName(conceptURI), false});
//	}

	/////////////////////////////////////////////RDBMS CODE/////////////////////////////////////////////
	
	/**
	 * 
	 * @param engine
	 * @param baseURI
	 * @param relationURI
	 * @param conceptBaseURI
	 * @param propertyBaseURI
	 * @param nodes
	 * @param relationships
	 * 
	 * Save form data to a RDBMS database
	 * @param user 
	 * @param auditLogTableName 
	 * @param formEng 
	 */
	private static void saveRDBMSFormData(IEngine engine, String baseURI, String relationURI, String conceptBaseURI, String propertyBaseURI, List<HashMap<String, Object>> nodes, List<HashMap<String, Object>> relationships, IEngine formEng, String auditLogTableName, String user) {
		String tableName;
		String tableColumn;
		String tableValue;
		Map<String, Map<String, String>> nodeMapping = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> tableColTypesHash = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);

		List<String> tablesToRemoveDuplicates = new ArrayList<String>();
		List<String> colsForTablesToRemoveDuplicates = new ArrayList<String>();
		for(int j = 0; j < nodes.size(); j++) {
			Map<String, Object> node = nodes.get(j);

			// concept name passed to FE from metamodel so it comes back as a URI
			String nodeURI = node.get("conceptName").toString();
			tableName = Utility.getInstanceName(nodeURI);
			tableColumn = Utility.getClassName(nodeURI);
			tableValue = node.get("conceptValue").toString();

			boolean override = false;
			if(node.get(OVERRIDE) != null) {
				override = Boolean.parseBoolean(node.get(OVERRIDE).toString());
			}

			Map<String, String> colNamesAndType = tableColTypesHash.get(tableName.toUpperCase());
			if(colNamesAndType == null) {
				throw new IllegalArgumentException("Table name, " + tableName + ", cannot be found.");
			}
			if(!colNamesAndType.containsKey(tableColumn.toUpperCase())) {
				throw new IllegalArgumentException("Table column, " + tableColumn + ", within table name, " + tableName + ", cannot be found.");
			}

			List<Map<String, Object>> properties = (List<Map<String, Object>>)node.get("properties");
			Map<String, String> innerMap = new HashMap<String, String>();
			innerMap.put(tableColumn, tableValue);
			nodeMapping.put(tableName, innerMap);

			List<String> propNames = new ArrayList<String>();
			List<Object> propValues = new ArrayList<Object>();
			List<String> types = new ArrayList<String>();
			for(int k = 0; k < properties.size(); k++) {
				Map<String, Object> property = properties.get(k);
				String propName = Utility.getInstanceName(property.get("propertyName").toString());
				if(!colNamesAndType.containsKey(propName.toUpperCase())) {
					throw new IllegalArgumentException("Table column, " + propName + ", within table name, " + tableName + ", cannot be found.");
				}
				propNames.add(propName);
				propValues.add(property.get("propertyValue"));
				types.add(colNamesAndType.get(propName.toUpperCase()));
			}

			if(override && RDBMSEngineCreationHelper.conceptExists(engine, tableName, tableColumn, tableValue)) {
				String updateQuery = createUpdateStatement(tableName, tableColumn, tableValue, propNames, propValues, types);
				//TODO: need to enable modifying the actual instance name as opposed to only its properties.. this would set the updateQuery to never return back an empty string
				if(!updateQuery.isEmpty()) {
					try {
						engine.insertData(updateQuery);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if(!tablesToRemoveDuplicates.contains(tableName)) {
						tablesToRemoveDuplicates.add(tableName);
						colsForTablesToRemoveDuplicates.add(tableColumn);
					}
				}
			} else {
				String insertQuery = createInsertStatement(tableName, tableColumn, tableValue, propNames, propValues, types);
				try {
					engine.insertData(insertQuery);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		String table = "";
		String conceptCol = "";
		String conceptVal = "";
		String foreignKeyCol = "";
		String foreignKeyVal = "";

		Set<String> deletedRels = new HashSet<String>();
		Map<String, String> colNamesAndType = null;
		for(int r = 0; r < relationships.size(); r++) {
			Map<String, Object> relationship = relationships.get(r);

			String startURI = relationship.get("startNodeType").toString();
			String startTable = Utility.getInstanceName(startURI);
			String startCol = Utility.getClassName(startURI);

			colNamesAndType = tableColTypesHash.get(startTable.toUpperCase());
			if(colNamesAndType == null) {
				throw new IllegalArgumentException("Table name, " + startTable + ", cannot be found.");
			}
			if(!colNamesAndType.containsKey(startCol.toUpperCase())) {
				throw new IllegalArgumentException("Table column, " + startCol + ", within table name, " + startTable + ", cannot be found.");
			}

			String endURI = relationship.get("endNodeType").toString();
			String endTable = Utility.getInstanceName(endURI);
			String endCol = Utility.getClassName(endURI);

			colNamesAndType = tableColTypesHash.get(endTable.toUpperCase());
			if(colNamesAndType == null) {
				throw new IllegalArgumentException("Table name, " + endTable + ", cannot be found.");
			}
			if(!colNamesAndType.containsKey(endCol.toUpperCase())) {
				throw new IllegalArgumentException("Table column, " + endCol + ", within table name, " + endTable + ", cannot be found.");
			}
			
			String[] relVals = Utility.getInstanceName(relationship.get("relType").toString()).split("\\.");
			// use the relationship to get the information 
			// format is Title.Title.Studio.Title_FK (OTHER_TABLE_NAME, OTHER_COL_NAME, TABLE_NAME, FOREIGN_KEY)
			if(relVals[0].equalsIgnoreCase(startTable) && relVals[1].equalsIgnoreCase(startCol)) {
				table = relVals[2];
				conceptCol = endTable;
				conceptVal = relationship.get("endNodeVal").toString();
				foreignKeyCol =  relVals[3];
				foreignKeyVal = relationship.get("startNodeVal").toString();
			} else if(relVals[2].equalsIgnoreCase(startTable) && relVals[3].equalsIgnoreCase(startCol)) {
				table = relVals[0];
				conceptCol = startTable;
				conceptVal = relationship.get("endNodeVal").toString();
				foreignKeyCol =  relVals[1];
				foreignKeyVal = relationship.get("startNodeVal").toString();
			} else if (relVals[0].equalsIgnoreCase(endTable) && relVals[1].equalsIgnoreCase(endCol)) {
				table = relVals[2];
				conceptCol = startTable;
				conceptVal = relationship.get("startNodeVal").toString();
				foreignKeyCol =  relVals[3];
				foreignKeyVal = relationship.get("endNodeVal").toString();
			} else if(relVals[3].equalsIgnoreCase(endTable) && relVals[4].equalsIgnoreCase(endCol)) {
				table = relVals[0];
				conceptCol = startTable;
				conceptVal = relationship.get("startNodeVal").toString();
				foreignKeyCol =  relVals[1];
				foreignKeyVal = relationship.get("endNodeVal").toString();
			}
			
			colNamesAndType = tableColTypesHash.get(table.toUpperCase());
			if(colNamesAndType == null) {
				throw new IllegalArgumentException("Table name, " + table + ", cannot be found.");
			}
			if(!colNamesAndType.containsKey(conceptCol.toUpperCase())) {
				throw new IllegalArgumentException("Table column, " + conceptCol + ", within table name, " +table + ", cannot be found.");
			}
			if(!colNamesAndType.containsKey(foreignKeyCol.toUpperCase())) {
				throw new IllegalArgumentException("Table column, " + foreignKeyCol + ", within table name, " + table + ", cannot be found.");
			}

			boolean override = false;
			if(relationship.get(OVERRIDE) != null) {
				override = Boolean.parseBoolean(relationship.get(OVERRIDE).toString());
			}
			if(override) {
				String type = relationship.get(OVERRIDE_TYPE).toString();
				boolean deleteUnconnectedConcepts = false;
				if(relationship.get(DELETE_UNCONNECTED_CONCEPTS) != null) {
					deleteUnconnectedConcepts = Boolean.parseBoolean(relationship.get(DELETE_UNCONNECTED_CONCEPTS).toString());
				}
				boolean removeNode = false;
				if(relationship.get(REMOVE_NODE) != null) {
					removeNode = Boolean.parseBoolean(relationship.get(REMOVE_NODE).toString());
				}
				if(type.equalsIgnoreCase(UPSTREAM)) {
					if(!deletedRels.contains(table + foreignKeyCol + conceptVal)) {
						overrideUpstreamRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash, deleteUnconnectedConcepts, removeNode);
						deletedRels.add(table + foreignKeyCol + conceptVal);
					} else {
						//already did the override once, now just insert
						addRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash);
					}
				} else if(type.equalsIgnoreCase(DOWNSTREAM)){
					if(!deletedRels.contains(table + conceptCol + foreignKeyVal)) {
						overrideDownstreamRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash, deleteUnconnectedConcepts, removeNode);
						deletedRels.add(table + conceptCol + foreignKeyVal);
					} else {
						//already did the override once, now just insert
						addRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash);
					}
				}
			} else {
				// adding without any override modifications
				// method checks is value currently exists
				addRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash);
			}
		}

		//remove duplicates for all tables affected
		removeDuplicates(engine, tablesToRemoveDuplicates, colsForTablesToRemoveDuplicates);
	}

	private static void overrideUpstreamRDBMSRelationship(IEngine engine, String table, String conceptCol,
			String conceptVal, String foreignKeyCol, String foreignKeyVal,
			Map<String, Map<String, String>> tableColTypesHash, boolean deleteUnconnectedConcepts, boolean removeNode) 
	{
		StringBuilder queryBuilder = new StringBuilder();
		// override all current values that exist for the relationship to the foreign key
		queryBuilder.append("DELETE FROM ").append(table).append(" WHERE ").append(foreignKeyCol).append("='").append(foreignKeyVal).append("'");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// insert the new relationship
		addRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash);
	}

	private static void overrideDownstreamRDBMSRelationship(IEngine engine, String table, String conceptCol, 
			String conceptVal, String foreignKeyCol, String foreignKeyVal, 
			Map<String, Map<String, String>> tableColTypesHash, boolean deleteUnconnectedConcepts, boolean removeNode) 
	{
		final String TEMP_EXTENSION = "____TEMP";
		
		StringBuilder queryBuilder = new StringBuilder();
		// create a new temp table for the specific instance
		queryBuilder.append("CREATE TABLE ").append(table).append(TEMP_EXTENSION).append(" AS (SELECT DISTINCT * FROM ")
					.append(table).append(" WHERE ").append(conceptCol).append("='").append(conceptVal).append("')");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// set all the values in the foreign key column to the new value we are adding
		queryBuilder.append("UPDATE ").append(table).append(TEMP_EXTENSION).append(" SET " ).append(foreignKeyCol).append("='").append(foreignKeyVal)
					.append("' WHERE ").append(conceptCol).append("='").append(conceptVal).append("';");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// remove the duplicated
		removeDuplicates(engine, table + TEMP_EXTENSION, conceptCol);
		// delete all the current values for the instance from the table 
		queryBuilder.append("DELETE FROM ").append(table).append(" WHERE ").append(foreignKeyCol).append("='").append(foreignKeyVal).append("'");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// add all the values from the temp table into the table we care about
		queryBuilder.append("INSERT INTO ").append(table).append(" SELECT * FROM ").append(table).append(TEMP_EXTENSION);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// drop the temp table
		queryBuilder.append("DROP TABLE ").append(table).append(TEMP_EXTENSION);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void addRDBMSRelationship(IEngine engine, String table, String conceptCol, String conceptVal, 
			String foreignKeyCol, String foreignKeyVal, Map<String, Map<String, String>> tableColTypesHash) {
		// if concept already exists, need to add in manner to preserve many-to-many relationship structure
		if(RDBMSEngineCreationHelper.conceptExists(engine, table, conceptCol, conceptVal)) {
			appendRDBMSRelationship(engine, table, conceptCol, conceptVal, foreignKeyCol, foreignKeyVal, tableColTypesHash);
		} else {
			// just perform an insert statement
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append("INSERT INTO ").append(table.toUpperCase()).append(" (" ).append(conceptCol).append(", ").append(foreignKeyCol)
					.append(") VALUES ('").append(conceptVal).append("', '").append(foreignKeyCol).append("')");
			try {
				engine.insertData(queryBuilder.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void appendRDBMSRelationship(IEngine engine, String table, String conceptCol, String conceptVal, 
			String foreignKeyCol, String foreignKeyVal, Map<String, Map<String, String>> tableColTypesHash) {
		StringBuilder queryBuilder = new StringBuilder();
		// it exists, now need to find all unique values given the instance except the foreign key and append that to the table 
		final String TEMP_EXTENSION = "____TEMP";
		StringBuilder cols = new StringBuilder();
		Map<String, String> tableCols = tableColTypesHash.get(table.toUpperCase());
		// find the type of the foreign key column
		String foreignKeyType = tableCols.get(foreignKeyCol.toUpperCase());
		// get the list of all the columns except the foreign key we are looking at
		for(String columnName : tableCols.keySet()) {
			if(columnName.equals(foreignKeyCol.toUpperCase())) {
				continue;
			}
			if(cols.length()==0) {
				cols.append(columnName.toUpperCase());
			} else {
				cols.append(", ").append(columnName.toUpperCase());
			}
		}

		// create a new temp table that is the unique set of all columns for the specific instance excluding other foreign key values
		queryBuilder.append("CREATE TABLE ").append(table).append(TEMP_EXTENSION).append(" AS (SELECT DISTINCT ").append(cols.toString())
					.append(" FROM ").append(table).append(" WHERE ").append(conceptCol).append("='").append(conceptVal).append("')");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// alter the table to add a column for the new foreign key value we are adding
		queryBuilder.append("ALTER TABLE ").append(table).append(TEMP_EXTENSION).append(" ADD ").append(foreignKeyCol).append(" ").append(foreignKeyType);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// set all the values in the foreign key column to the new value we are adding
		queryBuilder.append("UPDATE ").append(table).append(TEMP_EXTENSION).append(" SET " ).append(foreignKeyCol).append("='").append(foreignKeyVal)
					.append("' WHERE ").append(conceptCol).append("='").append(conceptVal).append("';");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		//TODO: is it possible to have duplicates at this point????
		// remove the duplicated
		removeDuplicates(engine, table + TEMP_EXTENSION, conceptCol);
		// add all the values from the temp table into the table we care about
		queryBuilder.append("INSERT INTO ").append(table).append("(").append(cols).append(", ").append(foreignKeyCol).append(")").append(" SELECT * FROM ").append(table).append(TEMP_EXTENSION);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		// drop the temp table
		queryBuilder.append("DROP TABLE ").append(table).append(TEMP_EXTENSION);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void removeDuplicates(IEngine engine, List<String> tablesToRemoveDuplicates, List<String> colsForTablesToRemoveDuplicates) {
		for(int i = 0; i < tablesToRemoveDuplicates.size(); i++) {
			String tableName = tablesToRemoveDuplicates.get(i);
			String colName = colsForTablesToRemoveDuplicates.get(i);
			removeDuplicates(engine, tableName, colName);
		}
	}
	
	private static void removeDuplicates(IEngine engine, String tableName, String colName) {
		final String TEMP_EXTENSION = "____TEMP";

		// remove the duplicated
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("CREATE TABLE ").append(tableName).append(TEMP_EXTENSION).append(" AS (SELECT DISTINCT * FROM ")
					.append(tableName).append(" WHERE ").append(colName).append(" IS NOT NULL AND TRIM(").append(colName)
					.append(") <> '' )");
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		queryBuilder.append("DROP TABLE ").append(tableName);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
		queryBuilder.append("ALTER TABLE ").append(tableName).append(TEMP_EXTENSION).append(" RENAME TO ").append(tableName);
		try {
			engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		queryBuilder.setLength(0);
	}

	private static String createInsertStatement(String tableName, String tableColumn, String tableValue, List<String> propNames, List<Object> propValues, List<String> types) {
		StringBuilder insertQuery = new StringBuilder();
		insertQuery.append("INSERT INTO ");
		insertQuery.append(tableName.toUpperCase());
		insertQuery.append(" (");
		insertQuery.append(tableColumn.toUpperCase());
		if(propNames.size() > 0) {
			insertQuery.append(",");
			for(int i = 0; i < propNames.size(); i++) {
				insertQuery.append(propNames.get(i).toUpperCase());
				if(i != propNames.size() - 1) {
					insertQuery.append(",");
				}
			}
		}

		insertQuery.append(") VALUES ('");
		insertQuery.append(tableValue);
		insertQuery.append("'");

		if(propNames.size() > 0) {
			insertQuery.append(",");
			for(int i = 0; i < propValues.size(); i++) {
				Object propertyValue = propValues.get(i);
				String type = types.get(i);
				if(type.contains("VARCHAR")) {
					insertQuery.append("'");
					insertQuery.append(RdbmsQueryBuilder.escapeForSQLStatement(propertyValue.toString()));
					insertQuery.append("'");
				} else if(type.contains("INT") || type.contains("DECIMAL") || type.contains("DOUBLE") || type.contains("LONG") || type.contains("BIGINT")
						|| type.contains("TINYINT") || type.contains("SMALLINT")){
					insertQuery.append(propertyValue);
				} else  if(type.contains("DATE")) {
					Date dateValue = null;
					try {
						dateValue = GENERIC_DF.parse(propertyValue + "");
					} catch (ParseException e) {
						e.printStackTrace();
						throw new IllegalArgumentException("Input value, " + propertyValue + " for column " + propNames.get(i) + " cannot be parsed as a date.");
					}
					propertyValue = SIMPLE_DATE_DF.format(dateValue);
					insertQuery.append("'");
					insertQuery.append(propertyValue);
					insertQuery.append("'");
				} else if(type.contains("TIMESTAMP")) {
					Date dateValue = null;
					try {
						dateValue = GENERIC_DF.parse(propertyValue + "");
					} catch (ParseException e) {
						e.printStackTrace();
						throw new IllegalArgumentException("Input value, " + propertyValue + " for column " + propNames.get(i) + " cannot be parsed as a date.");
					}
					propertyValue = DATE_DF.format(dateValue);
					insertQuery.append("'");
					insertQuery.append(propertyValue);
					insertQuery.append("'");
				}
				if(i != propNames.size() - 1) {
					insertQuery.append(", ");
				}
			}
		}
		insertQuery.append(");");

		return insertQuery.toString();
	}

	private static String createUpdateStatement(String tableName, String tableColumn, String tableValue, List<String> propNames, List<Object> propValues, List<String> types) {
		if(propNames.size() == 0) {
			return "";
		}

		StringBuilder insertQuery = new StringBuilder();
		insertQuery.append("UPDATE ");
		insertQuery.append(tableName.toUpperCase());
		insertQuery.append(" SET ");
		for(int i = 0; i < propNames.size(); i++) {
			String propName = propNames.get(i).toUpperCase();
			Object propertyValue = propValues.get(i);
			String type = types.get(i);
			insertQuery.append(propName);
			insertQuery.append("=");
			if(type.contains("VARCHAR")) {
				insertQuery.append("'");
				insertQuery.append(RdbmsQueryBuilder.escapeForSQLStatement(propertyValue.toString()));
				insertQuery.append("'");
			} else if(type.contains("INT") || type.contains("DECIMAL") || type.contains("DOUBLE") || type.contains("LONG") || type.contains("BIGINT")
					|| type.contains("TINYINT") || type.contains("SMALLINT")){
				insertQuery.append(propertyValue);
			} else  if(type.contains("DATE")) {
				Date dateValue = null;
				try {
					dateValue = GENERIC_DF.parse(propertyValue + "");
				} catch (ParseException e) {
					e.printStackTrace();
					throw new IllegalArgumentException("Input value, " + propertyValue + " for column " + propNames.get(i) + " cannot be parsed as a date.");
				}
				propertyValue = SIMPLE_DATE_DF.format(dateValue);
				insertQuery.append("'");
				insertQuery.append(propertyValue);
				insertQuery.append("'");
			} else if(type.contains("TIMESTAMP")) {
				Date dateValue = null;
				try {
					dateValue = GENERIC_DF.parse(propertyValue + "");
				} catch (ParseException e) {
					e.printStackTrace();
					throw new IllegalArgumentException("Input value, " + propertyValue + " for column " + propNames.get(i) + " cannot be parsed as a date.");
				}
				propertyValue = DATE_DF.format(dateValue);
				insertQuery.append("'");
				insertQuery.append(propertyValue);
				insertQuery.append("'");
			}
			if(i != propNames.size() - 1) {
				insertQuery.append(",");
			}
		}

		insertQuery.append(" WHERE ");
		insertQuery.append(tableColumn);
		insertQuery.append("='");
		insertQuery.append(tableValue);
		insertQuery.append("'");

		return insertQuery.toString();
	}

	private static void addAuditLog(IEngine formEng, String auditLogTableName, String user, String action, String startNode, String relName, String endNode, String propName, String propValue, String timeStamp) {
		if(formEng == null || auditLogTableName == null || auditLogTableName.isEmpty()) {
			return;
		}
		user = RdbmsQueryBuilder.escapeForSQLStatement(user);
		startNode = RdbmsQueryBuilder.escapeForSQLStatement(startNode);
		relName = RdbmsQueryBuilder.escapeForSQLStatement(relName);
		endNode = RdbmsQueryBuilder.escapeForSQLStatement(endNode);
		propName = RdbmsQueryBuilder.escapeForSQLStatement(propName);
		propValue = RdbmsQueryBuilder.escapeForSQLStatement(propValue);

		String valuesBreak = "', '";
		StringBuilder insertLogStatement = new StringBuilder("INSERT INTO ");
		insertLogStatement.append(auditLogTableName).append("(USER, ACTION, START_NODE, REL_NAME, END_NODE, PROP_NAME, PROP_VALUE, TIME) VALUES('")
						.append(user).append(valuesBreak).append(action).append(valuesBreak).append(startNode).append(valuesBreak)
						.append(relName).append(valuesBreak).append(endNode).append(valuesBreak).append(propName).append(valuesBreak)
						.append(propValue).append(valuesBreak).append(timeStamp).append("')");
		try {
			formEng.insertData(insertLogStatement.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Map<String, Object> getAuditDataForEngine(String engineName) {
		Map<String, Object> retMap = new Hashtable<String, Object>();
		String auditLogTableName = RdbmsQueryBuilder.escapeForSQLStatement(RDBMSEngineCreationHelper.cleanTableName(engineName)).toUpperCase() + AUDIT_FORM_SUFFIX;
		IEngine formEng = Utility.getEngine(FORM_BUILDER_ENGINE_NAME);
		
		String query = "SELECT * FROM " + auditLogTableName;
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(formEng, query);
		String[] names = wrapper.getVariables();
		List<Object[]> data = new Vector<Object[]>();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			Object[] row = new Object[names.length];
			for(int i = 0; i < names.length; i++) {
				row[i] = ss.getVar(names[i]);
			}
			data.add(row);
		}
		
		retMap.put("headers", names);
		retMap.put("data", data);
		return retMap;
	}
	
}
