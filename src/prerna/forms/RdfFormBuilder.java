package prerna.forms;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public class RdfFormBuilder extends AbstractFormBuilder {

	protected static final Logger classLogger = LogManager.getLogger(RdfFormBuilder.class);

	/////////////////////////////////////////////RDF CODE/////////////////////////////////////////////

	protected RdfFormBuilder(IDatabaseEngine engine) {
		super(engine);
	}

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
	protected void saveFormData(
			String baseURI,
			String conceptBaseURI,
			String relationBaseURI, 
			String propertyBaseURI, 
			List<HashMap<String, Object>> nodes, 
			List<HashMap<String, Object>> relationships, 
			List<HashMap<String, Object>> removeNodes, 
			List<HashMap<String, Object>> removeRelationships)
	{
		// get all the necessary values
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

			overrideRDFRelationship(instanceSubjectURI, subject, instanceObjectURI, object, baseRelationshipURI);
		}

		// for deleting existing concepts
		for(int i = 0; i < removeNodes.size(); i++) {
			Map<String, Object> deleteConcept = removeNodes.get(i);
			conceptType = deleteConcept.get("conceptName").toString();
			conceptValue = Utility.cleanString(deleteConcept.get("conceptValue").toString(), true);
			instanceConceptURI = baseURI + "/Concept/" + Utility.getInstanceName(conceptType) + "/" + conceptValue;

			boolean removeConcept = false;
			if(deleteConcept.get("removeNode") != null) {
				removeConcept = Boolean.parseBoolean(deleteConcept.get("removeNode").toString());
			}

			if(removeConcept){
				// need to delete all properties before deleting concept
				Set<String> uriBindingList = new HashSet<String>();
				uriBindingList.add(instanceConceptURI);
				deleteAllRDFConnectionsToConcept(uriBindingList);
				removeRDFNodeAndAllProps(uriBindingList);
			} else if(deleteConcept.containsKey("properties")) {
				List<Map<String, Object>> properties = (List<Map<String, Object>>) deleteConcept.get("properties");

				for(int j = 0; j < properties.size(); j++) {
					Map<String, Object> property = properties.get(j);
					propertyURI = property.get("propertyName").toString();
					propertyValue = property.get("propertyValue");
					
					// if it is null, there is nothing to remove
					if(propertyValue == null) {
						continue;
					}
					
					// TODO: trying to account for more FE issues - delete original value being sent
					// TODO: trying to account for more FE issues - delete original value being sent
					// TODO: trying to account for more FE issues - delete original value being sent
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propertyValue, false});
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, Utility.cleanString(propertyValue.toString(), true, false, true), false});

					if(propertyValue instanceof String) {
						// check if string val is a date
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
						try {
							dateFormat.setLenient(false);
							propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
						} catch (ParseException e) {
							// TODO: trying to account for more FE issues
							// TODO: trying to account for more FE issues
							// TODO: trying to account for more FE issues
							try {
								dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
								dateFormat.setLenient(false);
								propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
							} catch (ParseException e2) {
								dateFormat = new SimpleDateFormat("MMM_dd,_yyyy");
								try {
									dateFormat.setLenient(false);
									propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
								} catch (ParseException e4) {									
									propertyValue = propertyValue.toString();
								}
							}
						}
					}

					if(propertyValue instanceof Date) {
						Date propDateValue = (Date) propertyValue;
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propDateValue, false});
						propDateValue.setHours(12);
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propDateValue, false});
						propDateValue.setHours(0);
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propDateValue, false});
					} else {
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propertyValue, false});
					}
					
					if(propertyValue instanceof String) {
						// TODO: trying to account for more FE issues
						// TODO: trying to account for more FE issues
						// TODO: trying to account for more FE issues
						try {
							Double maybeThisIsANumber = Double.parseDouble(propertyValue.toString().trim());
							this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, maybeThisIsANumber, false});
						} catch(NumberFormatException e) {
							// do nothing
						}
					}
					// ugh... we need to push forms
					// values being passed are not properly keeping track of things that have underscores and things that don't
					// just going to try both versions
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{instanceConceptURI, propertyURI, Utility.cleanString(propertyValue.toString(), true, false, true), false});

					// add audit log statement
					Calendar cal = Calendar.getInstance();
					String currTime = DATE_DF.format(cal.getTime());
					addAuditLog(REMOVE, instanceConceptURI, "", "", propertyURI, propertyValue + "", currTime);
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{propertyURI, RDF.TYPE, propertyBaseURI, true});
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

			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, RDF.TYPE, subject, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceObjectURI, RDF.TYPE, object, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, relationBaseURI, instanceObjectURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, baseRelationshipURI, instanceObjectURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceSubjectURI, instanceRelationshipURI, instanceObjectURI, true});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(ADD, instanceSubjectURI, baseRelationshipURI, instanceObjectURI, "", "", currTime);
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{baseRelationshipURI, RDFS.SUBPROPERTYOF, relationBaseURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDFS.SUBPROPERTYOF, baseRelationshipURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDFS.SUBPROPERTYOF, relationBaseURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDF.TYPE, RDF.PROPERTY, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, RDFS.LABEL, instanceRel, false});


			// add relationship properties
			if(relationship.containsKey("properties")) {
				List<Map<String, Object>> properties = (List<Map<String, Object>>) relationship.get("properties");
				for(int j = 0; j < properties.size(); j++) {
					Map<String, Object> property = properties.get(j);
					propertyValue = property.get("propertyValue");
					if(propertyValue instanceof String) {
						// check if string val is a date
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
						try {
							dateFormat.setLenient(true);
							propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
							((Date) propertyValue).setHours(12);
						} catch (ParseException e) {
							propertyValue = propertyValue.toString();
						}
					}
					propertyURI = property.get("propertyName").toString();

					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelationshipURI, propertyURI, propertyValue, false});
					// add audit log statement
					cal = Calendar.getInstance();
					currTime = DATE_DF.format(cal.getTime());
					addAuditLog(ADD, "", instanceRelationshipURI, "", propertyURI, propertyValue + "", currTime);
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyURI, RDF.TYPE, propertyBaseURI, true});
				}
			}
		}

		//for adding concepts and properties of nodes
		for(int i = 0; i < nodes.size(); i++) {
			Map<String, Object> concept = nodes.get(i);
			conceptType = concept.get("conceptName").toString();
			conceptValue = Utility.cleanString(concept.get("conceptValue").toString(), true);

			instanceConceptURI = baseURI + "/Concept/" + Utility.getInstanceName(conceptType) + "/" + conceptValue;
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceConceptURI, RDF.TYPE, conceptType, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceConceptURI, RDFS.LABEL, conceptValue, false});
			if(concept.containsKey("properties")) {
				List<HashMap<String, Object>> properties = (List<HashMap<String, Object>>) concept.get("properties");

				for(int j = 0; j < properties.size(); j++) {
					Map<String, Object> property = properties.get(j);
					propertyValue = property.get("propertyValue");
					if(propertyValue instanceof String) {
						// check if string val is a date
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						try {
							dateFormat.setLenient(true);
							propertyValue= (Date) dateFormat.parse(((String) propertyValue).trim());
							if(((Date) propertyValue).getYear() < 99) { // 99 because the rule is 1999 but getYear returns the value minus 1990
								// TODO: this is a FE bug on forms
								// just ignore old dates and don't save them
								continue;
							}
							// always set the time to 12
							((Date) propertyValue).setHours(12);
						} catch (ParseException e) {
							propertyValue = propertyValue.toString();
						}
					}
					if(propertyValue instanceof String) {
						// TODO: trying to account for more FE issues
						// TODO: trying to account for more FE issues
						// TODO: trying to account for more FE issues
						try {
							propertyValue = Double.parseDouble(propertyValue.toString().trim());
						} catch(NumberFormatException e) {
							// do nothing
						}
					}
					propertyURI = property.get("propertyName").toString();

					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceConceptURI, propertyURI, propertyValue, false});
					// add audit log statement
					Calendar cal = Calendar.getInstance();
					String currTime = DATE_DF.format(cal.getTime());
					addAuditLog(ADD, instanceConceptURI, "", "", propertyURI, propertyValue + "", currTime);
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propertyURI, RDF.TYPE, propertyBaseURI, true});
				}
			}
		}
	}

	/**
	 * Deletes the relationship and relationship properties that exist between an instance and all other instances of a specified type
	 * @param instanceSubjectURI
	 * @param subjectTypeURI
	 * @param instanceObjectURI
	 * @param objectTypeURI
	 * @param baseRelationshipURI
	 */
	private void overrideRDFRelationship(
			String instanceSubjectURI,
			String subjectTypeURI,
			String instanceObjectURI,
			String objectTypeURI,
			String baseRelationshipURI)
	{
		// generate the query to override existing rdf relationships
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?SUB ?PRED ?OBJ ?LABEL ?PROP ?VAL WHERE { ");
		query.append("BIND(<" + instanceSubjectURI + "> AS ?SUB) ");
		query.append("BIND(<" + instanceObjectURI + "> AS ?OBJ) ");
		//         query.append("{?SUB <").append(RDF.TYPE).append("> <" + subjectTypeURI + ">} ");
		//         query.append("{?OBJ <").append(RDF.TYPE).append("> <" + objectTypeURI + ">} ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <" + baseRelationshipURI + ">} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("OPTIONAL{ ");
		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
		query.append("{?PRED ?PROP ?VAL} ");
		query.append("} }");

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(this.engine, query.toString());
		String[] names = wrapper.getVariables();
		String baseRelationURI = "http://semoss.org/ontologies/Relation";
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String subURI = ss.getRawVar(names[0]) + "";
			String predURI = ss.getRawVar(names[1]) + "";
			String objURI = ss.getRawVar(names[2]) + "";
			Object label = ss.getVar(names[3]);
			Object propURI = ss.getRawVar(names[4]);
			Object propVal = ss.getVar(names[5]);

			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, predURI, objURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, baseRelationshipURI, objURI, true});
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, baseRelationURI, objURI, true});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(REMOVE, subURI, baseRelationshipURI, objURI, "", "", currTime);
			
			// do not delete predUris if not instance lvl
			if(predURI.startsWith("http://semoss.org/ontologies/Relation/") && Utility.getClassName(predURI).equals("Relation")) {
				continue;
			}
			// gotta be smart since instance lvl preds are not actually unique
			// run a query and see
			StringBuilder miniQ = new StringBuilder("select distinct ?s ?p ?o where {");
			miniQ.append("bind(<").append(predURI).append("> as ?p)");
			miniQ.append("{?s ?p ?o} }");

			boolean otherUris = false;
			IRawSelectWrapper wrapper2 = null;
			try {
				wrapper2 = WrapperManager.getInstance().getRawWrapper(this.engine, miniQ.toString());
				if(wrapper2.hasNext()) {
					otherUris = true;
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper2 != null) {
					try {
						wrapper2.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			if(!otherUris) {
				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRelationshipURI, true});
				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRelationURI, true});
				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDF.TYPE, RDF.PROPERTY, true});
				if(label != null && !label.toString().isEmpty()) {
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.LABEL, label.toString(), false});
				}
				if(propURI != null && !propURI.toString().isEmpty()) {
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, propURI.toString(), propVal, false});
					// add audit log statement
					currTime = DATE_DF.format(cal.getTime());
					addAuditLog(REMOVE, "", predURI, "", propURI.toString(), propVal + "", currTime);
				}
			}
		}
	}

	private void deleteAllRDFConnectionsToConcept(Set<String> uriBindingList) {
		String[] queries = new String[]{
				generateDeleteAllRDFConnectionsToConceptQuery(uriBindingList, true),
				generateDeleteAllRDFConnectionsToConceptQuery(uriBindingList, false)};

		String baseRel = "http://semoss.org/ontologies/Relation";
		for(String query : queries) {
			if(query == null) {
				continue;
			}
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(this.engine, query);
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext()) {
				ISelectStatement ss = wrapper.next();
				String subURI = ss.getRawVar(names[0]) + "";
				String predURI = ss.getRawVar(names[1]) + "";
				String objURI = ss.getRawVar(names[2]) + "";
				Object label = ss.getVar(names[3]);
				Object propURI = ss.getRawVar(names[4]);
				Object propVal = ss.getVar(names[5]);

				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, predURI, objURI, true});
				// add audit log statement
				Calendar cal = Calendar.getInstance();
				String currTime = DATE_DF.format(cal.getTime());
				addAuditLog(REMOVE, subURI, predURI, objURI, "", "", currTime);
				if(label != null && !label.toString().isEmpty()) {
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.LABEL, label, false});
				}
				if(propURI != null && !propURI.toString().isEmpty()) {
					this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, propURI, propVal, false});
					// add audit log statement
					cal = Calendar.getInstance();
					currTime = DATE_DF.format(cal.getTime());
					addAuditLog(REMOVE, "", predURI, "", propURI.toString(), propVal + "", currTime);
				}

				// need to do a lot of stuff for relationships
				// ignore the base Relation
				if(!predURI.equals(baseRel)) {
					// remove the prefix so we only get RelType/Instance
					// assuming the instance is there..
					String suffix = predURI.replaceAll(".*ontologies/Relation/", "");
					if(suffix.contains("/")) {
						String[] split = suffix.split("/");
						String relType = split[0];

						// delete the sub properties around the instance
						String baseRelationURI = baseRel + "/" + relType;
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRel, true});
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRelationURI, true});
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{predURI, RDF.TYPE, RDF.PROPERTY, true});

						// and delete the base rel directly between the subject and object
						this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{subURI, baseRelationURI, objURI, true});
					}
				}
			}
		}
		// lastly, remove the node and all its props
		removeRDFNodeAndAllProps(uriBindingList);
	}

	/**
	 * 
	 * @param conceptURI
	 * @param downstream
	 * @return
	 */
	private String generateDeleteAllRDFConnectionsToConceptQuery(Set<String> conceptURI, boolean downstream) {
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

	private void removeRDFNodeAndAllProps(Set<String> uriBindingList) {
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

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(this.engine, query.toString());
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String nodeURI = ss.getRawVar(names[0]) + "";
			String propURI = ss.getRawVar(names[1]) + "";
			Object propVal = ss.getVar(names[2]);

			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, propURI, propVal, false});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(REMOVE, nodeURI, "", "", propURI, propVal + "", currTime);
		}

		// deletes the instances
		String semossBaseConcept = "http://semoss.org/ontologies/Concept";
		for(String nodeURI : uriBindingList) {
			String typeURI = semossBaseConcept + "/" + Utility.getClassName(nodeURI);
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, RDF.TYPE, typeURI, true});
			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(REMOVE, nodeURI, "", "", "", "", currTime);
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, RDFS.LABEL, Utility.getInstanceName(nodeURI), false});
		}
	}

	/**
	 * Since this is an RDF engine, the inputs are full URIs
	 * @param origName
	 * @param newName
	 * @param deleteInstanceBoolean
	 */
	@Override
	public void modifyInstanceValue(String origName, String newName, boolean deleteInstanceBoolean) {
		String newInstanceName = Utility.getInstanceName(newName);

		// get all the subjects
		List<Object[]> upTriples = new Vector<Object[]>();
		String upQuery = "SELECT DISTINCT ?s ?p ?o WHERE {"
				+ "BIND(<" + origName + "> AS ?s)"
				+ "{?s ?p ?o}"
				+ "}";
		IRawSelectWrapper upIt = null;
		try {
			upIt = WrapperManager.getInstance().getRawWrapper(this.engine , upQuery);
			storeValues(upIt, upTriples);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(upIt != null) {
				try {
					upIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// get all the objects
		List<Object[]> downTriples = new Vector<Object[]>();
		String downQuery = "SELECT DISTINCT ?s ?p ?o WHERE {"
				+ "BIND(<" + origName + "> AS ?o)"
				+ "{?s ?p ?o}"
				+ "}";

		IRawSelectWrapper downIt = null;
		try {
			downIt = WrapperManager.getInstance().getRawWrapper(this.engine , downQuery);
			storeValues(downIt, downTriples);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(downIt != null) {
				try {
					downIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// now go through and modify where necessary
		deleteTriples(upTriples);
		deleteTriples(downTriples);

		if(!deleteInstanceBoolean) {
			addUpTriples(upTriples, newName, newInstanceName);
			addDownTriples(downTriples, newName);
		}

		this.engine.commit();		
	}

	protected void addUpTriples(List<Object[]> triples, String newUri, String newName) {
		int size = triples.size();
		for(int i = 0; i < size; i++) {
			Object[] data = triples.get(i);
			// we replace the subject with the new uri we are adding
			Object[] statement = new Object[4];
			statement[0] = newUri;
			statement[1] = data[1];
			statement[2] = data[2];
			statement[3] = data[3];

			//handles if the instance of the object is a date. The doAction (used for add and delete, needs a specific date object for dates)
			try {
				statement[2] = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").parse((String) statement[2]);
			} catch (Exception e) {

			}

			// need to take into consideration if this is the label we add for each node
			if(statement[1].toString().equals(RDFS.LABEL.stringValue())) {
				statement[2] = newName;
				statement[3] = false;
			}

			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, statement);

			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			addAuditLog(ADD, statement[0].toString(), statement[1].toString(), statement[2].toString(), "", "", currTime);
		}
	}

	protected void addDownTriples(List<Object[]> triples, String newUri) {
		int size = triples.size();
		for(int i = 0; i < size; i++) {
			Object[] data = triples.get(i);
			// we replace the object with the new uri we are adding
			Object[] statement = new Object[4];
			statement[0] = data[0];
			statement[1] = data[1];
			statement[2] = newUri;
			statement[3] = data[3];

			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, statement);

			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			// determine if we are deleting a concept or a property
			// true means it is a relationship
			if((boolean) statement[3] == true ) {	
				addAuditLog(ADD, statement[0].toString(), statement[1].toString(), statement[2].toString(), "", "", currTime);
			} else {
				addAuditLog(ADD, statement[0].toString(), "", "", statement[1].toString(), statement[2].toString(), currTime);
			}
		}
	}

	protected void deleteTriples(List<Object[]> triples) {
		int size = triples.size();
		for(int i = 0; i < size; i++) {
			Object[] statement = triples.get(i);
			//handles if the instance of the object is a date. The doAction (used for add and delete, needs a specific date object for dates)
			try {
				statement[2] = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").parse((String) statement[2]);
			} catch (Exception e) {
				// ignore
			}
			this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, statement);

			// add audit log statement
			Calendar cal = Calendar.getInstance();
			String currTime = DATE_DF.format(cal.getTime());
			// determine if we are deleting a concept or a property
			// true means it is a relationship
			if((boolean) statement[3] == true ) {
				addAuditLog(REMOVE, statement[0].toString(), statement[1].toString(), statement[2].toString(), "", "", currTime);
			} else {
				addAuditLog(REMOVE, statement[0].toString(), "", "", statement[1].toString(), statement[2].toString(), currTime);
			}
		}
	}

	/**
	 * Go through the iterator and add it to the list
	 * @param it
	 * @param listToAdd
	 */
	protected void storeValues(IRawSelectWrapper it, List<Object[]> listToAdd) {
		while(it.hasNext()) {
			IHeadersDataRow datarow = it.next();
			//instance uri
			Object[] rawTriple = datarow.getRawValues();
			//only instance name
			Object[] cleanTriple = datarow.getValues();

			// we need to also consider
			// if the last thing is a literal
			// or if it is a uri
			Object[] adjustedTriple = new Object[4];
			for(int i = 0; i < 3; i++) {
				adjustedTriple[i] = rawTriple[i];
			}

			// if its a literal
			// use the clean value
			// checks the relationship to see if this a property or an RDF label
			// NOTE: There is a "/" after contains, because some concepts have the relationship: contains, so need to make sure just properties are grabbed
			if(adjustedTriple[1].toString().startsWith("http://semoss.org/ontologies/Relation/Contains/") 
					|| adjustedTriple[1].toString().equals(RDFS.LABEL.toString()) ) {
				adjustedTriple[2] = cleanTriple[2];
				adjustedTriple[3] = false;
			} else {
				adjustedTriple[3] = true;
			}

			listToAdd.add(adjustedTriple);
		}
	}

	@Override
	public void certifyInstance(String conceptType, String instanceName) {
		Calendar cal = Calendar.getInstance();
		String currTime = null;

		String instanceUri = this.engine.getNodeBaseUri() + conceptType + "/" + instanceName;
		String versionPropUri = Constants.PROPERTY_URI + "CertificationVersion";
		String timePropUri = Constants.PROPERTY_URI + "CertificationDate";
		String userPropUri = Constants.PROPERTY_URI + "CertificationUsername";

		// 1) delete old values
		Double oldVersion = null;
		String getPrevValuesQuery = "select distinct ?i ?v ?t ?u where {BIND(<" + instanceUri + "> as ?i) "
				+ "{?i <" + versionPropUri + "> ?v}"
				+ "{?i <" + timePropUri + "> ?t}"
				+ "{?i <" + userPropUri + "> ?u}"
				+ "}";

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(this.engine, getPrevValuesQuery);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] uris = row.getRawValues();
				Object[] clean = row.getValues();

				Object[] statement = new Object[4];
				statement[0] = uris[0];
				statement[1] = versionPropUri;
				// for properties, always grab clean output
				// otherwise, you will get double quotes around stuff
				statement[2] = clean[1];
				statement[3] = false;
				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, statement);
				// audit
				currTime = DATE_DF.format(cal.getTime());
				addAuditLog(REMOVE, statement[0].toString(), "", "", statement[1].toString(), statement[2].toString(), currTime);

				// repeat for all the props
				statement[1] = timePropUri;
				if(clean[2] instanceof SemossDate) {
					statement[2] = ((SemossDate) clean[2]).getDate();
				} else {
					statement[2] = clean[2];
				}
				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, statement);
				// audit
				currTime = DATE_DF.format(cal.getTime());
				addAuditLog(REMOVE, statement[0].toString(), "", "", statement[1].toString(), statement[2].toString(), currTime);

				statement[1] = userPropUri;
				statement[2] = clean[3];
				this.engine.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, statement);
				// audit
				currTime = DATE_DF.format(cal.getTime());
				addAuditLog(REMOVE, statement[0].toString(), "", "", statement[1].toString(), statement[2].toString(), currTime);

				// get the old version so we can update it
				oldVersion = Double.parseDouble(clean[1] + "");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// 2)  add new values
		if(oldVersion == null) {
			oldVersion = 0.0;
		}
		Double newVersion = oldVersion + 1;

		Object[] statement = new Object[4];
		statement[0] = instanceUri;
		statement[1] = versionPropUri;
		statement[2] = newVersion;
		statement[3] = false;
		this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, statement);
		// audit
		currTime = DATE_DF.format(cal.getTime());
		addAuditLog(ADD, instanceUri, "", "", versionPropUri, newVersion + "", currTime);

		// repeat for other props
		statement[1] = userPropUri;
		statement[2] = this.user;
		this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, statement);
		// audit
		currTime = DATE_DF.format(cal.getTime());
		addAuditLog(ADD,instanceUri, "", "", userPropUri, this.user, currTime);

		statement[1] = timePropUri;
		statement[2] = cal.getTime();
		this.engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, statement);
		// audit
		currTime = DATE_DF.format(cal.getTime());
		addAuditLog(ADD, instanceUri, "", "", timePropUri, currTime, currTime);

		// commit engine
		this.engine.commit();

		// add to certified log
		currTime = DATE_DF.format(cal.getTime());
		addAuditLog(ADMIN_SIGN_OFF, instanceUri, "", "", versionPropUri, newVersion + "", currTime);
		addAuditLog(ADMIN_SIGN_OFF, instanceUri, "", "", userPropUri, this.user, currTime);
		addAuditLog(ADMIN_SIGN_OFF, instanceUri, "", "", timePropUri, currTime, currTime);
	}
}
