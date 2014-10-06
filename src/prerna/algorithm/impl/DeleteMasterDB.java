package prerna.algorithm.impl;

import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.model.BigdataValueImpl;

/**
 * Deletes an engine from the master database.
 * @author ksmart
 */
public class DeleteMasterDB {
	private static final Logger logger = LogManager.getLogger(CreateMasterDB.class.getName());

	//variables for the master database
	private String masterDBName = "MasterDatabase";
	private BigDataEngine masterEngine;
	
	//uri variables
	private static final String semossURI = "http://semoss.org/ontologies";
	private static final String semossConceptURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
	private static final String semossRelationURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
	private static final String resourceURI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	private static final String mcBaseURI = semossConceptURI+"/MasterConcept";
	private static final String mccBaseURI = semossConceptURI+"/MasterConceptConnection";
	private static final String keywordBaseURI = semossConceptURI+"/Keyword";
	private static final String engineBaseURI = semossConceptURI+"/Engine";
	private static final String engineInsightBaseURI = semossRelationURI + "/Engine:Insight";
	private static final String enginePerspectiveBaseURI = semossRelationURI + "/Engine:Perspective";
	private static final String engineKeywordBaseURI = semossRelationURI + "/Has";
	private static final String mcKeywordBaseURI = semossRelationURI + "/ConsistsOf";
	private static final String engineMCCBaseURI = semossRelationURI + "/Has";
	private static final String mccToMCBaseURI = semossRelationURI + "/To";
	private static final String mccFromMCBaseURI = semossRelationURI + "/From";
	private final static String engineServerBaseURI = semossRelationURI + "/HostedOn";
	private final static String propURI = semossRelationURI + "/" + "Contains";
	private final static String similarityPropURI = propURI + "/" + "SimilarityScore";
	
	//queries for relations to delete
	private static final String enginePerspectiveQuery = "SELECT DISTINCT ?Engine ?p ?Perspective ?Label WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>}{?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label} {?Engine ?p ?Perspective}}";
	private static final String engineInsightQuery = "SELECT DISTINCT ?Engine ?p ?Insight ?Label WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>}{?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label} {?Engine ?p ?Insight}}";
	private static final String engineKeywordRelationQuery = "SELECT DISTINCT ?Engine ?p ?Keyword ?Label WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}{?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label} {?Engine ?p ?Keyword}}";
	private static final String mcKeywordRelationQuery = "SELECT DISTINCT ?MasterConcept ?p ?Keyword ?Label ?Score WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}{?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label}{?p <http://semoss.org/ontologies/Relation/Contains/SimilarityScore> ?Score} {?MasterConcept ?p ?Keyword} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}FILTER(!BOUND(?Engine))}";
	private static final String engineMCCRelationQuery = "SELECT DISTINCT ?Engine ?p ?MasterConceptConnection ?Label WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>}{?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label} {?Engine ?p ?MasterConceptConnection}}";
	private static final String mccToMCRelationQuery = "SELECT DISTINCT ?MasterConceptConnection ?p ?MasterConcept ?Label WHERE {{?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?p <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/To>;} {?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label}{?MasterConceptConnection ?p ?MasterConcept} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection}}FILTER(!BOUND(?Engine))}";
	private static final String mccFromMCRelationQuery = "SELECT DISTINCT ?MasterConceptConnection ?p ?MasterConcept ?Label WHERE {{?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?p <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/From>;} {?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label} {?MasterConceptConnection ?p ?MasterConcept} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection}}FILTER(!BOUND(?Engine))}";
	private static final String engineServerRelationQuery = "SELECT DISTINCT ?Engine ?p ?Server ?Label WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Server <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Server>} {?p <http://www.w3.org/2000/01/rdf-schema#label> ?Label} {?Engine ?p ?Server}}";
	
	//queries for nodes to delete
	private static final String loneKeywordQuery = "SELECT DISTINCT ?Keyword ?Label WHERE {{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Keyword <http://www.w3.org/2000/01/rdf-schema#label> ?Label} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}FILTER(!BOUND(?Engine))}";
	private static final String loneMCQuery = "SELECT DISTINCT ?MasterConcept ?Label WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?MasterConcept <http://www.w3.org/2000/01/rdf-schema#label> ?Label} OPTIONAL{{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?Keyword}}FILTER(!BOUND(?Keyword))}";
	private static final String loneMCCQuery = "SELECT DISTINCT ?MasterConceptConnection ?Label WHERE {{?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>}{?MasterConceptConnection <http://www.w3.org/2000/01/rdf-schema#label> ?Label}  OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection}}FILTER(!BOUND(?Engine))}";
	private static final String loneServerQuery = "SELECT DISTINCT ?Server ?p ?o WHERE {{?Server <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Server>} {?Server ?p ?o} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/HostedOn> ?Server}}FILTER(!BOUND(?Engine))}";
	
	/**
	 * Deletes an engine from the master database.
	 * Uses QuestionAdministrator to remove all perspectives, insights, and params associated with the engine from Master DB.
	 * Removes any keywords, master concept connections, and master concepts that no other engines use, otherwise leaves them alone.
	 * @param engineName	String instance name of the engine to be deleted
	 */
	public void deleteEngine(String engineName) {
		//instantiate the master database based on default, or what name is given for it
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		//delete all relationships between this engine and its insights
		String engineInsightQueryFilled = engineInsightQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineInsightQueryFilled,engineInsightBaseURI);

		//create an engine that can be used by question administrator to remove all the perspectives, insights, and params for this database
		RDFFileSesameEngine eng = new RDFFileSesameEngine();
		eng.setEngineName(engineName);
		eng.setEngineURI2Name(engineBaseURI+"/"+engineName);
		eng.createInsightBase();
		RDFFileSesameEngine insightBaseXML = eng.getInsightBaseXML();
		insightBaseXML.setRC(masterEngine.rc);
		insightBaseXML.setSC(masterEngine.sc);
		insightBaseXML.setVF(masterEngine.vf);

		//query for all the perspectives to be deleted using question administrator
		Vector<String> perspectiveList = eng.getPerspectivesURI();
		
		//delete all relationships between this engine and its perspectives
		String enginePerspectiveQueryFilled = enginePerspectiveQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(enginePerspectiveQueryFilled,enginePerspectiveBaseURI);

		//Use question administrator to remove all the perspectives, insights, and params for this database
		//set the engine, delete all from each perspective and then store the rc and sc.
		QuestionAdministrator qa = new QuestionAdministrator(eng);
		qa.selectedEngine = engineName;
		qa.setEngineURI2(engineBaseURI + "/" + engineName);
		for(String pers : perspectiveList) {
			qa.deleteAllFromPerspective(pers);
		}
		masterEngine.rc = (SailRepositoryConnection)(qa.getInsightBaseXML().getRC());
		masterEngine.sc = (SailConnection)(qa.getInsightBaseXML().getSC());
				
		//delete all relationships between this engine and its keywords
		String engineKeywordRelationQueryFilled = engineKeywordRelationQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineKeywordRelationQueryFilled,engineKeywordBaseURI);
		
		//if there are any "lone" keywords (they are not attached to an engine)
		//this keyword will be deleted, so delete any relationships that exist to it
		//i.e. delete the mc to keyword relationship for keywords with no engine
		deleteRelationship(mcKeywordRelationQuery,mcKeywordBaseURI,similarityPropURI);

		//delete the "lone" keyword itself now that all relations are gone
		deleteNodes(loneKeywordQuery,keywordBaseURI);
		
		//delete all relationships between this engine and its mccs
		String engineMCCRelationQueryFilled = engineMCCRelationQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineMCCRelationQueryFilled,engineMCCBaseURI);
		
		//if there are any "lone" mccs (they are not attached to an engine)
		//this mcc will be deleted, so delete any relationships that exist to it
		//i.e. delete the mcc to mc relationship for mccs with no engine AND delete the mcc from mc relationship for mccs with no engine
		deleteRelationship(mccToMCRelationQuery,mccToMCBaseURI);
		deleteRelationship(mccFromMCRelationQuery,mccFromMCBaseURI);
		
		//delete the "lone" MCC now that all the relations are gone
		deleteNodes(loneMCCQuery,mccBaseURI);
		
		//if there are any "lone" mcs (they have no keywords), delete them
		deleteNodes(loneMCQuery,mcBaseURI);
		
		//remove the 3 engine triples
		String engineURI = engineBaseURI + "/" + engineName;
		try {
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDFS.LABEL, masterEngine.vf.createLiteral(engineName));
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDF.TYPE, masterEngine.vf.createURI(resourceURI));
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDF.TYPE, masterEngine.vf.createURI(semossConceptURI));
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDF.TYPE, masterEngine.vf.createURI(engineBaseURI));
		} catch (SailException e) {
			logger.error("Could not remove engine type and label triples");
		}
		logger.info("Finished desktop delete");

	}
	
	/**
	 * Deletes an engine registered through the web.
	 * Uses QuestionAdministrator to remove all perspectives, insights, and params associated with the engine from Master DB.
	 * Removes any keywords, master concept connections, and master concepts that no other engines use, otherwise leaves them alone.
	 * Removes the server and baseURI if no other engines use, otherwise leaves them alone.
	 * @param engineName
	 * @return
	 */
	public String deleteEngineWeb(String engineName) {
		//delete all relationships between this engine and its server
		String engineServerRelationQueryFilled = engineServerRelationQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineServerRelationQueryFilled,engineServerBaseURI);
		
		//if there are any "lone" servers (they are not attached to an engine)
		// delete the server properties and node itself
		deleteAllTriples(loneServerQuery);
		
		deleteEngine(engineName);
		
		return "success";
	}

	/**
	 * Runs a query and deletes all the nodes returned.
	 * Query output should be of the form ?subject ?label,
	 * where subject is the node to be deleted and ?label is its stored label.
	 * @param query		String representing a query that returns a node and its label for deletion
	 * @param baseURI	String representing the baseURI of the nodes returned and to be deleted
	 */
	private void deleteNodes(String query,String baseURI) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			Object s = sjss.getRawVar(names[0]);
			String label = (String)sjss.getVar(names[1]);
			
			BigdataURIImpl sURI = null;
			if(s instanceof BigdataURIImpl) {
				sURI = ((BigdataURIImpl)s);
				
				try {
					masterEngine.sc.removeStatements(sURI, RDFS.LABEL, masterEngine.vf.createLiteral(label));
					masterEngine.sc.removeStatements(sURI, RDF.TYPE, masterEngine.vf.createURI(resourceURI));
					masterEngine.sc.removeStatements(sURI, RDF.TYPE, masterEngine.vf.createURI(semossConceptURI));
					masterEngine.sc.removeStatements(sURI, RDF.TYPE, masterEngine.vf.createURI(baseURI));
					logger.debug("Removed Node ::: " + sURI);
				} catch (SailException e) {
					logger.error("Could not delete node ::: " + sURI);
				} 
			}
			else {
				logger.error("Could not cast query results to delete node ::: " + s.toString());
			}
		}
	}
	
	/**
	 * Runs a query and deletes all the relationships returned.
	 * Query output should be of the form ?subject ?predicate ?object ?label, where:
	 * ?predicate is the relation to be deleted between ?subject and ?object
	 * and ?label is the stored label
	 * @param query		String representing a query that returns a relation, its label, and an optional property for deletion
	 * @param relationBaseURI	String representing the baseURI of the relations returned and to be deleted	
	 */
	private void deleteRelationship(String query, String relationBaseURI) {
		deleteRelationship(query,relationBaseURI,null);
	}
	
	/**
	 * Runs a query and deletes all the relationships returned.
	 * Query output should be of the form ?subject ?predicate ?object ?label and (optional) ?property, where:
	 * ?predicate is the relation to be deleted between ?subject and ?object,
	 * ?label is the stored label and ?property is optional but if included deletes a property on the relationship
	 * @param query		String representing a query that returns a relation, its label, and an optional property for deletion
	 * @param relationBaseURI	String representing the baseURI of the relations returned and to be deleted	
	 * @param propURI	String representing the uri of the property on the relation to delete.
	 */
	private void deleteRelationship(String query, String relationBaseURI,String propURI) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			SesameJenaSelectStatement sjss = wrapper.next();
			Object s = sjss.getRawVar(names[0]);
			Object p = sjss.getRawVar(names[1]);
			String pInstance = (String)sjss.getVar(names[1]);
			Object o = sjss.getRawVar(names[2]);
			String label = (String)sjss.getVar(names[3]);
			
			//if the query includes a property and a propURI was provided as input
			BigdataValueImpl prop = null;
			if(names.length>4&&propURI!=null) {
				Object relProp = sjss.getRawVar(names[4]);
				if(relProp instanceof BigdataValueImpl)
					prop = (BigdataValueImpl)relProp;
				else
					logger.error("Could not cast query results for relation property "+propURI);
			}
			
			if(s instanceof BigdataURIImpl && p instanceof BigdataURIImpl && o instanceof BigdataURIImpl) {
				BigdataURIImpl subj = ((BigdataURIImpl)s);
				BigdataURIImpl pred = ((BigdataURIImpl)p);
				BigdataURIImpl obj = ((BigdataURIImpl)o);
				try {
					masterEngine.sc.removeStatements(subj, pred, obj);
					masterEngine.sc.removeStatements(subj, masterEngine.vf.createURI(relationBaseURI), obj);
					masterEngine.sc.removeStatements(subj, masterEngine.vf.createURI(semossRelationURI), obj);
					masterEngine.sc.removeStatements(pred, RDFS.SUBPROPERTYOF, masterEngine.vf.createURI(semossRelationURI));
					masterEngine.sc.removeStatements(pred, RDFS.SUBPROPERTYOF, masterEngine.vf.createURI(relationBaseURI));
					masterEngine.sc.removeStatements(pred, RDFS.SUBPROPERTYOF, pred);
					masterEngine.sc.removeStatements(pred, RDFS.LABEL, masterEngine.vf.createLiteral(pInstance));
					masterEngine.sc.removeStatements(pred, RDFS.LABEL, masterEngine.vf.createLiteral(label));
					masterEngine.sc.removeStatements(pred, RDF.TYPE, masterEngine.vf.createURI(Constants.DEFAULT_PROPERTY_URI));
					masterEngine.sc.removeStatements(pred, RDF.TYPE, masterEngine.vf.createURI(resourceURI));
					if(prop!=null) {
						masterEngine.sc.removeStatements(pred, masterEngine.vf.createURI(propURI),prop);
					}
					logger.debug("Removed relationship ::: " + subj + " ::: " + pred + " ::: " + obj);
				} catch (SailException e) {
					logger.error("Could not delete relationship  ::: " + subj + " ::: " + pred + " ::: " + obj);
				} 
			}
			else {
				logger.error("Could not cast query results for triple ::: " + s.toString() + " ::: " + p.toString() + " ::: "+o.toString());
			}
		

		}
	}
	/**
	 * Deletes all triples returned from the query.
	 * @param query
	 */
	private void deleteAllTriples(String query) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			Object s = sjss.getRawVar(names[0]);
			Object p = sjss.getRawVar(names[1]);
			Object o = sjss.getRawVar(names[2]);
			try {
				masterEngine.sc.removeStatements((Resource)s, (URI)p, (Value)o);
				logger.info("Removed statement ::: " + s + " ::: " + p + " ::: "+ o);
			} catch (SailException e) {
				logger.error("Sail Exception in removing statement ::: " + s + " ::: " + p + " ::: "+ o);
			}
		}
	}

}
