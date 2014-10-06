package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.model.BigdataValueImpl;

import prerna.error.EngineException;
import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteMasterDB {
	private static final Logger logger = LogManager.getLogger(CreateMasterDB.class.getName());

	//variables for creating the db
	static final String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	static final String fileSeparator = System.getProperty("file.separator");
	String masterDBName = "MasterDatabase";
	String masterDBFileName="db" + fileSeparator + masterDBName;
	BigDataEngine masterEngine;
	
	//uri variables
	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String semossConceptURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
	protected final static String semossRelationURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
	protected final static String resourceURI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	protected final static String mcBaseURI = semossConceptURI+"/MasterConcept";
	protected final static String mccBaseURI = semossConceptURI+"/MasterConceptConnection";
	protected final static String keywordBaseURI = semossConceptURI+"/Keyword";
	protected final static String engineBaseURI = semossConceptURI+"/Engine";
	protected final static String serverBaseURI = semossConceptURI+"/Server";
	protected final static String engineInsightBaseURI = semossRelationURI + "/Engine:Insight";
	protected final static String enginePerspectiveBaseURI = semossRelationURI + "/Engine:Perspective";
	protected final static String engineKeywordBaseURI = semossRelationURI + "/Has";
	protected final static String mcKeywordBaseURI = semossRelationURI + "/ConsistsOf";
	protected final static String engineMCCBaseURI = semossRelationURI + "/Has";
	protected final static String mccToMCBaseURI = semossRelationURI + "/To";
	protected final static String mccFromMCBaseURI = semossRelationURI + "/From";
	protected final static String engineServerBaseURI = semossRelationURI + "/HostedOn";
	protected final static String propURI = semossRelationURI + "/" + "Contains";
	protected final static String similarityPropURI = propURI + "/" + "SimilarityScore";
	
	
	
	protected static final String perspectiveQuery = "SELECT DISTINCT ?Perspective WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?Engine ?p ?Perspective}}";
	
	protected static final String enginePerspectiveQuery = "SELECT DISTINCT ?Engine ?p ?Perspective WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?Engine ?p ?Perspective}}";
	
	protected static final String engineInsightQuery = "SELECT DISTINCT ?Engine ?p ?Insight WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?Engine ?p ?Insight}}";
	
	protected static final String paramKeywordQuery = "SELECT DISTINCT ?Param ?p ?Keyword WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) BIND(<PARAM:TYPE> AS ?p) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Engine:Insight> ?Insight} {?Insight <INSIGHT:PARAM> ?Param} {?Param ?p ?Keyword}}";
	
	protected static final String engineKeywordRelationQuery = "SELECT DISTINCT ?Engine ?p ?Keyword WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine ?p ?Keyword}}";
	
	protected static final String mcKeywordRelationQuery = "SELECT DISTINCT ?MasterConcept ?p ?Keyword ?Score WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}{?p <http://semoss.org/ontologies/Relation/Contains/SimilarityScore> ?Score} {?MasterConcept ?p ?Keyword} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}FILTER(!BOUND(?Engine))}";
	
	protected static final String loneKeywordQuery = "SELECT DISTINCT ?Keyword ?Label WHERE {{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Keyword <http://www.w3.org/2000/01/rdf-schema#label> ?Label} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}FILTER(!BOUND(?Engine))}";
	
	protected static final String loneMCQuery = "SELECT DISTINCT ?MasterConcept ?Label WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?MasterConcept <http://www.w3.org/2000/01/rdf-schema#label> ?Label} OPTIONAL{{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?Keyword}}FILTER(!BOUND(?Keyword))}";
	
	protected static final String engineMCCRelationQuery = "SELECT DISTINCT ?Engine ?p ?MasterConceptConnection WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?Engine ?p ?MasterConceptConnection}}";
	
	protected static final String mccToMCRelationQuery = "SELECT DISTINCT ?MasterConceptConnection ?p ?MasterConcept WHERE {{?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?MasterConceptConnection ?p ?MasterConcept} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection}}FILTER(!BOUND(?Engine))}";
//	protected static final String mccFromMCRelationQuery = "SELECT DISTINCT ?MasterConceptConnection ?p ?MasterConcept WHERE {{?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?p <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/From>;} {?MasterConceptConnection ?p ?MasterConcept} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection}}FILTER(!BOUND(?Engine))}";
	
	protected static final String loneMCCQuery = "SELECT DISTINCT ?MasterConceptConnection ?Label WHERE {{?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>}{?MasterConceptConnection <http://www.w3.org/2000/01/rdf-schema#label> ?Label}  OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection}}FILTER(!BOUND(?Engine))}";
	
	protected static final String engineServerRelationQuery = "SELECT DISTINCT ?Engine ?p ?Server WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Server <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Server>} {?Engine ?p ?Server}}";
	
	protected static final String loneServerQuery = "SELECT DISTINCT ?Server ?p ?o WHERE {{?Server <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Server>} {?Server ?p ?o} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/HostedOn> ?Server}}FILTER(!BOUND(?Engine))}";

//	Hashtable<String,ArrayList<String>> keyEngineHash = new Hashtable<String,ArrayList<String>>();
	
	public void deleteEngine(String engineName) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
//		//delete all param keyword relationships
//		String paramKeywordQueryFilled = paramKeywordQuery.replaceAll("@ENGINE@", engineName);
//		deleteAllTriples(paramKeywordQueryFilled);

		//delete the engine insight relationships
		String engineInsightQueryFilled = engineInsightQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineInsightQueryFilled,engineInsightBaseURI);

		RDFFileSesameEngine eng = new RDFFileSesameEngine();
		eng.setEngineName(engineName);
		eng.setEngineURI2Name(engineBaseURI+"/"+engineName);
		eng.createInsightBase();
		RDFFileSesameEngine insightBaseXML = eng.getInsightBaseXML();
		insightBaseXML.setRC(masterEngine.rc);
		insightBaseXML.setSC(masterEngine.sc);
		insightBaseXML.setVF(masterEngine.vf);

//		query for all the perspectives to be deleted using question administrator
		Vector<String> perspectiveList = eng.getPerspectivesURI();
		
		//delete the engine perpsective relationships
		String enginePerspectiveQueryFilled = enginePerspectiveQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(enginePerspectiveQueryFilled,enginePerspectiveBaseURI);

		QuestionAdministrator qa = new QuestionAdministrator(eng);
		qa.selectedEngine = engineName;
		qa.setEngineURI2(engineBaseURI + "/" + engineName);
	
		for(String pers : perspectiveList) {
			qa.deleteAllFromPerspective(pers);
		}
		
		masterEngine.rc = (SailRepositoryConnection)(qa.getInsightBaseXML().getRC());
		masterEngine.sc = (SailConnection)(qa.getInsightBaseXML().getSC());
		
//		String q = "SELECT DISTINCT ?s ?p WHERE {?s ?p \"1. Use Heat Map to show how different Services use a given System\"}";
//		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,q);
//		String[] names = wrapper.getVariables();
//		while(wrapper.hasNext())
//		{
//			SesameJenaSelectStatement sjss = wrapper.next();
//			BigdataURIImpl s = (BigdataURIImpl)sjss.getRawVar(names[0]);
//			BigdataURIImpl p = (BigdataURIImpl)sjss.getRawVar(names[1]);
//			System.out.println(s.stringValue() + " ::: "+p.stringValue());
//		}
		
		
		
	//	masterEngine.vf = masterEngine.rc.getValueFactory();
		//deleting the keywords and their relationship:
		//delete all the engine to keyword relationships
		String engineKeywordRelationQueryFilled = engineKeywordRelationQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineKeywordRelationQueryFilled,engineKeywordBaseURI);
		
		//if there are any "lone" keywords, delete the mc to keyword relationship
		deleteRelationship(mcKeywordRelationQuery,mcKeywordBaseURI,similarityPropURI);
		
		//delete the "lone" keyword itself now that all relations are gone
		deleteNodes(loneKeywordQuery,keywordBaseURI);
		
		//deleting the MCCs and their relationships
		//delete all the engine to mcc relationships
		String engineMCCRelationQueryFilled = engineMCCRelationQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineMCCRelationQueryFilled,engineMCCBaseURI);
		
		//if there are any "lone" mccs, delete the mcc to mc and mcc from mc relationships
		deleteRelationship(mccToMCRelationQuery,mccToMCBaseURI);
		deleteRelationship(mccToMCRelationQuery,mccFromMCBaseURI);
		
		//delete the "lone" MCC now that all the relations are gone
		deleteNodes(loneMCCQuery,mccBaseURI);
		
		//if there are any "lone" master concepts, delete them
		deleteNodes(loneMCQuery,mcBaseURI);
		
		//remove the 3 engine triples. 2 types and a label
		String engineURI = engineBaseURI + "/" + engineName;
		try {
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDFS.LABEL, masterEngine.vf.createLiteral(engineName));
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDF.TYPE, masterEngine.vf.createURI(resourceURI));
			masterEngine.sc.removeStatements(masterEngine.vf.createURI(engineURI), RDF.TYPE, masterEngine.vf.createURI(engineBaseURI));
		} catch (SailException e) {
			logger.error("Could not remove engine type and label triples");
		}
		logger.info("Finished desktop delete");

	}
	
	public void deleteEngineWeb(String engineName) {
		deleteEngine(engineName);
		
		//deleting the servers, base URIs and their relationships
		//delete all the engine to server relationships
		String engineServerRelationQueryFilled = engineServerRelationQuery.replaceAll("@ENGINE@", engineName);
		deleteRelationship(engineServerRelationQueryFilled,engineServerBaseURI);
		
		//if there are any "lone" servers, delete the server proeprties and node
		deleteAllTriples(loneServerQuery);

	}
	
	/**
	 * Query to get rid of a node of the form s p o.
	 * removes all the triples associated with p to delete the relationship.
	 * @param engineName
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
			}
			else {
				logger.error("Could not cast query results for triple ::: " + s);
			}
			
			try {
				masterEngine.sc.removeStatements(sURI, RDFS.LABEL, masterEngine.vf.createLiteral(label));
				masterEngine.sc.removeStatements(sURI, RDF.TYPE, masterEngine.vf.createURI(resourceURI));
				masterEngine.sc.removeStatements(sURI, RDF.TYPE, masterEngine.vf.createURI(semossConceptURI));
				masterEngine.sc.removeStatements(sURI, RDF.TYPE, masterEngine.vf.createURI(baseURI));
				logger.debug("Removed Node ::: " + sURI);
			} catch (SailException e) {
				logger.info("Could not delete node ::: " + sURI);
			} 
		}
	}
	
	private void deleteRelationship(String query, String relationBaseURI) {
		deleteRelationship(query,relationBaseURI,null);
	}
	
	/**
	 * Query to get a relationship of the form s p o.
	 * removes all the triples associated with p to delete the relationship.
	 * @param engineName
	 */
	private void deleteRelationship(String query, String relationBaseURI,String relationProp) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			Object s = sjss.getRawVar(names[0]);
			Object p = sjss.getRawVar(names[1]);
			String pInstance = (String)sjss.getVar(names[1]);
			Object o = sjss.getRawVar(names[2]);
			Object relProp = null;
			if(relationProp!=null)
				relProp = sjss.getRawVar(names[3]);
			
			BigdataURIImpl eng = null;
			BigdataURIImpl pred = null;
			BigdataURIImpl insight = null;
			if(s instanceof BigdataURIImpl && p instanceof BigdataURIImpl && o instanceof BigdataURIImpl) {
				eng = ((BigdataURIImpl)s);
				pred = ((BigdataURIImpl)p);
				insight = ((BigdataURIImpl)o);
			}
			else {
				logger.error("Could not cast query results for triple ::: " + s + " ::: " + p + " ::: "+o);
			}
			
			BigdataValueImpl prop = null;
			if(relProp instanceof BigdataValueImpl)
				prop = (BigdataValueImpl)relProp;
			
			try {
				masterEngine.sc.removeStatements(eng, pred, insight);
				masterEngine.sc.removeStatements(pred, RDFS.SUBPROPERTYOF, masterEngine.vf.createURI(semossRelationURI));
				masterEngine.sc.removeStatements(pred, RDFS.SUBPROPERTYOF, masterEngine.vf.createURI(relationBaseURI));
				masterEngine.sc.removeStatements(pred, RDFS.SUBPROPERTYOF, pred);
				masterEngine.sc.removeStatements(pred, RDFS.LABEL, masterEngine.vf.createLiteral(pInstance));
				masterEngine.sc.removeStatements(pred, RDF.TYPE, masterEngine.vf.createURI(Constants.DEFAULT_PROPERTY_URI));
				masterEngine.sc.removeStatements(pred, RDF.TYPE, masterEngine.vf.createURI(resourceURI));
				if(relationProp!=null)
					masterEngine.sc.removeStatements(pred, masterEngine.vf.createURI(similarityPropURI), prop);					
				logger.debug("Removed Statement ::: " + eng + " ::: " + pred + " ::: " + insight);
			} catch (SailException e) {
				logger.info("Could not delete statement ::: " + eng + " ::: " + pred + " ::: " + insight);
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
