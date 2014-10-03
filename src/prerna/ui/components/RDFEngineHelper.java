/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.SEMOSSEdge;
import prerna.om.GraphDataModel;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;

/**
 * This class is responsible for loading many of the hierarchies available in RDF through the repository connection.
 */
public class RDFEngineHelper {

	private static final Resource RDFXML = null;
	static final Logger logger = LogManager.getLogger(RDFEngineHelper.class.getName());
	// responsible for handling various engine related stuff
	// loads the concepts from the engine into the specified sesame
	/**
	 * Loads the concept hierarchy.
	 * @param fromEngine 		Engine where data is stored.
	 * @param subjects 			Subject.
	 * @param objects 			Object.
	 * @param ps 				Graph playsheet that allows properties to be added to the repository connection.
	 */
	public static void loadConceptHierarchy(IEngine fromEngine, String subjects, String objects, GraphDataModel ps)
	{
		String conceptHierarchyForSubject = "" ;

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{			
			conceptHierarchyForSubject = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{" +
					"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
					"{?Subject ?Predicate ?Object}" + 
					"} BINDINGS ?Subject { " + subjects + objects + " } " +
					"";
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			conceptHierarchyForSubject = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{VALUES ?Subject {" + subjects + objects + "}" +
					"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
					"{?Subject ?Predicate ?Object}" + 
					"}";
		}
		
		addResultsToRC(fromEngine, conceptHierarchyForSubject, ps);
	}


	/**
	 * Loads the relation hierarchy.
	 * @param fromEngine 			Engine where data is stored.
	 * @param predicates 			Predicate.
	 * @param ps 					Graph playsheet that allows properties to be added to the repository connection.
	 */
	public static void loadRelationHierarchy(IEngine fromEngine, String predicates, GraphDataModel ps)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "";

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			relationHierarchy = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{" +
					"{?Subject ?Predicate ?Object}" + 
					"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
					"} BINDINGS ?Subject { " + predicates + " } " +
					"";// relation hierarchy		
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			relationHierarchy = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{ VALUES ?Subject {" + predicates + "}" + 
					"{?Subject ?Predicate ?Object}" + 
					"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
					"}";// relation hierarchy					
		}
		
		addResultsToRC(fromEngine, relationHierarchy, ps);

	}	

	/**
	 * Loads the property hierarchy.
	 * @param fromEngine 		Engine where data is stored.
	 * @param predicates 		Predicate.
	 * @param containsRelation 	String that shows the relation.
	 * @param ps 				Graph playsheet that allows properties to be added to the repository connection.
	 */
	public static void loadPropertyHierarchy(IEngine fromEngine, String predicates, String containsRelation, GraphDataModel ps)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "";

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			relationHierarchy = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{" +
					"{?Subject ?Predicate ?Object}" + 
					"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + containsRelation + " }" +
					"} BINDINGS ?Subject { " + predicates + " } " +
					"";// relation hierarchy
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			relationHierarchy = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{ VALUES ?Subject {" + predicates + "}" + 
					"{?Subject ?Predicate ?Object}" + 
					"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + containsRelation + " }" +
					"}";// relation hierarchy					
		}
		
		addResultsToRC(fromEngine, relationHierarchy, ps);

	}

	/**
	 * Gets general properties given a subject, object, predicate, and relationship.
	 * @param fromEngine 		Engine where data is stored.
	 * @param subjects 			Subject.
	 * @param objects 			Object.
	 * @param predicates 		Predicate.
	 * @param containsRelation 	String that shows the relation for the property query.
	 * @param ps 				Graph playsheet that allows properties to be added to repository connection.
	 */
	public static void genPropertiesRemote(IEngine fromEngine, String subjects, String objects, String predicates, String containsRelation, GraphDataModel ps)
	{

		String propertyQuery = "";
		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME|| fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			propertyQuery = "CONSTRUCT { ?Subject ?Predicate ?Object . ?Predicate ?type ?contains} WHERE {" +
					"BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?type)"+
					"BIND("+containsRelation+" as ?contains)"+
					"{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
					//"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
					"{?Subject ?Predicate ?Object}}" +
					"BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects + " }";
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			propertyQuery = "CONSTRUCT { ?Subject ?Predicate ?Object. ?Predicate ?type ?contains} WHERE {" +
					"VALUES ?Subject {" + subjects + " " + predicates + " " + objects + "}" +
					"BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?type)"+
					"BIND("+containsRelation+" as ?contains)"+
					"{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
					//"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
					"{?Subject ?Predicate ?Object}}";			
		}
		
		addResultsToRC(fromEngine, propertyQuery, ps);

	}

	/**
	 * Gets node properties from a local repository connection.
	 * @param rc 				Repository connection: main interface for updating data in and performing queries on a Sesame repository.
	 * @param containsRelation 	String that shows the relation for the property query.
	 * @param ps 				Graph playsheet where edge properties are added.
	 */
	public static void genNodePropertiesLocal(RepositoryConnection rc, String containsRelation, GraphDataModel ps)
	{

		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		String propertyQuery =  "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE {" +
				"{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
				"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
				"{?Subject ?Predicate ?Object}}";


		SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		sjsc.setEngine(sesameEngine);
		sjsc.setQuery(propertyQuery);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			SesameJenaConstructStatement sct = sjsc.next();

			String subject = sct.getSubject();
			String predicate = sct.getPredicate();
			Object obj = sct.getObject();

			// add the property
			ps.addNodeProperty(subject, obj, predicate);

		}
	}
	/**
	 * Gets edge properties from a local repository connection.
	 * @param rc 				Repository connection: main interface for updating data in and performing queries on a Sesame repository.
	 * @param containsRelation 	String that shows the relation for the property query.
	 * @param ps 				Graph playsheet where edge properties are added.
	 */
	public static void genEdgePropertiesLocal(RepositoryConnection rc, String containsRelation, GraphDataModel ps)
	{

		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		String propertyQuery =  "SELECT ?edge ?prop ?value ?outNode ?inNode WHERE {" +
				"{?prop " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
				"{?edge " + "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>   " +  " <http://semoss.org/ontologies/Relation>;}" +
				"{?outNode " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>   " +  " <http://semoss.org/ontologies/Concept>;}" +
				"{?inNode " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>   " +  " <http://semoss.org/ontologies/Concept>;}" +
				"{?edge ?prop ?value} {?outNode ?edge ?inNode} }";


		SesameJenaSelectWrapper sjsc = new SesameJenaSelectWrapper();
		sjsc.setEngine(sesameEngine);
		sjsc.setQuery(propertyQuery);
		sjsc.executeQuery();
		sjsc.getVariables();
		while(sjsc.hasNext())
		{
			SesameJenaSelectStatement sct = sjsc.next();

			String edge = sct.getRawVar("edge") + "";
			String prop = sct.getRawVar("prop") + "";
			String inNode = sct.getRawVar("inNode") + "";
			String outNode = sct.getRawVar("outNode") + "";
			Object value = sct.getRawVar("value");

			// add the property
			ps.addEdgeProperty(edge, value, prop, outNode, inNode);

		}
	}

	/**
	 * Loads all of the labels from subjects.
	 * @param fromEngine 	Engine where data is stored.
	 * @param subjects 		String containing the subjects.
	 * @param ps 			Graph playsheet where vertexes and edges are stored.
	 */
	public static void loadLabels(IEngine fromEngine, String subjects, GraphDataModel ps)
	{
		// loads all of the labels
		// http://www.w3.org/2000/01/rdf-schema#label
		String labelQuery = "";
		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{			
			labelQuery = "SELECT DISTINCT ?Subject ?Label WHERE " +
					"{" +
					"{?Subject <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
					"} BINDINGS ?Subject { " + subjects + " } " +
					"";
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			labelQuery = "SELECT DISTINCT ?Subject ?Label WHERE " +
					"{VALUES ?Subject {" + subjects + "}" +
					"{?Subject <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
					"}";
		}
		System.err.println("Query is " + labelQuery);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(fromEngine);
		sjsw.setQuery(labelQuery);
		sjsw.executeQuery();
		sjsw.getVariables();

		Hashtable<String, SEMOSSVertex> vertStore = ps.getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = ps.getEdgeStore();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement st = sjsw.next();
			String subject = st.getRawVar("Subject") + "";
			String label = st.getVar("Label") + "";

			SEMOSSVertex vert = vertStore.get(subject);
			if(vert != null)
				vert.setProperty(Constants.VERTEX_NAME, label);
			else
			{
				// may be an edge ?
				SEMOSSEdge edge = edgeStore.get(subject);
				if(edge != null)
					edge.setProperty(Constants.EDGE_NAME, label);
			}
		}
	}

	/**
	 * Add results from a query on an engine to the respository connection.
	 * @param fromEngine 	Engine where data is stored.
	 * @param query 		Query to be run.
	 * @param ps 			Graph playsheet where sesame construct statement is stored.
	 */
	private static void addResultsToRC(IEngine fromEngine, String query, GraphDataModel ps) {
		SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(query);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			SesameJenaConstructStatement st = sjsc.next();
			ps.addToSesame(st, false, false);
		}
		
	}

	/**
	 * Adds data to a specified engine.
	 * @param fromEngine 	Engine where data is stored.
	 * @param toRC 			Main interface for updating data in and performing queries on a Sesame repository.
	 */
	public static void addAllData(IEngine fromEngine, RepositoryConnection toRC)
	{
		// same concept as the subject, but only for relations
		String constructAllQuery = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
				"{" +
				"{?Subject ?Predicate ?Object} }" + 
				"";

		SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(constructAllQuery);
		sjsc.execute();

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{			
			try {
				toRC.add(sjsc.gqr);
				//TODO: Delete? Engine type is Sesame
//				while(sjsc.hasNext())
//				{
//					SesameJenaConstructStatement st = sjsc.next();
//					logger.debug(st.getSubject() + st.getPredicate() + st.getObject());
//					addToJenaModel(st);
//				}
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}

	}
	/**
	 * Removes all data from a certain engine.
	 * @param fromEngine 	Engine where data is stored.
	 * @param toRC 			Main interface for updating data in and performing queries on a Sesame repository.
	 */
	public static void removeAllData(IEngine fromEngine, RepositoryConnection toRC)
	{
		// same concept as the subject, but only for relations
		String constructAllQuery = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
				"{" +
				"{?Subject ?Predicate ?Object} }" + 
				"";// relation hierarchy

		SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(constructAllQuery);
		sjsc.execute();

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{			
			try {
				toRC.remove(sjsc.gqr);
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}

	}
	
	/**
	 * Loads base relations from the OWL file.
	 * @param owlFilePath 	String that contains the path to the OWL file.
	
	 * @return Hashtable 	Hashtable of base relations. 
	 * @throws RepositoryException 
	 * @throws IOException 
	 * @throws RDFParseException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException */
	public static Hashtable loadBaseRelationsFromOWL(String owlFilePath) throws RepositoryException, RDFParseException, IOException, MalformedQueryException, QueryEvaluationException {
		Repository myRepository = new SailRepository(new MemoryStore());
		myRepository.initialize();
		RepositoryConnection rcOWL = myRepository.getConnection();
		
		File owlFile = new File(owlFilePath);
		rcOWL.add(owlFile, owlFilePath, RDFFormat.RDFXML);
		rcOWL.commit();
		Hashtable retHash = createBaseFilterHash(rcOWL);
		return retHash;
	}	
	
	public static Hashtable createBaseFilterHash(RepositoryConnection rcOWL) throws RepositoryException, MalformedQueryException, QueryEvaluationException{
		Hashtable<String,String> baseFilterHash = new Hashtable<String,String>();
		String queryString = "SELECT ?x ?p ?y WHERE { ?x ?p ?y } ";
		TupleQuery tupleQuery = rcOWL.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		List<String> bindingNames = result.getBindingNames();
		while (result.hasNext())
		{
			BindingSet bindingSet = result.next();
			baseFilterHash.put(bindingSet.getValue(bindingNames.get(0)).toString(), bindingSet.getValue(bindingNames.get(0)).toString());
			baseFilterHash.put(bindingSet.getValue(bindingNames.get(1)).toString(), bindingSet.getValue(bindingNames.get(1)).toString());
			baseFilterHash.put(bindingSet.getValue(bindingNames.get(2)).toString(), bindingSet.getValue(bindingNames.get(2)).toString());
		}		
		return baseFilterHash;
	}
}
