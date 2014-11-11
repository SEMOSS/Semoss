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
package prerna.ui.components.playsheets;

import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;

/**
 */
public class SimpleGraphPlaySheet extends GraphPlaySheet{

	private static final Logger logger = LogManager.getLogger(SimpleGraphPlaySheet.class.getName());
	/**
	 * Method createForest.  Takes the base information from the query and inserts it into the jena model.
	 */
	public void createForest()
	{
		// need to take the base information from the base query and insert it into the jena model
		// this is based on EXTERNAL ontology
		// then take the ontology and insert it into the jena model
		// (may be eventually we can run this through a reasoner too)
		// Now insert our base model into the same ontology
		// Now query the model for 
		// Relations - Paint the basic graph
		// Now find a way to get all the predicate properties from them
		// Hopefully the property is done using subproperty of
		// predicates - Pick all the predicates but for the properties
		// paint them
		// properties
		// and then paint it appropriately
		logger.debug("creating the in memory jena model");
//		boolean isError = false;
		
		StringBuffer subjects = new StringBuffer("");
		StringBuffer predicates = new StringBuffer("");
		StringBuffer objects = new StringBuffer("");
		

		SesameJenaConstructWrapper sjw = null;
		while(sjw.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			SesameJenaConstructStatement st = sjw.next();
			Object obj = st.getObject();
			logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
			//predData.addPredicate2(st.getPredicate());
			//predData.addConceptAvailable(st.getSubject());//, st.getSubject());
			//predData.addPredicateAvailable(st.getPredicate());//, st.getPredicate());

			if(subjects.indexOf(st.getSubject()) < 0)
			{
				if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
					subjects.append("(<" + st.getSubject() + ">)");
				else
					subjects.append("<" + st.getSubject() + ">");
			}
			if(predicates.indexOf(st.getPredicate()) < 0)
			{
				if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
					predicates.append("(<" + st.getPredicate() +">)");
				else
					predicates.append( "<" + st.getPredicate() +">");
			}
			// need to find a way to do this for jena too
			if(obj instanceof URI && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal))
			{			
				if(objects.indexOf(obj+"") < 0)
				{
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
						objects.append("(<" + obj +">)");
					else
						objects.append("<" + obj +">");
				}
			}
			//addToJenaModel(st);
//			addToSesame(st, false, false);
//			if (search) addToJenaModel3(st);
		}		
		genBaseConcepts();
		genBaseGraph();//subjects2, predicates2, subjects2);
		
//		try {
//			RDFEngineHelper.loadLabels(engine, subjects.toString() + objects.toString(), this);
//		} catch (Exception e) {
//			// TODO: Specify exception
//			e.printStackTrace();
//		}
		genAllData();

		logger.warn("Done with everything");
		// first execute all the predicate selectors
		// Backdoor entry
				
		logger.info("Creating Forest Complete >>>>>> ");										
	}
	
	/**
	 * Method genBaseConcepts.  Creates all the concepts and relationships to build the graph.
	 */
	public void genBaseConcepts()
	{
		// create all the relationships now
		String conceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  //"VALUES ?Subject {"  + subjects + "}"+
									  //"VALUES ?Object {"  + subjects + "}"+
									  //"VALUES ?Object {"  + objects + "}" +
									  //"VALUES ?Predicate {"  + predicates + "}" +
									  //"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
									  //"{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		
		System.err.println(" conceptSelectQuery query " + conceptSelectQuery);
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
//		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);

		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
				
		logger.debug(conceptSelectQuery);
		
		try {
			sjsc.setQuery(conceptSelectQuery);
			sjsc.execute();
			logger.warn("Execute complete.");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.warn("Iterating " + count);
				count++;

				SesameJenaConstructStatement sct = sjsc.next();

//						SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
//						if(vert1 == null)
//						{
//							vert1 = new SEMOSSVertex(sct.getSubject());
//							vertStore.put(sct.getSubject()+"", vert1);
//							genControlData(vert1);
//						}
//						// add my friend
//						if(filteredNodes == null || (filteredNodes != null && !filteredNodes.containsKey(sct.getSubject()+"")))
//							this.forest.addVertex(vertStore.get(sct.getSubject()));
//						filterData.addVertex(vert1);
			}
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
		}
	}

	
	/**
	 * Method genBaseGraph.  This executes the first SPARQL query and generates the graphs.
	 */
	public void genBaseGraph()
	{
		// create all the relationships now
		String predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  //"VALUES ?Subject {"  + subjects + "}"+
									  //"VALUES ?Object {"  + subjects + "}"+
									  //"VALUES ?Object {"  + objects + "}" +
									  //"VALUES ?Predicate {"  + predicates + "}" +
									  //"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
									  //"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  //"{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
//		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);

		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
				
		logger.debug(predicateSelectQuery);
		
		try {
			sjsc.setQuery(predicateSelectQuery);
			sjsc.execute();
			logger.warn("Execute complete.");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.warn("Iterating " + count);
				count++;

				SesameJenaConstructStatement sct = sjsc.next();
				String predicateName = sct.getPredicate();

						// get the subject, predicate and object
						// look for the appropriate vertices etc and paint it
						gdm.predData.addConceptAvailable(sct.getSubject());
						gdm.predData.addConceptAvailable(sct.getObject()+"");
//						SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
//						if(vert1 == null)
//						{
//							vert1 = new SEMOSSVertex(sct.getSubject());
//							vertStore.put(sct.getSubject()+"", vert1);
//							genControlData(vert1);
//						}
//						SEMOSSVertex vert2 = vertStore.get(sct.getObject()+"");
//						if(vert2 == null )//|| forest.getInEdges(vert2).size()>=1)
//						{
//							if(sct.getObject() instanceof URI)
//								vert2 = new SEMOSSVertex(sct.getObject()+"");
//							else // ok this is a literal
//								vert2 = new SEMOSSVertex(sct.getPredicate(), sct.getObject());
//							vertStore.put(sct.getObject()+"", vert2);
//							genControlData(vert2);
//						}
//						// create the edge now
//						SEMOSSEdge edge = edgeStore.get(sct.getPredicate()+"");
//						// check to see if this is another type of edge
//						if(sct.getPredicate().indexOf(vert1.getProperty(Constants.VERTEX_NAME)+"") < 0 && sct.getPredicate().indexOf(vert2.getProperty(Constants.VERTEX_NAME)+"") < 0)
//							predicateName = sct.getPredicate() + "/" + vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME);
//						if(edge == null)
//							edge = edgeStore.get(predicateName);
//						if(edge == null)
//						{
//							// need to create the predicate at runtime I think
//							/*edge = new DBCMEdge(vert1, vert2, sct.getPredicate());
//							System.err.println("Predicate plugged is " + predicateName);
//							edgeStore.put(sct.getPredicate()+"", edge);*/
//
//							// the logic works only when the predicates dont have the vertices on it.. 
//							edge = new SEMOSSEdge(vert1, vert2, predicateName);
//							edgeStore.put(predicateName, edge);
//						}
//						filterData.addVertex(vert1);
//						filterData.addVertex(vert2);
//						filterData.addEdge(edge);
//						//logger.warn("Found Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
//
//						
//						// add the edge now if the edge does not exist
//						// need to handle the duplicate issue again
//						try
//						{
//							if ((filteredNodes == null) || (filteredNodes != null && !filteredNodes.containsKey(sct.getSubject()+"")
//									&& !filteredNodes.containsKey(sct.getObject() +"") && !filterData.edgeFilterNodes.containsKey(sct.getPredicate() + ""))) 						{	
//								predData.addPredicateAvailable(sct.getPredicate());
//								// try to see if the predicate here is a property
//								// if so then add it as a property
//							this.forest.addEdge(edge, vertStore.get(sct.getSubject()+""),
//								vertStore.get(sct.getObject()+""));
//							genControlData(edge);
//							// to be removed later
//							// I dont know if we even use this
//							// need to ask Bill and Tom
//							graph.addVertex(vertStore.get(sct.getSubject()));
//							graph.addVertex(vertStore.get(sct.getObject()+""));
//							
//							graph.addEdge(vertStore.get(sct.getSubject()),
//									vertStore.get(sct.getObject()+""), edge);
//							}
//						}catch (Exception ex)
//						{
//							ex.printStackTrace();
//							logger.warn("Missing Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
//							// ok.. I am going to ignore for now that this is a duplicate edge
//						}
			}
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
		//}		
	}
}
