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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.helpers.EntityFiller;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import edu.uci.ics.jung.graph.DelegateForest;

/**
 * This class collects the information that is used in DistanceDownstreamProcessor.
 */
public class DistanceDownstreamInserter {

	Hashtable masterHash = new Hashtable();
	Logger logger = Logger.getLogger(getClass());
	String RELATION_URI = null;
	String PROP_URI = null;
	IEngine engine;
	double depreciationRate;
	double appreciationRate;
	// references to main vertstore
	Hashtable<String, SEMOSSVertex> vertStore = new Hashtable();
	// references to the main edgeStore
	Hashtable<String, SEMOSSEdge> edgeStore = new Hashtable();
	PropertySpecData predData = new PropertySpecData();
	Hashtable networkValueHash = new Hashtable();
	Hashtable soaValueHash = new Hashtable();

	/**
	 * Uses the engine to get all data objects in the selected database.
	 * For each data object, creates the forest (system network query) and passes it to DistanceDownstreamProcessor
	 * Gets back the master hash from DistanceDownstreamProcessor so that it knows distances for the systems
	 * Uses the returned master hash to create insert query for the database, runs query.
	 */
	public void insertAllDataDownstream(){
		Vector<String> dataObjectsArray = getObjects("http://semoss.org/ontologies/Concept/DataObject");
		//dataObjectsArray.add("Patient_Procedures");

		Hashtable distanceDownstreamHash = new Hashtable();
		int count = 0;
		//this will add distance downstream for all systems connected to creators
		for(String dataObjectString: dataObjectsArray){
			count++;
			//if (count>15) break;
			double maxCreditValue = 0.0;
			//Create the forest for this data object
			String unfilledQuery = (String) DIHelper.getInstance().getProperty(Constants.DISTANCE_DOWNSTREAM_QUERY);
			Hashtable<String, String> paramHash = new Hashtable<String, String>();
			paramHash.put("Data-Data", dataObjectString);
			String query = Utility.fillParam(unfilledQuery, paramHash);
			DelegateForest<SEMOSSVertex, SEMOSSEdge> dataForest = new DelegateForest<SEMOSSVertex, SEMOSSEdge>();
			try {
				dataForest = createForest(query);
			} catch (Exception e) {
				//TODO: Specify exception
				e.printStackTrace();
			}

			DistanceDownstreamProcessor processor = new DistanceDownstreamProcessor();
			//now set everything in DistanceDownstreamProcessor and let that buddy run
			processor.setForest(dataForest);
			processor.setRootNodesAsSelected();
			processor.addSelectedNode(dataObjectString, 0);//need to make sure that creators first have the chance to go from data object directly
			processor.execute();
		
			//now add everything but the data object to fullSystemHash
			//format of fullSystemHash will be key:data object, object: sysHash with key: system and object: distance and system_network: network weight
			Hashtable sysHash = new Hashtable();
			Iterator keyIt = processor.masterHash.keySet().iterator();
			while (keyIt.hasNext()){
				
				SEMOSSVertex sysVertex = (SEMOSSVertex) keyIt.next();
				String system = sysVertex.getProperty(Constants.VERTEX_NAME) +"";
				String sysURI = sysVertex.getProperty(Constants.URI) +"";
				
				//the processor masterHash will have the dataObject as a key.  Need to make sure I don't take that
				if (sysURI.equals(dataObjectString)){
					//do nothing
				}
				else {
					Hashtable vertHash = (Hashtable) processor.masterHash.get(sysVertex);
					ArrayList<SEMOSSVertex> path = (ArrayList<SEMOSSVertex>) vertHash.get(processor.pathString);
					//if the path starts with the data object, need to subtract 1 from distance
					int distance = (Integer) vertHash.get(processor.distanceString);
					if((path.get(0)).getProperty(Constants.URI).equals(dataObjectString))
						distance = distance - 1;
					sysHash.put(system, distance);
					
					//this will fill networkValueHash and soaValueHash with raw numbers
					//normalizeNetworkWeights will then update the values in networkValueHash by normalizing it
					maxCreditValue = calculateWeights(vertHash, system, dataObjectString, distance, processor.pathString, maxCreditValue);
				}
				
			}
			distanceDownstreamHash.put(dataObjectString, sysHash);
			normalizeNetworkWeights(maxCreditValue, dataObjectString);
		}
				
		//now all of the information should be contained within fullSystemHash.
		//Just need to prepare insert query and run insert
		
		String insertQuery = prepareInsert(distanceDownstreamHash, "DistanceDownstream", "integer");
		UpdateProcessor updatePro = new UpdateProcessor();
		updatePro.setQuery(insertQuery);
		logger.info("Update Query 1 " + insertQuery);
		updatePro.processQuery();
		
		String insertSOAweightQuery = prepareInsert(soaValueHash, "weight", "double");
		updatePro.setQuery(insertSOAweightQuery);
		logger.info("Update Query 2 " + insertSOAweightQuery);
		updatePro.processQuery();

		String insertNetworkWeightQuery = prepareInsert(networkValueHash, "NetworkWeight", "double");
		updatePro.setQuery(insertNetworkWeightQuery);
		logger.info("Update Query 3 " + insertNetworkWeightQuery);
		updatePro.processQuery();
	}
	
	/**
	 * Gets network value from a hashtable given the data object key
	 * Iterates through the datahash and normalizes the network weights by dividing by the max credit value
	 * Replaces normalized values from the data hash into the network value hash, which has the data object strings as keys
	 * 
	 * @param maxCreditValue Double			Maximum credit value
	 * @param dataObjectString String		Data object name as a string
	 */
	public void normalizeNetworkWeights(Double maxCreditValue, String dataObjectString){
		Hashtable dataHash = (Hashtable) networkValueHash.get(dataObjectString);
		if(dataHash!=null){
			Iterator dataIt = dataHash.keySet().iterator();
			while(dataIt.hasNext()){
				String sysName = (String) dataIt.next();
				Double rawNetworkValue = (Double) dataHash.get(sysName);
				Double networkValue = rawNetworkValue/maxCreditValue;
				dataHash.put(sysName, networkValue);
			}
			networkValueHash.put(dataObjectString, dataHash);
		}
	}
	
	/**
	 * Sets the engine.
	 * @param e IEngine		Engine to be set.
	 */
	public void setEngine(IEngine e){
		this.engine = e;
	}
	
	/**
	 * Sets the appreciation and depreciation values for the calculation.
	 * 
	 * @param appreciation Double		Appreciation rate
	 * @param depreciation Double		Depreciation rate
	 */
	public void setAppAndDep(Double appreciation, Double depreciation){
		appreciationRate = appreciation;
		depreciationRate = depreciation;
	}
	
	/**
	 * Uses the vert hash to calculate the SOA weight and network weight.
	 * 
	 * @param vertHash Hashtable			Contains distance and path with the key being the actual vertex.
	 * @param systemName String				System name.
	 * @param dataName String				Name of data, used in SOA Transition Planning.
	 * @param distance int					Distance of path.
	 * @param pathString String				Path of the vertex.
	 * @param maxCreditValue double			Maximum credit value.
	
	 * @return double */
	private double calculateWeights(Hashtable vertHash, String systemName, String dataName, int distance, String pathString, double maxCreditValue){
		double newMax = maxCreditValue;
		//first the SOA part.  just need to put the calculation on the distance
		double soaWeight = Math.pow(depreciationRate, distance);
		Hashtable dataSOAHash = new Hashtable();
		if(soaValueHash.containsKey(dataName)) dataSOAHash = (Hashtable) soaValueHash.get(dataName);
		dataSOAHash.put(systemName, soaWeight);
		soaValueHash.put(dataName, dataSOAHash);
		
		//if its not a root node, take the edge above its weight and add its network value everywhere
		ArrayList<SEMOSSVertex> pathArray = (ArrayList<SEMOSSVertex>) vertHash.get(pathString);
		if(distance>0){
			Hashtable dataNetHash = new Hashtable();
			if(networkValueHash.containsKey(dataName)) dataNetHash = (Hashtable) networkValueHash.get(dataName);
			int pathLength = pathArray.size();
			double difference = Math.pow(depreciationRate, distance-1)- Math.pow(depreciationRate, distance);
			double prevValue = 0.0;
			for(int i= 0; i<distance; i++){
				int thisNodeIdx = pathLength-1-i;
				SEMOSSVertex thisVert = pathArray.get(thisNodeIdx);
				SEMOSSVertex vertAbove = pathArray.get(thisNodeIdx-1);

				double addedValue;
				if(i == 0) addedValue = difference*appreciationRate;
				else addedValue= prevValue*(1-appreciationRate);
				double oldValue =0.0;
				if(dataNetHash.containsKey(vertAbove.getProperty(Constants.VERTEX_NAME))){
					oldValue = (Double) dataNetHash.get(vertAbove.getProperty(Constants.VERTEX_NAME));
				}
				double newValue = oldValue+addedValue;
				//if newValue > max, max becomes new
				if(newValue>newMax) newMax = newValue;
				dataNetHash.put(vertAbove.getProperty(Constants.VERTEX_NAME), newValue);
				networkValueHash.put(dataName, dataNetHash);
				prevValue = addedValue;
			}
		}
		return newMax;
	}

	/**
	 * Creates the insert query.
	 * 
	 * @param hash Hashtable	Must contain object level, subject level, value
	 * @param propName String	Name of the property to be inserted
	 * @param type String		Must be "integer" or "double"
	
	 * @return String			The insert query */
	private String prepareInsert(Hashtable hash, String propName, String type){
		String predUri = "<http://semoss.org/ontologies/Relation/Contains/"+propName+">";
		
		//add start with type triple
		String insertQuery = "INSERT DATA { " +predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://semoss.org/ontologies/Relation/Contains>. ";
		
		//add other type triple
		insertQuery = insertQuery + predUri +" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
		
		//add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
		insertQuery = insertQuery + predUri +" <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
				predUri + ". ";
		
		Iterator dataIt = hash.keySet().iterator();
		while(dataIt.hasNext()){
			String dataName = (String) dataIt.next();
			Hashtable sysHash = (Hashtable) hash.get(dataName);
			Iterator sysIt = sysHash.keySet().iterator();
			while(sysIt.hasNext()){
				String sysName = (String) sysIt.next();
				String dataInstance = Utility.getInstanceName(dataName);
				String relationUri = "<http://health.mil/ontologies/Relation/Provide/"+sysName +
						Constants.RELATION_URI_CONCATENATOR+dataInstance+">";
				
				
				Object value = sysHash.get(sysName);
				
				String objUri = "\"" + value + "\"" + "^^<http://www.w3.org/2001/XMLSchema#"+type+">";
				
				insertQuery = insertQuery + relationUri + " " + predUri + " " + objUri + ". ";
				
			}
		}
		insertQuery = insertQuery + "}";
		
		return insertQuery;
	}
	
	
	/**
	 * Creates the forest.
	 * @param query String								Query needed to create the forest
	
	 * @return DelegateForest<DBCMVertex,DBCMEdge>		Forest, comprised of vertices and edges. */
	public DelegateForest<SEMOSSVertex, SEMOSSEdge> createForest(String query) throws Exception {
		//run query
		SesameJenaConstructWrapper sjw = new SesameJenaConstructWrapper();
		sjw.setQuery(query);
		sjw.setEngine(engine);
		sjw.execute();
		
		
		//this is pretty much directly from GraphPlaySheet CreateForest().  I removed Jena Model and Control Data though
		logger.info("Creating Forest >>>>>");

		DelegateForest<SEMOSSVertex, SEMOSSEdge> forest = new DelegateForest<SEMOSSVertex, SEMOSSEdge>();
		Properties rdfMap = DIHelper.getInstance().getRdfMap();

		createBaseURIs();
		// iterate through the graph query result and set everything up
		// this is also the place where the vertex filter data needs to be created
		
		logger.debug(" Adding graph to forest " );
		int count = 0;
		while(sjw.hasNext())
		{
			//logger.warn("Iterating " + count);
			count++;

			SesameJenaConstructStatement sct = sjw.next();
			String predicateName = sct.getPredicate();

					// get the subject, predicate and object
					// look for the appropriate vertices etc and paint it
					predData.addConceptAvailable(sct.getSubject());
					predData.addConceptAvailable(sct.getObject()+"");
					SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
					if(vert1 == null)
					{
						vert1 = new SEMOSSVertex(sct.getSubject());
						vertStore.put(sct.getSubject()+"", vert1);
					}
					SEMOSSVertex vert2 = vertStore.get(sct.getObject()+"");
					if(vert2 == null )//|| forest.getInEdges(vert2).size()>=1)
					{
						if(sct.getObject() instanceof URI)
							vert2 = new SEMOSSVertex(sct.getObject()+"");
						else // ok this is a literal
							vert2 = new SEMOSSVertex(sct.getPredicate(), sct.getObject());
						vertStore.put(sct.getObject()+"", vert2);
					}
					// create the edge now
					SEMOSSEdge edge = edgeStore.get(sct.getPredicate()+"");
					// check to see if this is another type of edge
					if(sct.getPredicate().indexOf(vert1.getProperty(Constants.VERTEX_NAME)+"") < 0 && sct.getPredicate().indexOf(vert2.getProperty(Constants.VERTEX_NAME)+"") < 0)
						predicateName = sct.getPredicate() + "/" + vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME);
					if(edge == null)
						edge = edgeStore.get(predicateName);
					if(edge == null)
					{
						// need to create the predicate at runtime I think
						/*edge = new DBCMEdge(vert1, vert2, sct.getPredicate());
						System.err.println("Predicate plugged is " + predicateName);
						edgeStore.put(sct.getPredicate()+"", edge);*/

						// the logic works only when the predicates dont have the vertices on it.. 
						edge = new SEMOSSEdge(vert1, vert2, predicateName);
						edgeStore.put(predicateName, edge);
					}
					
					// add the edge now if the edge does not exist
					// need to handle the duplicate issue again
					try
					{
						forest.addEdge(edge, vertStore.get(sct.getSubject()+""),
							vertStore.get(sct.getObject()+""));
					}catch (Exception ex)
					{
						ex.printStackTrace();
						logger.warn("Missing Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
						// ok.. I am going to ignore for now that this is a duplicate edge
					}
		}
		logger.info("Creating Forest Complete >>>>>> ");
		return forest;
	}
	
	/**
	 * Creates base URIs.
	 */
	private void createBaseURIs(){
		RELATION_URI = DIHelper.getInstance().getProperty(
				Constants.PREDICATE_URI);
		PROP_URI = DIHelper.getInstance()
				.getProperty(Constants.PROP_URI);
	}
	
	//this is for getting all of the objects of a certain type.  I am most likely getting DataObject so that I can populate my forest queries
	/**
	 * Gets all objects of a certain type - in this case, probably DataObject in order to populate the forest queries.
	 * @param type String		Must be integer or double
	
	 * @return Vector<String> 	Vector containing string names of all the data objects. */
	public Vector<String> getObjects(String type){
		Vector<String> dataObjectArray = new Vector<String>();
		EntityFiller filler = new EntityFiller();
		//filler.engine = this.engine;
		filler.engineName = this.engine.getEngineName();
		filler.type = type;
		filler.run();
		dataObjectArray = filler.names;
		return dataObjectArray;
		
	}

}
