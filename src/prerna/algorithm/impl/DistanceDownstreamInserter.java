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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.specific.tap.QueryProcessor;
import prerna.ui.helpers.EntityFiller;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * This class collects the information that is used in DistanceDownstreamProcessor.
 */
public class DistanceDownstreamInserter {
	String unfilledQuery = "SELECT DISTINCT ?System2 ?System3 WHERE { BIND( <@Data-Data@> AS ?Data1). { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System1 ?provide ?Data1 ;} {?provide <http://semoss.org/ontologies/Relation/Contains/SOR> 'Yes' ;} BIND(?Data1 AS ?System2) BIND(?System1 AS ?System3) } UNION { BIND(URI(CONCAT('http://health.mil/ontologies/Relation/', SUBSTR(STR(?System2), 45), ':', SUBSTR(STR(?System3), 45))) AS ?passes). {?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System2 ?upstream1 ?icd1 ;} {?icd1 ?downstream1 ?System3;} {?icd1 ?carries ?Data1;} } }";
	Hashtable masterHash = new Hashtable();
	static final Logger logger = LogManager.getLogger(DistanceDownstreamInserter.class.getName());
	IDatabaseEngine engine;
	double depreciationRate;
	double appreciationRate;

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
		Hashtable soaValueHash = new Hashtable();
		Hashtable networkValueHash = new Hashtable();
		
		//this will add distance downstream for all systems connected to creators
		for(String dataObjectString: dataObjectsArray){
			
			//Construct Query
			Map<String, List<Object>> paramHash = new HashMap<String, List<Object>>();
			List<Object> dataObjList = new ArrayList<Object>();
			dataObjList.add(dataObjectString);
			paramHash.put("Data-Data", dataObjList);
			String query = Utility.fillParam(unfilledQuery, paramHash);
			
			//Run Query
			HashMap<String,ArrayList<String>> interfaceMap = QueryProcessor.getStringListMap(query,"TAP_Core_Data");
			//printMap(interfaceMap);
			
			if( interfaceMap.keySet().size() > 0 )
			{
				//System.out.println(Utility.getInstanceName(dataObjectString));
				//Contruct Graph from query results.
				Graph g = new Graph(Utility.getInstanceName(dataObjectString),interfaceMap);
				//g.printGraph();
				g.processDownstream();
				g.processWeight(appreciationRate);
				g.processNetworkWeight(depreciationRate);
				//g.printGraphValues();
				
				distanceDownstreamHash.put(dataObjectString, g.getDownstream());
				soaValueHash.put(dataObjectString, g.getWeight());
				networkValueHash.put(dataObjectString, g.getNetworkWeight());
			}
		}
		
		String insertQuery = prepareInsert(distanceDownstreamHash, "DistanceDownstream", "integer");
		UpdateProcessor updatePro = new UpdateProcessor();
		updatePro.setQuery(insertQuery);
		logger.info("Update Query 1 " + Utility.cleanLogString(insertQuery));
		System.out.println(insertQuery);
		updatePro.processQuery();
		
		String insertSOAweightQuery = prepareInsert(soaValueHash, "weight", "double");
		updatePro.setQuery(insertSOAweightQuery);
		logger.info("Update Query 2 " + Utility.cleanLogString(insertSOAweightQuery));
		updatePro.processQuery();
		System.out.println(insertSOAweightQuery);

		String insertNetworkWeightQuery = prepareInsert(networkValueHash, "NetworkWeight", "double");
		updatePro.setQuery(insertNetworkWeightQuery);
		logger.info("Update Query 3 " + Utility.cleanLogString(insertNetworkWeightQuery));
		updatePro.processQuery();
		System.out.println(insertNetworkWeightQuery);
	}
	
	/**
	 * Verbose print out of HashMap. This is the same type of map that is created in the query from 
	 *  QueryProcessor.getStringListMap(String, String). Used for testing of query results before
	 *  before construction of any objects.
	 *  
	 *  @param inMap HashMap<String,ArrayList<String>>	Map to be printed.
	 */
	private void printMap(HashMap<String,ArrayList<String>> inMap)
	{
		System.out.println("Size of Map:" +inMap.keySet().size());
		for(String key: inMap.keySet())
		{
			ArrayList<String> list = inMap.get(key);
			System.out.print(key + ":\t");
			for( String s : list )
			{
				System.out.print(s + ", \t");
			}
			System.out.println();
		}
	}
			
		
	/**
	 * Sets the engine.
	 * @param e IDatabase		Engine to be set.
	 */
	public void setEngine(IDatabaseEngine e){
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
	
	//this is for getting all of the objects of a certain type.  I am most likely getting DataObject so that I can populate my forest queries
	/**
	 * Gets all objects of a certain type - in this case, probably DataObject in order to populate the forest queries.
	 * @param type String		Must be integer or double
	
	 * @return Vector<String> 	Vector containing string names of all the data objects. */
	public Vector<String> getObjects(String type){
		Vector<String> dataObjectArray = new Vector<String>();
		EntityFiller filler = new EntityFiller();
		//filler.engine = this.engine;
		filler.engineName = this.engine.getEngineId();
		filler.type = type;
		filler.run();
		dataObjectArray = filler.names;
		return dataObjectArray;
		
	}
	
	
	/**
	 * This class creates a graph to store Nodes where the Nodes can be identified by their Value.
	 *  Only one node with the same value should be in the graph at any one time. The graph is constructed
	 *  by from a String, which identifies the root node, and a HashMap<String, ArrayList<String>> which 
	 *  identifies the parent-child relationships of the nodes to each other.
	 *  
	 *  This is used in the distance downstream class to hold the data object as the root node and the systems
	 *  and their interfaces as the all the other nodes of the graph. Distance from the root node, weight due
	 *  to distance and Network Weight due to passing data from one system to another are all calculated using
	 *  recursive algorithms.
	 *  
	 * @author jvidalis
	 *
	 */
	public class Graph {

		public ArrayList<Node> nodes= new ArrayList<Node>();
		Node root;
		public Graph()	{	}
		public Graph(Node root)	{this.root = root; nodes.add(root);}
		
		/**
		 * Constructs a graph where the keys of the HashMap are the parent and the values in the arraylist
		 *  are the children to the parent key. Nodes are created for all Strings in the keys and values of
		 *  the HashMap. The root node is also set based on input.
		 * @param inRoot String Value of the Node to be set to root
		 * @param hash HashMap<String, ArrayList<String>> Parent - Children relationships 
		 */
		public Graph(String inRoot, HashMap<String, ArrayList<String>> hash){	
			
			for( String parent : hash.keySet())
			{
				ArrayList<String> relations = hash.get(parent);
				for(String child : relations)
				{
					//System.out.println(parent + "->" + child);
					this.addRelation(parent, child);
				}
			}
			this.setRoot(inRoot);
		}
		/**
		 * Sets the root node with the value of the input string. Will no set the node as root
		 *  if the node does not all ready exist in the string.
		 * @param inRoot
		 */
		public void setRoot(String inRoot)
		{
			for( Node n : nodes)
			{
				if( n.equals(inRoot))
				{
					root = n;
				}
			}
		}
		/**
		 * Prints the Graph to console for debugging.
		 */
		public void printGraph()
		{
			System.out.println("The graph has "+nodes.size() + " nodes.");
			System.out.println("Root Node is: "+root.value);
			for( Node n : nodes )
			{
				n.printNode();
			}		
		}
		/**
		 * Prints the values of each of the values of each node to the console for debugging.
		 */
		public void printGraphValues()
		{
			System.out.println("The graph has "+nodes.size() + " nodes.");
			System.out.println("Root Node is: "+root.value);
			for( Node n : nodes )
			{
				n.printNodeValues();
			}		
		}
		
		/**
		 * Adds a relationship to the graph based on an input parent string and and input child string.
		 *  This will creat a node if the node does not already exist for the values provided.
		 *  
		 * @param parent String 	Value of the parent Node
		 * @param child String		Value of the child Node
		 */
		public void addRelation(String parent, String child)
		{
			Node p = new Node(parent);
			Node c = new Node(child,p);
			
			ArrayList<Node> newRelation = new ArrayList<Node>();
			newRelation.add(p);
			newRelation.add(c);
			this.addGraphArrayList(newRelation);
		}
		
		/**
		 * Adds an arraylist of nodes to the graph 
		 * 
		 * @param inGraph ArrayList<Node> 	List of Nodes to be added to the Graph
		 */
		public void addGraphArrayList(ArrayList<Node> inGraph)
		{
			for(Node n : inGraph)
			{
				this.addNode(n);
				//this.printGraph();
			}
		}
		
		/**
		 * Adds a node to the graph. If a node exists with the same value as the input node, the
		 *  relationships of the two nodes will be combine such that only one node exists with
		 *  all of the relationships.
		 *  
		 * @param n Node 	Node to be added.
		 */
		public void addNode( Node n)
		{
			int index = this.indexOf(n);
			if( index >= 0 )
			{
				for(Node p : n.parents)
				{
					p.addChild(nodes.get(index));
					nodes.get(index).addParent(p);
				}
				for(Node c : n.children)
				{
					c.addParent(nodes.get(index));
					nodes.get(index).addChild(c);
				}
			}
			else
			{
				nodes.add(n);
			}
		}
		
		/**
		 * Determines if the Node with the same value is contained in the graph.
		 * 
		 * @param n Node	Node with value of interest.
		 * @return boolean 	Returns TRUE if there is a node with the same value as the input node.
		 */
		public boolean contains( Node n)
		{
			for( Node x : nodes)
			{
				if( x.equals(n))
				{
					return true;
				}
			}
			return false;
		}
		
		/**
		 * Determines the index of the Node with the same value in the nodes ArrayList<Node> of the Graph
		 * 
		 * @param n Node	Node with value of interest.
		 * @return int 	The index of the Node with the same value of as the input node.
		 */
		public int indexOf( Node n)
		{
			for( int i = 0; i < nodes.size(); i++)
			{
				if( nodes.get(i).equals(n))
				{
					return i;
				}
			}
			return -1;
		}
		
		/**
		 * Processes the downstream values of the nodes in the graph. The distance variable in the node is the
		 *  the distance the node is from the root. Nodes directly connected to the root have a distance value
		 *  of 0. Those connected to a node with a value of 0 distance and not connected to the root will have
		 *  a distance of 1, and so on. Uses a recursive calls.
		 */
		public void processDownstream()
		{
			//root.downstream = 0;
			processDownstream(root);
		}
		private void processDownstream(Node n)
		{
			
			for( Node child : n.children)
			{
				if( child.downstream < 0 || child.downstream > n.downstream + 1)
				{
					//System.out.println(n.value + " Downstream:" + n.downstream);
					child.setDownstream(n.downstream + 1);
					//System.out.println(child.value +" Downstream:" + child.downstream);
					processDownstream(child);
				}
			}
		}
		
		/**
		 * Processes the Weight values of the nodes in the graph. The weight variable in the node is equal to
		 *  r^(distance) of the node.
		 *  
		 *  @param r double 	Value to determine how weight is calculated.
		 */
		public void processWeight(double r)
		{
			for( Node n : nodes)
			{
				n.setWeight(r);
			}
		}
		
		/**
		 * Processes the Network Weight values of the nodes in the graph. The Network Weight is passed up to parents,
		 *  grandparents, and those further upstream. The parents receive the value of the node's weight * r. All Nodes
		 *  Further upstream than the parent receive the value their child received * (1-r). Nodes with multiple parents
		 *  have their propagated weight split among 'true' parents; where true parents have a lower distance than the node.
		 *  Finally Network is normalized by the max network weight in the graph (Not including the root).
		 *  
		 *  @param r double 	Value to determine how network weight is calculated.
		 */
		public void processNetworkWeight(double r)
		{
			for( Node n : nodes)
			{
				n.setNetworkWeight(r);
			}
			double max = maxNetworkWeight();
			for( Node n : nodes)
			{
				n.networkWeight = n.networkWeight/max;
			}
		}
		
		/**
		 * Returns the Max value of the network weights in the graph excluding the root.
		 * 
		 * @return double 	Max value of the network weights in the graph excluding the root.
		 */
		public double maxNetworkWeight()
		{
			double max = 0;
			for( Node n: nodes)
			{
				if( n.networkWeight > max && n != root)
				{
					max = n.networkWeight;
				}
			}
			return max;
		}
		/**
		 * Returns a hashtable where the key is the value of the nodes and the values are the distance property
		 *  of the nodes.
		 *  
		 * @return HashTable<String, Integer> 	
		 */
		public Hashtable<String, Integer> getDownstream()
		{
			Hashtable<String, Integer> table = new Hashtable<String, Integer>();
			for( Node n : nodes )
			{
				if( n != root )
				{
					table.put(n.value, n.downstream);
				}
			}
			return table;
		}
		/**
		 * Returns a hashtable where the key is the value of the nodes and the values are the weight property
		 *  of the nodes.
		 *  
		 * @return HashTable<String, Double> 	
		 */
		public Hashtable<String, Double> getWeight()
		{
			Hashtable<String, Double> table = new Hashtable<String, Double>();
			for( Node n : nodes )
			{
				if( n != root )
				{
					table.put(n.value, n.weight);
				}
			}
			return table;
		}
		/**
		 * Returns a hashtable where the key is the value of the nodes and the values are the networkWeight
		 *  property of the nodes.
		 *  
		 * @return HashTable<String, Double> 	
		 */
		public Hashtable<String, Double> getNetworkWeight()
		{
			Hashtable<String, Double> table = new Hashtable<String, Double>();
			for( Node n : nodes )
			{
				if( n != root )
				{
					table.put(n.value, n.networkWeight);
				}
			}
			return table;
		}
		/**
		 * Returns the size of the graph including the root node.
		 * 
		 * @return int	Size of graph
		 */
		public int size(){return nodes.size();}
		
		/**
		 * Class which comprises a value with several properties. Nodes can be connected to each other using
		 *  a parent child relationship. Used as the primary components of the graph.
		 * @author jvidalis
		 *
		 */
		public class Node{	
			public String value  = "";
			public int downstream=-1;
			public double weight=0;
			public double networkWeight=0;
			//public int rand = (int)Math.round(Math.random()*100);
			public ArrayList<Node> children= new ArrayList<Node>();
			public ArrayList<Node> parents= new ArrayList<Node>();
			/**
			 * Generic Constructor
			 */
			public Node(){}
			/**
			 * Constructor. Sets value to the input string.
			 * 
			 * @param inValue
			 */
			public Node(String inValue){value = inValue;}
			/**
			 * Constructor. Sets value to the input string. Sets the Node input to be a parent of the node.
			 * @param inValue
			 * @param parent
			 */
			public Node(String inValue, Node parent)
			{
				value = inValue;
				this.addParent(parent);
				parent.addChild(this);
			}
			/**
			 * Adds a parent to the Node.
			 * @param inParent
			 */
			public void addParent(Node inParent)
			{
				int index = this.indexOfParent(inParent);
				if( index >= 0)
				{
					//System.out.println("Removing Parent: " + parents.get(index) + " from " + this );
					parents.remove(index);
				}
				//System.out.println("Adding Parent: " + inParent + " to " + this );
				parents.add(inParent);
			}
			/**
			 * Determines if there is a parent of the node with the same value as the input node.
			 * 
			 * @param n
			 * @return
			 */
			public boolean containsParent( Node n)
			{
				for( Node x : parents)
				{
					if( x.value == n.value)
					{
						return true;
					}
				}
				return false;
			}
			/**
			 * Determines the index of the parent in the parents ArrayList<Node> with the same value
			 *  as the input node. Returns -1 if no node is found.
			 *
			 * @param n
			 * @return
			 */
			public int indexOfParent( Node n)
			{
				for( int i = 0; i < parents.size(); i++)
				{
					if( parents.get(i).equals(n))
					{
						return i;
					}
				}
				return -1;
			}
			/**
			 * Adds the input node as a child to the node.
			 * 
			 * @param inChild
			 */
			public void addChild(Node inChild)
			{
				int index = this.indexOfChild(inChild);
				if( index >= 0)
				{
					//System.out.println("Removing Child: " + children.get(index) + " from " + this );
					children.remove(index);
				}
				//System.out.println("Adding Child: " + inChild + " to " + this );
				children.add(inChild);
			}
			/**
			 * Determines if there is a parent of the node with the same value as the input node.
			 * 
			 * @param n
			 * @return
			 */
			public boolean containsChild( Node n)
			{
				
				for( Node x : children)
				{
					if( x.value == n.value)
					{
						return true;
					}
				}
				return false;
			}
			/**
			 * Determines the index of the parent in the parents ArrayList<Node> with the same value
			 *  as the input node. Returns -1 if no node is found.
			 *  
			 * @param n
			 * @return
			 */
			public int indexOfChild( Node n)
			{
				
				for( int i = 0; i < children.size(); i++)
				{
					if( children.get(i).equals(n))
					{
						return i;
					}
				}
				return -1;
			}
			/**
			 * Sets the value of the node to the input value.
			 * 
			 * @param inValue
			 */
			public void setValue( String inValue){value = inValue;}
			/**
			 * Sets the values of the nodes back to their defaults.
			 */
			public void clearValues()
			{
				downstream = -1;
				weight = 0;
				networkWeight = 0;
			}
			/**
			 * Determines if the input has the same value as this node.
			 * 
			 * @param in
			 * @return
			 */
			@Override
			public boolean equals(Object in) {
				if(in == null) {
					return false;
				}
				
				if(in instanceof Node) {
					if(this.value.equals( ((Node) in).value)){
						return true;
					}
				} else if(in instanceof String) {
					if(this.value.equals(in.toString())) {
						return true;
					}
				}
				
				return false;
			}

			/**
			 * Prints a verbose representation of the node to the console.
			 */
			public void printNode()
			{
				System.out.println("Node:" + this);
				System.out.println("Node Downstream:" + downstream);
				System.out.println("Node weight:" + weight);
				System.out.println("Node networkWeight:" + this.networkWeight);
				for(Node s : children)
				{
					System.out.println("\tChild:" + s);
				}
				for(Node s : parents)
				{
					System.out.println("\tParent:" + s);
				}
			}
			/**
			 * Prints a verbose representation of the node's values to the console.
			 */
			public void printNodeValues()
			{
				System.out.print(this);
				System.out.print("\t" + downstream);
				System.out.print("\t" + weight);
				System.out.println("\t" + networkWeight);
			}
			/**
			 * Returns the value of the node.
			 */
			public String toString()
			{
				return value;
				
			}
			/**
			 * Sets the downstream value of the node to the input.
			 * 
			 * @param in int 	New Downstream Valule.
			 */
			public void setDownstream(int in){this.downstream = in;}
			/**
			 * Calculates the weight property of the node from the distance downstream.
			 * 	weight = r^(distance)
			 * @param r
			 */
			public void setWeight(double r){this.weight = Math.pow(r, (double)this.downstream);}
			/**
			 * Uses a series of recursive network values to pass the weight from this node as network weight
			 *  to its upstream providers. This is done through 3 function calls.
			 * @param r
			 */
			public void setNetworkWeight(double r)
			{
				//Starting at the base
				
				ArrayList<Node> realParents = new ArrayList<Node>();
				for(Node p : parents ){	if( p.downstream < this.downstream)	{realParents.add(p);} }
				for(Node p : realParents)
				{
					p.propagateFirstNetworkWeight( this.weight / realParents.size(), r );
				}
				
			}
			private void propagateFirstNetworkWeight( double nw, double r )
			{
				this.networkWeight = this.networkWeight + nw * r;
				ArrayList<Node> realParents = new ArrayList<Node>();
				for(Node p : parents ){	if( p.downstream < this.downstream)	{realParents.add(p);} }
				for(Node p : realParents)
				{
					p.propagateSecondNetworkWeight( nw * r / realParents.size(), r );
				}
			}
			private void propagateSecondNetworkWeight( double nw, double r )
			{
				this.networkWeight = this.networkWeight + nw * (1-r);
				ArrayList<Node> realParents = new ArrayList<Node>();
				for(Node p : parents ){	if( p.downstream < this.downstream)	{realParents.add(p);} }
				for(Node p : realParents)
				{
					p.propagateSecondNetworkWeight( nw * (1-r) / realParents.size(), r );
				}
			}
		}
	}
}
