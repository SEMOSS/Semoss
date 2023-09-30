package prerna.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.ResourceIterator;
//import org.neo4j.graphdb.Result;
//import org.neo4j.graphdb.Transaction;

import prerna.algorithm.api.SemossDataType;

public class GraphUtility {

	/**
	 * Get metamodel from a type property
	 * 
	 * @param gts
	 * @param graphTypeId
	 * @return
	 */
	public static Map<String, Object> getMetamodel(GraphTraversalSource gts, String graphTypeId) {
		HashMap<String, Object> retMap = new HashMap<String, Object>();
		Map<String, ArrayList<String>> edges = new HashMap<>();
		Map<String, Map<String, String>> nodes = new HashMap<>();
		
		GraphTraversal<Vertex, Map<Object, Object>> gtTest = gts.V().has(graphTypeId).group().by(__.values(graphTypeId));
		// get the types from the specified prop key
		Set<Object> types = null;
		while (gtTest.hasNext()) {
			Map<Object, Object> v = gtTest.next();
			types = v.keySet();
		}
		if (types != null) {
			for (Object t : types) {
				// get the properties for each type
				GraphTraversal<Vertex, String> x = gts.V().has(graphTypeId, t).properties().key().dedup().order();
				Map<String, String> propMap = new HashMap<>();
				while (x.hasNext()) {
					String nodeProp = x.next();
					// determine data types
					GraphTraversal<Vertex, Object> testType = gts.V().has(graphTypeId, t).has(nodeProp).values(nodeProp);
					int i = 0;
					int limit = 50;
					SemossDataType[] smssTypes = new SemossDataType[limit];
					// might need to default to string
					boolean isString = false;
					boolean next = true;
					while (testType.hasNext() && next) {
						Object value = testType.next();
						Object[] prediction = Utility.determineInputType(value.toString());
						SemossDataType smssType = (SemossDataType) prediction[1];
						if (smssType == SemossDataType.STRING) {
							isString = true;
							break;
						}
						smssTypes[i] = smssType;
						i++;
						if (i <= limit) {
							if (!testType.hasNext()) {
								next = false;
							}
						}
						if (i == limit) {
							next = false;
						}
					}
					if (isString) {
						propMap.put(nodeProp, SemossDataType.STRING.toString());
					} else {
						SemossDataType defaultType = smssTypes[0];
						boolean useDefault = true;
						// check type array if all types are the same
						for (SemossDataType tempType : smssTypes) {
							if (tempType != null) {
								if (tempType != defaultType) {
									// if different types treat all as String
									propMap.put(nodeProp, SemossDataType.STRING.toString());
									useDefault = false;
									break;
								}
							}
						}
						if (useDefault) {
							propMap.put(nodeProp, defaultType.toString());
						}
					}
				}
				nodes.put(t.toString(), propMap);
			}
		}
		// get edges
		Iterator<String> edgeLabels = gts.E().label().dedup();
		while (edgeLabels.hasNext()) {
			String edgeLabel = edgeLabels.next();
			Iterator<Edge> it = gts.V().outE(edgeLabel);
			while (it.hasNext()) {
				Edge edge = it.next();
				Vertex outV = edge.outVertex();
				GraphTraversal<Vertex, Vertex> outTraversal = gts.V(outV.id());
				Set<String> outVKeys = null;
				while (outTraversal.hasNext()) {
					outV = outTraversal.next();
					outVKeys = outV.keys();
				}
				Vertex inV = edge.inVertex();
				GraphTraversal<Vertex, Vertex> inTraversal = gts.V(inV.id());
				Set<String> inVKeys = null;
				while (inTraversal.hasNext()) {
					inV = inTraversal.next();
					inVKeys = inV.keys();
				}
				if (outVKeys != null && inVKeys != null) {
					if (outVKeys.contains(graphTypeId) && inVKeys.contains(graphTypeId)) {
						Object outVLabel = outV.value(graphTypeId);
						Object inVLabel = inV.value(graphTypeId);
						if (!edges.containsKey(edgeLabel)) {
							ArrayList<String> vertices = new ArrayList<>();
							vertices.add(outVLabel.toString());
							vertices.add(inVLabel.toString());
							edges.put(edgeLabel, vertices);
						} else {
							break;
						}
					}
				}
			}
		}
		if (!nodes.isEmpty()) {
			retMap.put("nodes", nodes);
			if (!edges.isEmpty()) {
				retMap.put("edges", edges);
			}
		}
		return retMap;

	}

	/**
	 * Get graph metamodel using the label
	 * 
	 * @param gts
	 * @return
	 */
	public static Map<String, Object> getMetamodel(GraphTraversalSource gts) {
		Map<String, Object> retMap = new HashMap<>();
		Map<String, ArrayList<String>> edges = new HashMap<>();
		Map<String, Map<String, String>> nodes = new HashMap<>();
		// get nodes
		GraphTraversal<Vertex, Map<Object, Object>> it = gts.V().group().by(__.label())
				.by(__.properties().label().dedup().fold());
		while (it.hasNext()) {
			Map<Object, Object> value = it.next();
			for (Object key : value.keySet()) {
				List props = (List) value.get(key);
				Map<String, String> propMap = new HashMap<>();
				for (Object property : props) {
					propMap.put(property + "", SemossDataType.STRING.toString());
				}
				nodes.put(key + "", propMap);
			}
		}

		Iterator<String> edgeLabels = gts.E().label().dedup();
		while (edgeLabels.hasNext()) {
			String edgeLabel = edgeLabels.next();
			Iterator<Edge> eIt = gts.V().outE(edgeLabel);
			while (eIt.hasNext()) {
				Edge edge = eIt.next();
				Vertex outV = edge.outVertex();
				GraphTraversal<Vertex, Vertex> outTraversal = gts.V(outV.id());
				while (outTraversal.hasNext()) {
					outV = outTraversal.next();
				}
				Vertex inV = edge.inVertex();
				GraphTraversal<Vertex, Vertex> inTraversal = gts.V(inV.id());
				while (inTraversal.hasNext()) {
					inV = inTraversal.next();
				}

				if (!edges.containsKey(edgeLabel)) {
					ArrayList<String> vertices = new ArrayList<>();
					vertices.add(outV.label());
					vertices.add(inV.label());
					edges.put(edgeLabel, vertices);
				} else {
					break;
				}
				
			}
		}
		if (!nodes.isEmpty()) {
			retMap.put("nodes", nodes);
			if (!edges.isEmpty()) {
				retMap.put("edges", edges);
			}
		}
		return retMap;
	}

	/**
	 * Get all the node properties for a graph
	 * 
	 * @param gts
	 * @return
	 */
	public static List<String> getAllNodeProperties(GraphTraversalSource gts) {
		ArrayList<String> properties = new ArrayList<>();
		GraphTraversal<Vertex, String> x = gts.V().properties().key().dedup().order();
		while (x.hasNext()) {
			String prop = x.next();
			properties.add(prop);
		}
		return properties;
	}

	////////////////////////////////////////////////////////////////////
	//////////// Graph Utility Methods for Remote Neo4j ////////////////
	////////////////////////////////////////////////////////////////////
	
	/**
	 * Get all the graph properties for all labels
	 * 
	 * @param dbService
	 * @param label
	 * @return
	 */
	public static List<String> getAllNodeProperties(Connection conn) {
		String query = "MATCH (n) WITH KEYS (n) AS keys UNWIND keys AS key RETURN DISTINCT key ORDER BY key";
		List<String> properties = new ArrayList<String>();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String property = resultSet.getString(1);
				properties.add(property);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		return properties;
	}
	
	/**
	 * Get all the labels for a graph
	 * 
	 * @param dbService
	 * @return
	 */
	public static List<String> getNodeLabels(Connection conn) {
		String query = "MATCH (n) RETURN DISTINCT LABELS(n)";
		List<String> labels = new ArrayList<String>();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				List<String> resultSetList = (ArrayList<String>) resultSet.getObject(1);
				String label = (String) resultSetList.get(0);
				labels.add(label);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		return labels;
	}

	/**
	 * Get node properties using a label
	 * 
	 * @param conn
	 * @param label
	 * @return
	 */
	public static List<String> getProperties(Connection conn, String label) {
//		String query = "MATCH (n:?) WITH KEYS (n) AS keys UNWIND keys AS key RETURN DISTINCT key";
//		List<String> properties = new ArrayList<>();
//		try(PreparedStatement statement = conn.prepareStatement(query)){
//			statement.setString(1, label);
//			ResultSet resultSet = statement.executeQuery();
//			while(resultSet.next()){
//				properties.add(resultSet.getString(1));
//			}
//		}catch (SQLException e){
//			e.printStackTrace();
//		}
//		return properties;
		String query = "MATCH (n:" + label + ") WITH KEYS (n) AS keys UNWIND keys AS key RETURN DISTINCT key";
		List<String> properties = new ArrayList<String>();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String property = resultSet.getString(1);
				properties.add(property);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		return properties;
	}

	/**
	 * Get node properties using node type property
	 * 
	 * @param conn
	 * @param typeId
	 * @param typeName
	 * @return
	 */
	public static List<String> getProperties(Connection conn, String typeId, String typeName) {
		typeName = typeName.replaceAll("`", "");
		String query = "Match (n) WHERE n." + typeId + "=`" + typeName + "` WITH KEYS (n) AS keys UNWIND keys AS key return distinct key";
		List<String> properties = new ArrayList<String>();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String property = resultSet.getString(1);
				properties.add(property);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		return properties;
	}
	
	/**
	 * Get graph edges using label
	 * 
	 * @param conn
	 * @return
	 */
	public static Map<String, Object> getEdges(Connection conn) {
		String query = "MATCH (n)-[r]->(p) RETURN DISTINCT labels(n) AS StartNode, TYPE(r) AS RelationshipName , labels(p) as EndNode";
		Map<String, Object> edgeMap = new HashMap<>();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				ArrayList<String> startNodeList = (ArrayList<String>) resultSet.getObject(1);
				String startNode = startNodeList.get(0);
				String relationship = resultSet.getString(2);
				ArrayList<String> endNodeList = (ArrayList<String>) resultSet.getObject(3);
				String endNode = endNodeList.get(0);
				ArrayList<String> nodes = new ArrayList<>();
				nodes.add(startNode);
				nodes.add(endNode);
				edgeMap.put(relationship, nodes);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		return edgeMap;
	}
	
	/**
	 * Get graph edges using the type id
	 * 
	 * @param conn
	 * @param typeId
	 * @return
	 */
	public static Map<String, Object> getEdges(Connection conn, String typeId) {
		String query = "MATCH (n)-[r]->(p) UNWIND n." + typeId + " as StartNode UNWIND p." + typeId + " as EndNode RETURN DISTINCT StartNode, TYPE(r) AS RelationshipName , EndNode";
		Map<String, Object> edgeMap = new HashMap<>();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String startNode = resultSet.getString(1);
				String relationship = resultSet.getString(2);
				String endNode = resultSet.getString(3);
				ArrayList<String> nodes = new ArrayList<>();
				nodes.add(startNode);
				nodes.add(endNode);
				edgeMap.put(relationship, nodes);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		return edgeMap;
	}

	/**
	 * Get the metamodel using node labels
	 * 
	 * @param conn
	 * @return
	 */
	public static Map<String, Object> getMetamodel(Connection conn) {
		Map<String, Object> metamodel = new HashMap<>();
		Map<String, Object> nodeMap = new HashMap<>();
		// get edges
		Map<String, Object> edges = GraphUtility.getEdges(conn);
		// get nodes and properties
		List<String> nodes = GraphUtility.getNodeLabels(conn);
		for (String label : nodes) {
			Map<String, String> propMap = new HashMap<>();
			List<String> properties = GraphUtility.getProperties(conn, label);
			// neo4j does not enforce types so we will assume strings
			for (String prop : properties) {
				propMap.put(prop, SemossDataType.STRING.toString());
			}
			nodeMap.put(label, propMap);
		}
		if (!nodeMap.isEmpty()) {
			metamodel.put("nodes", nodeMap);
			if (!edges.isEmpty()) {
				metamodel.put("edges", edges);
			}
		}
		return metamodel;
	}
	
	/**
	 * Get the metamodel using node type property
	 * 
	 * @param conn
	 * @return
	 */
	public static Map<String, Object> getMetamodel(Connection conn, String typeId) {
		Map<String, Object> metamodel = new HashMap<>();
		Map<String, Object> nodeMap = new HashMap<>();
		// get edges
		Map<String, Object> edges = GraphUtility.getEdges(conn, typeId);
		// get nodes and properties
		String query = "MATCH (n) RETURN DISTINCT n."+ typeId +" AS " + typeId;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String node = resultSet.getString(1);
				Map<String, String> propMap = new HashMap<>();
				List<String> properties = GraphUtility.getProperties(conn, typeId, node);
				// neo4j does not enforce types so we will assume strings
				for (String prop : properties) {
					propMap.put(prop, SemossDataType.STRING.toString());
				}
				nodeMap.put(node, propMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				 if(statement != null) {
					 statement.close();
			        }
			} catch (SQLException e) {
		          e.printStackTrace();
			}

		}
		if (!nodeMap.isEmpty()) {
			metamodel.put("nodes", nodeMap);
			if (!edges.isEmpty()) {
				metamodel.put("edges", edges);
			}
		}
		return metamodel;
	}
	
	
	////////////////////////////////////////////////////////////////////
	//////////// Graph Utility Methods for Embedded Neo4j //////////////
	////////////////////////////////////////////////////////////////////

	/*
	 * Since neo4j-tinkerpop-api-impl is no longer supported
	 * Removing logic around interacting with neo4j through gremlin
	 */
	
//	/**
//	 * Get the metamodel from a neo4j graph using the labels
//	 * 
//	 * @param dbService
//	 * @return
//	 */
//	public static Map<String, Object> getMetamodel(GraphDatabaseService dbService) {
//		Map<String, Object> metamodel = new HashMap<>();
//		Map<String, Object> nodeMap = new HashMap<>();
//		// get edges
//		Map<String, Object> edges = GraphUtility.getEdges(dbService);
//		// get nodes and properties
//		List<String> nodes = GraphUtility.getNodeLabels(dbService);
//		for (String label : nodes) {
//			Map<String, String> propMap = new HashMap<>();
//			List<String> properties = GraphUtility.getProperties(dbService, label);
//			// neo4j does not enforce types so we will assume strings
//			for(String prop: properties) {
//				propMap.put(prop, SemossDataType.STRING.toString());
//			}
//			nodeMap.put(label, propMap);
//		}
//		if (!nodeMap.isEmpty()) {
//			metamodel.put("nodes", nodeMap);
//			if (!edges.isEmpty()) {
//				metamodel.put("edges", edges);
//			}
//		}
//		return metamodel;
//	}
//	
//	/**
//	 * Get the metamodel from a neo4j graph using a property to identify the type
//	 * 
//	 * @param dbService
//	 * @return
//	 */
//	public static Map<String, Object> getMetamodel(GraphDatabaseService dbService, String typeId) {
//		Map<String, Object> metamodel = new HashMap<>();
//		Map<String, Object> nodeMap = new HashMap<>();
//		// get edges
//		Map<String, Object> edges = GraphUtility.getEdges(dbService, typeId);
//		// get the list of nodes
//		Transaction tx = dbService.beginTx();
//		String query = "MATCH (n) RETURN DISTINCT n."+ typeId +" AS " + typeId;
//		Result result = dbService.execute(query);
//		while (result.hasNext()) {
//			String node = (String) result.next().get(typeId);
//			Map<String, String> propMap = new HashMap<>();
//			List<String> nodeProperties = GraphUtility.getProperties(dbService, typeId, node);
//			for(String prop: nodeProperties) {
//				propMap.put(prop, SemossDataType.STRING.toString());
//			}
//			nodeMap.put(node, propMap);
//		}
//		tx.close();
//		if (!nodeMap.isEmpty()) {
//			metamodel.put("nodes", nodeMap);
//			if (!edges.isEmpty()) {
//				metamodel.put("edges", edges);
//			}
//		}
//		return metamodel;
//	}
//	
//	/**
//	 * Get the node properties by querying the node using the type property
//	 * 
//	 * @param dbService
//	 * @param typeId
//	 * @return
//	 */
//	private static List<String> getProperties(GraphDatabaseService dbService, String typeId, String typeName) {
//		List<String> properties = new ArrayList<>();
//		Transaction tx = dbService.beginTx();
//		String query = "Match (n) WHERE n." + typeId + "='" + typeName
//				+ "' WITH KEYS (n) AS keys UNWIND keys AS key return distinct key";
//		Result result = dbService.execute(query);
//		while (result.hasNext()) {
//			String node = (String) result.next().get("key");
//			properties.add(node);
//		}
//		tx.success();
//		return properties;
//	}
//
//	/**
//	 * Get all the labels for a graph
//	 * 
//	 * @param dbService
//	 * @return
//	 */
//	public static List<String> getNodeLabels(GraphDatabaseService dbService) {
//		Transaction tx = dbService.beginTx();
//		ResourceIterator<Label> labelsIt = dbService.getAllLabels().iterator();
//		List<String> labels = new ArrayList<String>();
//		while (labelsIt.hasNext()) {
//			Label l = labelsIt.next();
//			String name = l.name();
//			labels.add(name);
//		}
//		tx.close();
//		return labels;
//	}
//
//	/**
//	 * Get all the graph properties for a label
//	 * 
//	 * @param dbService
//	 * @param label
//	 * @return
//	 */
//	public static List<String> getProperties(GraphDatabaseService dbService, String label) {
//		List<String> properties = new ArrayList<String>();
//		Transaction tx = dbService.beginTx();
//		String query = "MATCH (a:" + label + ") UNWIND keys(a) AS key RETURN distinct key as property";
//		Result result = dbService.execute(query);
//		while (result.hasNext()) {
//			String prop = (String) result.next().get("property");
//			properties.add(prop);
//		}
//		tx.close();
//		return properties;
//	}
//	
//	/**
//	 * Get all the graph properties for all labels
//	 * 
//	 * @param dbService
//	 * @param label
//	 * @return
//	 */
//	public static List<String> getAllNodeProperties(GraphDatabaseService dbService) {
//		List<String> properties = new ArrayList<String>();
//		Transaction tx = dbService.beginTx();
//		String query = "MATCH (a) UNWIND keys(a) AS key RETURN distinct key as property";
//		Result result = dbService.execute(query);
//		while (result.hasNext()) {
//			String prop = (String) result.next().get("property");
//			properties.add(prop);
//		}
//		tx.close();
//		return properties;
//	}
//
//	/**
//	 * Get map of edges: {edgeLabel:[startNode, endNode]}
//	 * 
//	 * @param dbService
//	 * @return
//	 */
//	public static Map<String, Object> getEdges(GraphDatabaseService dbService) {
//		Transaction tx = dbService.beginTx();
//		String query = "MATCH (n)-[r]->(p) UNWIND labels(n) as StartNode UNWIND labels(p) as EndNode RETURN DISTINCT StartNode, TYPE(r) AS RelationshipName , EndNode";
//		Result result = dbService.execute(query);
//		Map<String, Object> edgeMap = new HashMap<>();
//		while (result.hasNext()) {
//			Map<String, Object> map = result.next();
//			String startNode = (String) map.get("StartNode");
//			String rel = (String) map.get("RelationshipName");
//			String endNode = (String) map.get("EndNode");
//			ArrayList<String> nodes = new ArrayList<>();
//			nodes.add(startNode);
//			nodes.add(endNode);
//			edgeMap.put(rel, nodes);
//		}
//		tx.close();
//		return edgeMap;
//	}
//	
//	/**
//	 * Get map of edges: {edgeLabel:[startNode, endNode]}
//	 * 
//	 * @param dbService
//	 * @return
//	 */
//	public static Map<String, Object> getEdges(GraphDatabaseService dbService, String typeId) {
//		Transaction tx = dbService.beginTx();
//		String query = "MATCH (n)-[r]->(p) UNWIND n." + typeId + " as StartNode UNWIND p." + typeId + " as EndNode RETURN DISTINCT StartNode, TYPE(r) AS RelationshipName , EndNode";
//		Result result = dbService.execute(query);
//		Map<String, Object> edgeMap = new HashMap<>();
//		while (result.hasNext()) {
//			Map<String, Object> map = result.next();
//			String startNode = (String) map.get("StartNode");
//			String rel = (String) map.get("RelationshipName");
//			String endNode = (String) map.get("EndNode");
//			ArrayList<String> nodes = new ArrayList<>();
//			nodes.add(startNode);
//			nodes.add(endNode);
//			edgeMap.put(rel, nodes);
//		}
//		tx.close();
//		return edgeMap;
//	}
}
