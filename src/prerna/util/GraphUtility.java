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
package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

	private static final Logger classLogger = LogManager.getLogger(GraphUtility.class);
	private static final Pattern CYPHER_IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z0-9_\\-]+$");

	private static String requireNonBlank(String value, String inputName) {
		if (value == null || (value = value.trim()).isEmpty()) {
			throw new IllegalArgumentException(inputName + " cannot be null or empty");
		}
		return value;
	}

	/**
	 * Cypher doesn't support binding labels/property identifiers via JDBC
	 * placeholders, so we validate against a strict allowlist and then quote.
	 */
	private static String quoteValidatedCypherIdentifier(String identifier, String inputName) {
		String cleanedIdentifier = requireNonBlank(identifier, inputName);
		if (!CYPHER_IDENTIFIER_PATTERN.matcher(cleanedIdentifier).matches()) {
			throw new IllegalArgumentException("Invalid " + inputName + ": " + cleanedIdentifier);
		}
		return "`" + cleanedIdentifier + "`";
	}

	/**
	 * Get metamodel from a type property
	 * 
	 * @param gts
	 * @param graphTypeId
	 * @return
	 */
	public static Map<String, Object> getMetamodel(GraphTraversalSource gts, String graphTypeId) {
		graphTypeId = requireNonBlank(graphTypeId, "graphTypeId");
		HashMap<String, Object> retMap = new HashMap<String, Object>();
		Map<String, ArrayList<String>> edges = new HashMap<>();
		Map<String, Map<String, String>> nodes = new HashMap<>();

		GraphTraversal<Vertex, Map<Object, Object>> gtTest = gts.V().has(graphTypeId).group()
				.by(__.values(graphTypeId));
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
					GraphTraversal<Vertex, Object> testType = gts.V().has(graphTypeId, t).has(nodeProp)
							.values(nodeProp);
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
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String property = resultSet.getString(1);
				properties.add(property);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve all node properties from remote Neo4j", e);
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
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				List<String> resultSetList = (ArrayList<String>) resultSet.getObject(1);
				String label = resultSetList.get(0);
				labels.add(label);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve node labels from remote Neo4j", e);
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
		label = requireNonBlank(label, "label");
		String query = "MATCH (n) WHERE ? IN LABELS(n) WITH KEYS (n) AS keys UNWIND keys AS key RETURN DISTINCT key";
		List<String> properties = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			statement.setString(1, label);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String property = resultSet.getString(1);
				properties.add(property);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve node properties for label '{}'", Utility.cleanLogString(label), e);
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
		List<String> properties = new ArrayList<String>();
		String safeTypeId = quoteValidatedCypherIdentifier(typeId, "typeId");
		typeName = requireNonBlank(typeName, "typeName");
		String query = "MATCH (n) WHERE n." + safeTypeId
				+ " = ? WITH KEYS (n) AS keys UNWIND keys AS key RETURN DISTINCT key";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			statement.setString(1, typeName);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String property = resultSet.getString(1);
				properties.add(property);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve node properties for typeId '{}'", Utility.cleanLogString(typeId), e);
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
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
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
			classLogger.error("Failed to retrieve graph edges from remote Neo4j", e);
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
		String safeTypeId = quoteValidatedCypherIdentifier(typeId, "typeId");
		String query = "MATCH (n)-[r]->(p) UNWIND n." + safeTypeId + " AS StartNode UNWIND p." + safeTypeId
				+ " AS EndNode RETURN DISTINCT StartNode, TYPE(r) AS RelationshipName, EndNode";
		Map<String, Object> edgeMap = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
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
			classLogger.error("Failed to retrieve graph edges for typeId '{}'", Utility.cleanLogString(typeId), e);
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
		String safeTypeId = quoteValidatedCypherIdentifier(typeId, "typeId");
		String query = "MATCH (n) RETURN DISTINCT n." + safeTypeId + " AS node_type";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String node = resultSet.getString("node_type");
				Map<String, String> propMap = new HashMap<>();
				List<String> properties = GraphUtility.getProperties(conn, typeId, node);
				// neo4j does not enforce types so we will assume strings
				for (String prop : properties) {
					propMap.put(prop, SemossDataType.STRING.toString());
				}
				nodeMap.put(node, propMap);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve metamodel for typeId '{}'", Utility.cleanLogString(typeId), e);
		} finally {
			ConnectionUtils.closeAllConnections(null, statement, resultSet);
		}
		if (!nodeMap.isEmpty()) {
			metamodel.put("nodes", nodeMap);
			if (!edges.isEmpty()) {
				metamodel.put("edges", edges);
			}
		}
		return metamodel;
	}

}
