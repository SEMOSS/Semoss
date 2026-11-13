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
package prerna.ds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.crypto.Cipher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.util.ArrayUtilityMethods;
import prerna.util.MyGraphIoMappingBuilder;
import prerna.util.Utility;

public class TinkerFrame extends AbstractTableDataFrame {

	private static final Logger classLogger = LogManager.getLogger(TinkerFrame.class);

	public static final String PRIM_KEY = "_GEN_PRIM_KEY";
	public static final String EMPTY = "_";
	public static final String EDGE_LABEL_DELIMETER = "+++";
	public static final String EDGE_LABEL_DELIMETER_REGEX_SPLIT = "\\+\\+\\+";
	public static final String PRIM_KEY_DELIMETER = ":::";

	public static final String TINKER_ID = "_T_ID";
	// public static final String TINKER_VALUE = "_T_VALUE";
	public static final String TINKER_TYPE = "_T_TYPE";
	public static final String TINKER_NAME = "_T_NAME";
	public static final String TINKER_FILTER = "_T_FILTER";
	public static final String TINKER_COUNT = "_T_COUNT";

	// keeps the cache of whether a column is numerical or not
	protected Map<String, Boolean> isNumericalMap = new HashMap<String, Boolean>();

	public TinkerGraph g = null;

	private boolean iGraphSynched = false;

	/******************************
	 * CONSTRUCTORS
	 ******************************/

	public TinkerFrame() {
		g = TinkerGraph.open();
		g.createIndex(TINKER_TYPE, Vertex.class);
		g.createIndex(TINKER_ID, Vertex.class);
		g.createIndex(TINKER_ID, Edge.class);
		g.createIndex(T.label.toString(), Edge.class);
		setDefaultName();
		this.originalName = this.frameName;
	}

	public TinkerFrame(String alias) {
		g = TinkerGraph.open();
		g.createIndex(TINKER_TYPE, Vertex.class);
		g.createIndex(TINKER_ID, Vertex.class);
		g.createIndex(TINKER_ID, Edge.class);
		g.createIndex(T.label.toString(), Edge.class);
		this.frameName = alias;
		this.originalName = this.frameName;
	}

	public TinkerFrame(String[] headerNames) {
		this();
		this.qsNames = headerNames;
	}

	private void setDefaultName() {
		String uuid = UUID.randomUUID().toString().toUpperCase();
		uuid = uuid.replace("-", "_");
		setName("TINKER_" + uuid);
	}

	public boolean isIGraphSynched() {
		return iGraphSynched;
	}

	public void setIGraphSynched(boolean iGraphSynched) {
		this.iGraphSynched = iGraphSynched;
	}

	/******************************
	 * END CONSTRUCTORS
	 ******************************/

	/******************************
	 * GRAPH OUTPUT METHODS
	 ******************************/

	private Map<String, Object> createVertStores() {
		Map<String, SEMOSSVertex> vertStore = new HashMap<>();
		Map<String, SEMOSSEdge> edgeStore = new HashMap<>();

		// get all edges not attached to a filter node or is a filtered edge
		GraphTraversal<Edge, Edge> edgesIt = g.traversal().E().not(__.or(__.has(TINKER_TYPE, TINKER_FILTER),
				__.bothV().in().has(TINKER_TYPE, TINKER_FILTER), __.V().has(PRIM_KEY, true)));
		while (edgesIt.hasNext()) {
			Edge e = edgesIt.next();
			Vertex outV = e.outVertex();

			Vertex inV = e.inVertex();
			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);

			SEMOSSEdge semossE = new SEMOSSEdge(outVert, inVert,
					"https://semoss.org/Relation/" + e.property(TINKER_ID).value() + "");
			edgeStore.put("https://semoss.org/Relation/" + e.property(TINKER_ID).value() + "", semossE);

			// need to add edge properties
			Iterator<Property<Object>> edgeProperties = e.properties();
			while (edgeProperties.hasNext()) {
				Property<Object> prop = edgeProperties.next();
				String propName = prop.key();
				if (!propName.equals(TINKER_ID) && !propName.equals(TINKER_NAME) && !propName.equals(TINKER_TYPE)) {
					semossE.propHash.put(propName, prop.value());
				}
			}
		}

		// now i just need to get the verts with no edges
		// GraphTraversal<Vertex, Vertex> vertIt =
		// g.traversal().V().not(__.or(__.both(),__.has(TINKER_TYPE,
		// TINKER_FILTER),__.in().has(TINKER_TYPE, TINKER_FILTER)));

		// not (has type filter or has in node type filter)
		// = not has type filter or not has in node type filter
		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.has(TINKER_TYPE, TINKER_FILTER),
				__.in().has(TINKER_TYPE, TINKER_FILTER), __.has(PRIM_KEY, true)));
		// GraphTraversal<Vertex, Vertex> vertIt =
		// g.traversal().V().not(__.in().has(TINKER_TYPE, TINKER_FILTER));
		while (vertIt.hasNext()) {
			Vertex outV = vertIt.next();
			// if(!outV.property("TYPE").equals(TINKER_FILTER)) {
			getSEMOSSVertex(vertStore, outV);
			// }
		}

		Map<String, Object> retHash = new HashMap<>();
		retHash.put("nodes", vertStore);
		retHash.put("edges", edgeStore.values());
		return retHash;
	}

	/**
	 * Returns the SEMOSS vertex for the tinker vertex, creating it from the
	 * vertex's properties and adding it to the store when it is not there yet.
	 * 
	 * @param vertStore  the SEMOSS vertices by URI
	 * @param tinkerVert the tinker vertex
	 * @return the SEMOSS vertex
	 */
	private SEMOSSVertex getSEMOSSVertex(Map<String, SEMOSSVertex> vertStore, Vertex tinkerVert) {
		Object value = tinkerVert.property(TINKER_NAME).value();
		String type = tinkerVert.property(TINKER_TYPE).value() + "";

		// construct the URI without the base URI, since the base URI sits on the OWL
		// and is added when the query is created
		String newValue = Utility.getInstanceName(value.toString());
		String uri = "http://semoss.org/ontologies/Concept/" + type + "/" + newValue;

		SEMOSSVertex semossVert = vertStore.get(uri);
		if (semossVert == null) {
			semossVert = new SEMOSSVertex(uri);
			// generic - move anything that is a property on the node
			Iterator<VertexProperty<Object>> vertexProperties = tinkerVert.properties();
			while (vertexProperties.hasNext()) {
				VertexProperty<Object> prop = vertexProperties.next();
				String propName = prop.key();
				if (!propName.equals(TINKER_ID) && !propName.equals(TINKER_NAME) && !propName.equals(TINKER_TYPE)) {
					semossVert.propHash.put(propName, prop.value());
				}
			}
			vertStore.put(uri, semossVert);
		}
		return semossVert;
	}

	/******************************
	 * END GRAPH OUTPUT METHODS
	 ******************************/

	/******************************
	 * GRAPH SPECIFIC METHODS
	 ******************************/

	// TODO: need to update and remove uniqueName from method signature
	// create or add vertex
	protected Vertex upsertVertex(String type, Object data) {
		if (data == null) {
			data = EMPTY;
		}
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
		// GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TINKER_TYPE,
		// type).has(TINKER_ID, type + ":" + data);
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TINKER_ID, type + ":" + data);
		if (gt.hasNext()) {
			retVertex = gt.next();
		} else {
			if (data instanceof Number) {
				// need to keep values as they are, not with XMLSchema tag
				// push the actual value as well, who knows when you would need it
				retVertex = g.addVertex(TINKER_ID, type + ":" + data, TINKER_TYPE, type, TINKER_NAME, data);
			} else {
				// LOGGER.debug(" adding vertex ::: " + TINKER_ID + " = " + type + ":" + data+ "
				// & " + TINKER_VALUE+ " = " + value+ " & " + TINKER_TYPE+ " = " + type+ " & " +
				// TINKER_NAME+ " = " + data);
				// LOGGER.debug(" adding vertex ::: " + TINKER_ID + " = " + type + ":" + data+ "
				// & " + " & " + TINKER_TYPE+ " = " + type+ " & " + TINKER_NAME+ " = " + data);
				// push the actual value as well, who knows when you would need it
				retVertex = g.addVertex(TINKER_ID, type + ":" + data, TINKER_TYPE, type, TINKER_NAME, data.toString());
			}

		}
		return retVertex;
	}

	/**
	 * Returns the edge between the two vertices. An existing edge has its count
	 * increased by one, and a new edge starts with a count of one.
	 * 
	 * @param fromVertex           the vertex the edge starts at
	 * @param fromVertexUniqueName the column of the start vertex
	 * @param toVertex             the vertex the edge ends at
	 * @param toVertexUniqueName   the column of the end vertex
	 * @return the edge
	 */
	protected Edge upsertEdge(Vertex fromVertex, String fromVertexUniqueName, Vertex toVertex,
			String toVertexUniqueName) {
		Edge retEdge = null;
		String type = fromVertexUniqueName + EDGE_LABEL_DELIMETER + toVertexUniqueName;
		String edgeID = type + "/" + fromVertex.value(TINKER_NAME) + ":" + toVertex.value(TINKER_NAME);
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(TINKER_ID, edgeID);
		if (gt.hasNext()) {
			retEdge = gt.next();
			Integer count = (Integer) retEdge.value(TINKER_COUNT);
			count++;
			retEdge.property(TINKER_COUNT, count);
		} else {
			retEdge = fromVertex.addEdge(type, toVertex, TINKER_ID, edgeID, TINKER_COUNT, 1);
		}

		return retEdge;
	}

	/******************************
	 * END GRAPH SPECIFIC METHODS
	 ******************************/

	/******************************
	 * AGGREGATION METHODS
	 ******************************/

	@Override
	public void addRow(Object[] rowCleanData, String[] headerNames) {
		// if(rowCleanData.length != headerNames.length) {
		// throw new IllegalArgumentException("Input row must have same dimensions as
		// levels in dataframe."); // when the HELL would this ever happen ?
		// }
		//
		// String rowString = "";
		// Vertex[] toVertices = new Vertex[headerNames.length];
		// for(int index = 0; index < headerNames.length; index++) {
		// Vertex toVertex = upsertVertex(headerNames[index], rowCleanData[index]); //
		// need to discuss if we need specialized vertices too
		// toVertices[index] = toVertex;
		// rowString = rowString+rowCleanData[index]+":";
		// }
		//
		// String nextPrimKey = rowString.hashCode()+"";
		// Vertex primVertex = upsertVertex(this.metaData.getLatestPrimKey(),
		// nextPrimKey);
		//
		// for(int i = 0; i < toVertices.length; i++) {
		// this.upsertEdge(primVertex, this.metaData.getLatestPrimKey(), toVertices[i],
		// headerNames[i]);
		// }
		//
		// //Need to update Header Names if incoming headers is different from stored
		// header names
		// if(this.headerNames == null) {
		// this.headerNames = headerNames;
		// }
	}

	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Set<String>> edgeHash,
			Map<String, String> logicalToTypeMap) {
		boolean hasRel = false;
		for (String startNode : rowCleanData.keySet()) {
			Set<String> set = edgeHash.get(startNode);
			if (set == null) {
				continue;
			}

			for (String endNode : set) {
				if (rowCleanData.keySet().contains(endNode)) {
					hasRel = true;

					// get from vertex
					Object startNodeValue = rowCleanData.get(startNode);
					String startNodeType = logicalToTypeMap.get(startNode);
					Vertex fromVertex = upsertVertex(startNodeType, startNodeValue);

					// get to vertex
					Object endNodeValue = rowCleanData.get(endNode);
					String endNodeType = logicalToTypeMap.get(endNode);
					Vertex toVertex = upsertVertex(endNodeType, endNodeValue);

					upsertEdge(fromVertex, startNodeType, toVertex, endNodeType);
				}
			}
		}

		// this replaces the addRow method, which needs to be called on the first pass
		// since edges do not exist yet
		if (!hasRel) {
			String singleColName = rowCleanData.keySet().iterator().next();
			String singleNodeType = logicalToTypeMap.get(singleColName);
			Object startNodeValue = rowCleanData.get(singleColName);
			upsertVertex(singleNodeType, startNodeValue);
		}

	}

	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality) {
		boolean hasRel = false;

		for (Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if (endIndices == null) {
				continue;
			}

			for (Integer endIndex : endIndices) {
				hasRel = true;

				// get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = headers[startIndex];
				Object startNodeValue = values[startIndex];
				Vertex fromVertex = upsertVertex(startNode, startNodeValue);

				// get to vertex
				String endNode = headers[endIndex];
				String endUniqueName = headers[endIndex];
				Object endNodeValue = values[endIndex];
				Vertex toVertex = upsertVertex(endNode, endNodeValue);

				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}

		// this replaces the addRow method, which needs to be called on the first pass
		// since edges do not exist yet
		if (!hasRel) {
			String singleColName = headers[0];
			String singleNodeType = singleColName;
			Object startNodeValue = values[0];
			upsertVertex(singleNodeType, startNodeValue);
		}
	}

	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality,
			Map<String, String> logicalToTypeMap) {
		boolean hasRel = false;

		for (Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if (endIndices == null) {
				continue;
			}

			for (Integer endIndex : endIndices) {
				hasRel = true;

				// get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = logicalToTypeMap.get(startNode);
				if (startUniqueName == null) {
					startUniqueName = startNode;
				}
				Object startNodeValue = values[startIndex];
				Vertex fromVertex = upsertVertex(startNode, startNodeValue);

				// get to vertex
				String endNode = headers[endIndex];
				String endUniqueName = logicalToTypeMap.get(endNode);
				if (endUniqueName == null) {
					endUniqueName = endNode;
				}
				Object endNodeValue = values[endIndex];
				Vertex toVertex = upsertVertex(endNode, endNodeValue);

				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}

		// this replaces the addRow method, which needs to be called on the first pass
		// since edges do not exist yet
		if (!hasRel) {
			String singleColName = headers[0];
			String singleNodeType = logicalToTypeMap.get(singleColName);
			Object startNodeValue = values[0];
			upsertVertex(singleNodeType, startNodeValue);
		}
	}

	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality,
			String[] logicalToTypeMap) {
		boolean hasRel = false;

		for (Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if (endIndices == null) {
				continue;
			}

			for (Integer endIndex : endIndices) {
				hasRel = true;

				// get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = logicalToTypeMap[startIndex];
				Object startNodeValue = values[startIndex];
				Vertex fromVertex = upsertVertex(startNode, startNodeValue);

				// get to vertex
				String endNode = headers[endIndex];
				String endUniqueName = logicalToTypeMap[endIndex];
				Object endNodeValue = values[endIndex];
				Vertex toVertex = upsertVertex(endNode, endNodeValue);

				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}

		// this replaces the addRow method, which needs to be called on the first pass
		// since edges do not exist yet
		if (!hasRel) {
			String singleNodeType = logicalToTypeMap[0];
			Object startNodeValue = values[0];
			upsertVertex(singleNodeType, startNodeValue);
		}
	}

	/******************************
	 * END AGGREGATION METHODS
	 ******************************/

	/******************************
	 * FILTER METHODS
	 ******************************/

	/**
	 * Removes the vertices of the column that hold any of the values.
	 * 
	 * @param columnHeader the column to remove values from
	 * @param removeValues the values to remove
	 */
	public void remove(String columnHeader, List<Object> removeValues) {
		// for each value
		for (Object val : removeValues) {
			String id = columnHeader + ":" + val.toString();

			// find the vertex
			GraphTraversal<Vertex, Vertex> fgt = g.traversal().V().has(TINKER_ID, id);
			Vertex nextVertex = null;
			if (fgt.hasNext()) {
				// remove
				nextVertex = fgt.next();
				nextVertex.remove();
			}
		}
	}

	/******************************
	 * END FILTER METHODS
	 ******************************/

	/******************************
	 * TRAVERSAL METHODS
	 ******************************/

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if (isNumeric(columnHeader)) {
			GremlinInterpreter interp = new GremlinInterpreter(this.g.traversal(), this.metaData);
			SelectQueryStruct qs = new SelectQueryStruct();
			// add selector
			qs.addSelector(new QueryColumnSelector(columnHeader));
			// add filters
			qs.mergeImplicitFilters(this.grf);
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
			interp.setQueryStruct(qs);
			try (RawGemlinSelectWrapper it = new RawGemlinSelectWrapper(interp, qs)) {
				it.execute();
				List<Double> columnList = new ArrayList<>();
				while (it.hasNext()) {
					columnList.add(((Number) it.next().getValues()[0]).doubleValue());
				}

				return columnList.toArray(new Double[columnList.size()]);
			} catch (IOException e) {
				logger.error("Failed to flush column {} as a double array", columnHeader);
				classLogger.error("Error flushing column {} as a double array", columnHeader, e);
			}
		}
		return null;
	}

	@Override
	public void removeColumn(String columnHeader) {
		// if column header doesn't exist, do nothing
		if (!ArrayUtilityMethods.arrayContainsValue(this.qsNames, columnHeader)) {
			return;
		}

		// A couple of thoughts from Bill Sutton
		// there are quite a few interesting scenarios here
		// the first question is: do we want to maintain duplicate rows after a column
		// is removed? I could see yes and no depending on the scenario
		// If yes, primary keys of some sort will have to be used. if the tinker already
		// has PKs, we are good to go. Otherwise, we are probably best off just removing
		// the column from the selectors since it will need PKs anyway
		// If no, primary keys cause a big issue--would have to remove the nodes of
		// interest and then clean up extra PKs
		// If no and no primary keys we again have a couple scenarios. If the node is on
		// the fringe of the tinker, good to go--just remove it. If the node is in the
		// middle... not sure exactly what we can do--kind of similar to the issue
		// above (no and pks)

		// For now, the most common use for this will be through explore when clicking
		// through the metamodel. This scenario will also be don't keep duplicates, no
		// pk, node is on the fringe. I am handling that here:
		// Remove the actual nodes from tinker
		logger.info("REMOVING COLUMN :::: {}", columnHeader);
		// delete from the instances

		// if columnHeader has incoming prim key with no other outgoing types, delete
		// that prim key first, then delete the columnHeader
		String columnValue = columnHeader; // this.metaData.getValueForUniqueName(columnHeader);
		GraphTraversal<Vertex, Vertex> primKeyTraversal = g.traversal().V().has(TINKER_NAME, PRIM_KEY).as("PrimKey")
				.out(PRIM_KEY + EDGE_LABEL_DELIMETER + columnValue).has(TINKER_TYPE, columnValue)
				.in(PRIM_KEY + EDGE_LABEL_DELIMETER + columnValue).has(TINKER_NAME, PRIM_KEY).as("PrimKey2")
				.where("PrimKey", P.eq("PrimKey2"));
		while (primKeyTraversal.hasNext()) {
			Vertex nextPrimKey = primKeyTraversal.next();
			Iterator<Vertex> verts = nextPrimKey.vertices(Direction.OUT);

			boolean delete = true;
			while (verts.hasNext()) {
				delete = verts.next().value(TINKER_TYPE).equals(columnHeader);
				if (!delete) {
					delete = false;
					break;
				}
			}
			if (delete) {
				nextPrimKey.remove();
			}
		}

		g.traversal().V().has(TINKER_TYPE, columnValue).drop().iterate();
		// remove the node from meta
		this.metaData.dropVertex(columnHeader);
		this.syncHeaders();
	}

	public Object degree(String type, String data) {
		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().has(TINKER_ID, type + ":" + data).group()
				.by().by(__.bothE().count());
		Object degree = null;
		if (gt.hasNext()) {
			Map<Object, Object> map = gt.next();
			Iterator mapKeys = map.keySet().iterator();
			while (mapKeys.hasNext()) {
				Object key = mapKeys.next();
				Object value = map.get(key);
				degree = value;

				logger.info("{}<<>>{}", ((Vertex) key).value(TINKER_ID), value);
			}
		}
		return degree;
	}

	public Long eigen(String type, String data) {
		Long retLong = null;
		GraphTraversal<Vertex, Map<String, Object>> gt2 = g.traversal().V()
				.repeat(__.groupCount("m").by(TINKER_ID).out()).times(5).cap("m").V().select("m");
		if (gt2.hasNext()) {
			Map<String, Object> map = gt2.next();
			retLong = (Long) map.get(type + ":" + data);
		}

		return retLong;
	}

	public void printEigenMatrix() {
		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V()
				.repeat(__.groupCount("m").by(TINKER_ID).out()).times(5).cap("m");
		// order(Scope.local).by(__.values(), Order.decr).limit(Scope.local, 10).next();
		if (gt.hasNext()) {
			Map<Object, Object> map = gt.next();
			Iterator mapKeys = map.keySet().iterator();
			while (mapKeys.hasNext()) {
				Object key = mapKeys.next();
				Object value = map.get(key);
				logger.info("{}<<>>{}", Utility.cleanLogString(key.toString()), value);
				// logger.info(((Vertex)key).value(TINKER_ID) + "<<>>" + value);
			}
		}
	}

	@Override
	public long size(String tableName) {
		// get a flat QS
		// which contains all the selectors
		// and all the joins as inner
		SelectQueryStruct qs = this.metaData.getFlatTableQs(false);
		if (qs.getSelectors().isEmpty()) {
			return 0;
		}

		GremlinInterpreter interp = new GremlinInterpreter(this.g.traversal(), this.metaData);
		interp.setLogger(this.logger);
		interp.setQueryStruct(qs);

		GraphTraversal it = interp.composeIterator().count();
		if (it.hasNext()) {
			return ((Number) it.next()).longValue();
		}

		return 0;
	}

	public boolean isOrphan(String type, String data) {
		boolean retValue = false;
		GraphTraversal<Vertex, Edge> gt = g.traversal().V().has(TINKER_ID, type + ":" + data).bothE();
		if (gt.hasNext()) {
			retValue = false;
		} else {
			retValue = true;
		}

		return retValue;
	}

	public Map<String, Object> getGraphOutput() {
		return createVertStores();
	}

	/**
	 * Removes all nodes that are not META and are not part of the main query
	 * return. This keeps the graph as small as possible as we are making joins.
	 * Blank nodes must be used to keep nodes in the tinker that do not connect to
	 * every type.
	 */
	public void removeExtraneousNodes() {
		// LOGGER.info("removing extraneous nodes");
		// GremlinBuilder builder = new GremlinBuilder(g);
		// List<String> selectors = builder.generateFullEdgeTraversal();
		// builder.addSelector(selectors);
		// GraphTraversal gt = builder.executeScript();
		// if(selectors.size()>1){
		// gt = gt.mapValues();
		// }
		// GraphTraversal metaT = g.traversal().V().has(TINKER_TYPE, META).outE();
		// while(metaT.hasNext()){
		// gt = gt.inject(metaT.next());
		// }
		//
		// TinkerGraph newG = (TinkerGraph)
		// gt.subgraph("subGraph").cap("subGraph").next();
		// this.g = newG;
		// LOGGER.info("extraneous nodes removed");
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();
		String randFrameName = "Tinker" + Utility.getRandomString(6);
		cf.setFrameName(randFrameName);

		String frameFileName = folderDir + DIR_SEPARATOR + randFrameName + ".tg";
		// save frame
		Builder<GryoIo> builder = GryoIo.build();
		builder.graph(this.g);
		builder.onMapper(new MyGraphIoMappingBuilder());
		GryoIo writer = builder.create();
		try {
			writer.writeGraph(frameFileName);
		} catch (IOException e) {
			logger.error("Failed to write tinker graph frame to cache file {}", frameFileName);
			classLogger.error("Failed to write tinker graph frame to cache file {}", frameFileName, e);
			throw new IOException("Error occurred attempting to cache graph frame");
		}
		cf.setFrameCacheLocation(frameFileName);

		// also save the meta details
		this.saveMeta(cf, folderDir, randFrameName, cipher);
		return cf;
	}

	@Override
	public void open(CachePropFileFrameObject cf, Cipher cipher) {
		// load the frame
		try {
			Builder<GryoIo> builder = GryoIo.build();
			builder.graph(this.g);
			builder.onMapper(new MyGraphIoMappingBuilder());
			GryoIo reader = builder.create();
			reader.readGraph(cf.getFrameCacheLocation());
		} catch (IOException e) {
			logger.error("Failed to read tinker graph frame from cache file {}", cf.getFrameCacheLocation());
			classLogger.error("Failed to read tinker graph frame from cache file {}", cf.getFrameCacheLocation(), e);
		}

		// open the meta details
		this.openCacheMeta(cf, cipher);
	}

	public void insertBlanks(String colName, List<String> addedColumns) {
		// // for each node in colName
		// // if it does not have a relationship to any node in any of the addedColumns
		// // add that node to a blank
		// LOGGER.info("PERFORMING inserting of blanks.......");
		// for(String addedType : addedColumns){
		// Vertex emptyV = null;
		// String colValue = colName; //this.metaData.getValueForUniqueName(colName);
		// String addedValue = addedType;
		// //this.metaData.getValueForUniqueName(addedType);
		// boolean forward = false;
		// GraphTraversal<Vertex, Vertex> gt = null;
		// if(this.metaData.isConnectedInDirection(colValue, addedType)){
		// forward = true;
		// gt = g.traversal().V().has(TINKER_TYPE,
		// colValue).not(__.out(colValue+EDGE_LABEL_DELIMETER+addedValue).has(TINKER_TYPE,
		// addedValue));
		// }
		// else {
		// gt = g.traversal().V().has(TINKER_TYPE,
		// colValue).not(__.in(addedValue+EDGE_LABEL_DELIMETER+colValue).has(TINKER_TYPE,
		// addedValue));
		// }
		// while(gt.hasNext()){ // these are the dudes that need an empty
		// if(emptyV == null){
		// emptyV = this.upsertVertex(addedValue, EMPTY);
		// }
		//
		// Vertex existingVert = gt.next();
		// if(forward){
		// this.upsertEdge(existingVert, colName, emptyV, addedType);
		// }
		// else {
		// this.upsertEdge(emptyV, addedType, existingVert, colName);
		// }
		// }
		// }
		// LOGGER.info("DONE inserting of blanks.......");
	}

	@Override
	public DataFrameTypeEnum getFrameType() {
		return DataFrameTypeEnum.GRAPH;
	}

	public void changeDataType(String columnName, String newType) {
		String typeName = columnName; // getValueForUniqueName(columnName);
		GraphTraversal<Vertex, Vertex> traversal = this.g.traversal().V().has(TinkerFrame.TINKER_TYPE, typeName);
		if (newType.equalsIgnoreCase("NUMBER")) {
			// convert the string to a number
			// if it cannot be cast to a number, make it into an empty node
			while (traversal.hasNext()) {
				Vertex v = traversal.next();
				String currName = v.value(TinkerFrame.TINKER_NAME);
				try {
					double numName = Double.parseDouble(currName);
					v.property(TinkerFrame.TINKER_NAME, numName);
				} catch (NumberFormatException ex) {
					// get the empty vertex, and do all the edge connections
					Vertex emptyVertex = upsertVertex(columnName, TinkerFrame.EMPTY);
					// first do the out connections
					Iterator<Edge> outEdges = v.edges(Direction.OUT);
					while (outEdges.hasNext()) {
						Edge e = outEdges.next();
						Vertex inVertex = e.inVertex();
						String[] label = e.label().split(EDGE_LABEL_DELIMETER_REGEX_SPLIT);
						upsertEdge(emptyVertex, label[0], inVertex, label[1]);
					}
					// now do the in connections
					Iterator<Edge> inEdges = v.edges(Direction.IN);
					while (inEdges.hasNext()) {
						Edge e = inEdges.next();
						Vertex outVertex = e.inVertex();
						String[] label = e.label().split(EDGE_LABEL_DELIMETER_REGEX_SPLIT);
						upsertEdge(outVertex, label[0], emptyVertex, label[1]);
					}
					// and now drop the vertex
					// that was an invalid type
					v.remove();
				}
			}
		} else if (newType.equalsIgnoreCase("STRING")) {
			// if converting to a string
			// just loop through and get the current value and make it into a string type
			while (traversal.hasNext()) {
				Vertex v = traversal.next();
				v.property(TinkerFrame.TINKER_NAME, v.value(TinkerFrame.TINKER_NAME) + "");
			}
		} else if (newType.equalsIgnoreCase("DATE")) {
			logger.info("TINKER FRAME DOES NOT SUPPORT DATE TYPES!!!");
			throw new IllegalArgumentException("Graphs do not support date as a data type!");
		}
	}

	@Override
	public IRawSelectWrapper query(String query) {
		return null;
	}

	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
		// qs.mergeRelations(flushRelationships(this.metaData.getAllRelationships()));
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		GremlinInterpreter interp = new GremlinInterpreter(this.g.traversal(), this.metaData);
		interp.setLogger(this.logger);
		interp.setQueryStruct(qs);
		logger.info("Generating Gremlin query...");
		RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interp, qs);
		gdi.execute();
		logger.info("Done generating query...");

		logger.info("Executing query...");
		QueryStructExpressionIterator qsd = new QueryStructExpressionIterator(gdi, qs);
		qsd.execute();
		logger.info("Done executing query");
		return qsd;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new GremlinInterpreter(this.g.traversal(), this.metaData);
	}

	private Map<String, Map<String, List>> flushRelationships(List<String[]> rels) {
		Map<String, Map<String, List>> relMap = new HashMap<>();
		// iterate through list
		// and make it into a map
		for (String[] rel : rels) {
			String start = rel[0];
			String end = rel[1];
			String relType = rel[2];

			Map<String, List> nodeComparatorMap = new HashMap<>();
			if (relMap.containsKey(start)) {
				nodeComparatorMap = relMap.get(start);
			} else {
				// add it to the map
				relMap.put(start, nodeComparatorMap);
			}

			List values = new ArrayList();
			if (nodeComparatorMap.containsKey(relType)) {
				values = nodeComparatorMap.get(relType);
			} else {
				nodeComparatorMap.put(relType, values);
			}

			values.add(end);
		}
		return relMap;
	}

	@Override
	public boolean isEmpty() {
		GraphTraversal<Vertex, Long> tv = g.traversal().V().count();
		if (tv.hasNext()) {
			Long count = tv.next();
			if (count > 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected Boolean calculateIsUnqiueColumn(String columnName) {
		// This reactor checks for duplicates
		boolean isUnique = false;

		// we need to know how to traverse
		List<String[]> tinkerRelationships = this.metaData.getAllRelationships();

		SelectQueryStruct qs1 = new SelectQueryStruct();
		qs1.setDistinct(false);
		qs1.mergeImplicitFilters(getFrameFilters());
		for (String[] rel : tinkerRelationships) {
			qs1.addRelation(rel[0], rel[1], rel[2]);

		}
		// as long as i have the correct joins
		// i can just query a count for any single column
		// and call it a day
		{
			QueryFunctionSelector countSelector = new QueryFunctionSelector();
			countSelector.setFunction(QueryFunctionHelper.COUNT);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if (columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			countSelector.addInnerSelector(innerSelector);
			qs1.addSelector(countSelector);
		}
		Iterator<IHeadersDataRow> nRowIt = query(qs1);
		long nRow = ((Number) nRowIt.next().getValues()[0]).longValue();

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs1.setDistinct(true);
		for (String[] rel : tinkerRelationships) {
			qs2.addRelation(rel[0], rel[1], rel[2]);
		}
		qs2.mergeImplicitFilters(getFrameFilters());
		{
			// TODO: WHY CAN'T I DO A UNIQUE COUNT!!! ???
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if (columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			qs2.addSelector(innerSelector);
		}

		Iterator<IHeadersDataRow> uniqueNRowIt = query(qs2);
		Set<String> uniqueSet = new HashSet<String>();
		while (uniqueNRowIt.hasNext()) {
			uniqueSet.add(Arrays.toString(uniqueNRowIt.next().getValues()));
		}
		long uniqueNRow = uniqueSet.size();

		isUnique = nRow == uniqueNRow;
		return isUnique;
	}

	@Override
	public void close() {
		super.close();
		this.g.clear();
		this.g.close();
	}
}
