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
package prerna.reactor.masterdatabase.util;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.layout.Layout;
import org.graphstream.ui.layout.springbox.implementations.SpringBox;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.owl.ReadOnlyOWLEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Lays out metamodels for the metamodel page: the {@code top} and {@code left}
 * of each table, by name. A force layout spreads the tables out by their joins,
 * the tables are then separated so none overlap, and the result is scaled to
 * page positions. Each table's height follows its number of columns.
 */
public class GenerateMetamodelLayout {

	private static final Logger classLogger = LogManager.getLogger(GenerateMetamodelLayout.class);

	private static final Gson PRETTY_GSON = new GsonBuilder().setPrettyPrinting().create();

	/** How settled the force layout has to be before it stops */
	private static final double LAYOUT_STABILIZATION = 0.9;
	/** The most steps the force layout runs, however unsettled */
	private static final int MAX_LAYOUT_STEPS = 1_000;
	/** The longest the force layout runs, however unsettled */
	private static final long LAYOUT_TIME_LIMIT_MS = 5_000;

	/*
	 * Roughly the size a table is drawn at on the metamodel page, for placing new
	 * tables beside saved ones
	 */
	private static final double TABLE_WIDTH_PX = 320;
	private static final double TABLE_HEADER_PX = 50;
	private static final double TABLE_ROW_PX = 54;
	private static final double TABLE_GAP_PX = 60;

	/**
	 * Lays out a database's metamodel from its OWL file and writes the positions
	 * beside the OWL, as {@link AbstractDatabaseEngine#OWL_POSITION_FILENAME}. A
	 * failure is logged, and the positions are written only once the layout has
	 * finished.
	 *
	 * @param engineId   the database, for the OWL engine's name and the logs
	 * @param engineName the database's name, for reading its OWL
	 * @param owlF       the database's OWL file
	 */
	public static void generateLayout(String engineId, String engineName, File owlF) {
		Map<String, Map<String, Double>> positionMap = null;
		// the owl is stored as an RDF/XML file
		try (RDFFileSesameEngine rfse = new RDFFileSesameEngine()) {
			rfse.setBasic(true);
			Properties owlSmss = new Properties();
			owlSmss.put(Constants.RDF_FILE_PATH, owlF.getAbsolutePath());
			owlSmss.put(Constants.ENGINE, engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
			rfse.open(owlSmss);
			// we create the meta helper to facilitate querying the engine OWL
			ReadOnlyOWLEngine owlReader = new OWLEngineFactory(rfse, null, engineId, engineName).getReadOWL();

			long startTimer = System.currentTimeMillis();

			Graph graph = new MultiGraph(Utility.getRandomString(6));
			Layout layout = new SpringBox(false);
			graph.addSink(layout);
			layout.addAttributeSink(graph);

			List<String> nodes = owlReader.getPhysicalConcepts();
			Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

			for (String node : nodes) {
				String nodeName = Utility.getInstanceName(node);
				List<String> properties = owlReader.getPropertyUris4PhysicalUri(node);
				nodeSizes.put(nodeName, properties.size());
				graph.addNode(Utility.getInstanceName(node));
			}

			List<String[]> relationships = owlReader.getPhysicalRelationships();
			for (String[] relation : relationships) {
				String start = Utility.getInstanceName(relation[0]);
				String end = Utility.getInstanceName(relation[1]);
				String edge = Utility.getInstanceName(relation[2]) + "-" + start + "-" + end;
				graph.addEdge(edge, start, end);
			}

			computeLayout(layout);

			Map<String, Rectangle2D> rectangles = GenerateMetamodelLayout.getRectangles(graph, nodeSizes);
			Rectangles fixRectangles = new Rectangles();
			// get map of correctly spaced nodes/rectangles for metamodel
			Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
			positionMap = GenerateMetamodelLayout.generatePositionMap(graph, fixedRectangles);

			long endTimer = System.currentTimeMillis();
			classLogger.info("Laid out the metamodel of database {} in {} ms", Utility.cleanLogString(engineId),
					endTimer - startTimer);
		} catch (Exception e) {
			classLogger.error("Failed to lay out the metamodel of database {} from its OWL",
					Utility.cleanLogString(engineId), e);
		}
		if (positionMap == null) {
			return;
		}

		// write the positions beside the owl
		File positionFile = new File(owlF.getParentFile(), AbstractDatabaseEngine.OWL_POSITION_FILENAME);
		try (FileWriter writer = new FileWriter(positionFile)) {
			writer.write(PRETTY_GSON.toJson(positionMap));
		} catch (IOException e) {
			classLogger.error("Failed to write the metamodel positions of database {} to {}",
					Utility.cleanLogString(engineId), positionFile.getAbsolutePath(), e);
		}
	}

	// for owl metamodel reactor

	/**
	 * Lays out a metamodel given as concepts with their properties, and the joins
	 * between them.
	 *
	 * @param databaseTables each concept's properties, by concept name
	 * @param databaseJoins  each join, with its {@code source} and {@code target}
	 *                       concepts and its {@code rel} name
	 * @return the {@code top} and {@code left} of each concept, by name
	 */
	public static Map<String, Map<String, Double>> generateOWLMetamodelLayout(
			Map<String, Collection<String>> databaseTables, List<Map<String, String>> databaseJoins) {
		Rectangles fixRectangles = new Rectangles();
		Graph graph = addNodesToGraphOwl(databaseTables, databaseJoins);
		Map<String, Integer> nodeSizes = getNodeSizesOwl(databaseTables);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}

	// owl metamodel reactor helper functions

	/**
	 * Builds the graph of an OWL metamodel, a node for each concept and an edge for
	 * each join, and runs the force layout on it.
	 *
	 * @return the laid out graph
	 */
	private static Graph addNodesToGraphOwl(Map<String, Collection<String>> databaseTables,
			List<Map<String, String>> databaseJoins) {
		Graph graph = new MultiGraph(Utility.getRandomString(6));
		Layout layout = new SpringBox(false);
		graph.addSink(layout);
		layout.addAttributeSink(graph);

		databaseTables.forEach((conceptName, properties) -> {
			graph.addNode(conceptName);
		});

		for (Map<String, String> relations : databaseJoins) {
			String start = relations.get("source");
			String end = relations.get("target");
			String edge = relations.get("rel").toString() + "-" + start + "-" + end;
			graph.addEdge(edge, start, end);
		}

		computeLayout(layout);

		return graph;
	}

	/**
	 * The number of properties of each concept, which sets its height.
	 *
	 * @return the property count, by concept name
	 */
	private static Map<String, Integer> getNodeSizesOwl(Map<String, Collection<String>> databaseTables) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

		databaseTables.forEach((conceptName, properties) -> {
			nodeSizes.put(conceptName, properties.size());
		});

		return nodeSizes;
	}

	// for graph metamodel reactors

	/**
	 * Lays out the metamodel of a graph database.
	 *
	 * @param graphMap the {@code nodes}, each node's properties by name, and the
	 *                 {@code edges}, each edge's from and to node by name
	 * @return the {@code top} and {@code left} of each node, by name
	 */
	public static Map<String, Map<String, Double>> generateMetamodelLayoutForGraphDBs(Map<String, Object> graphMap) {
		Rectangles fixRectangles = new Rectangles();
		Map<String, Map<String, SemossDataType>> nodeMap = (Map<String, Map<String, SemossDataType>>) graphMap
				.get("nodes");
		Map<String, List<String>> relationshipMap = (Map<String, List<String>>) graphMap.get("edges");
		Graph graph = addNodesToGraph(nodeMap, relationshipMap);
		Map<String, Integer> nodeSizes = getNodeSizes(nodeMap);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}

	// graph metamodel reactors helper functions

	/**
	 * Builds the graph of a graph database's metamodel, a node for each node type
	 * and an edge for each edge type, and runs the force layout on it.
	 *
	 * @return the laid out graph
	 */
	private static Graph addNodesToGraph(Map<String, Map<String, SemossDataType>> nodeMap,
			Map<String, List<String>> relationshipMap) {
		Graph graph = new MultiGraph(Utility.getRandomString(6));
		Layout layout = new SpringBox(false);
		graph.addSink(layout);
		layout.addAttributeSink(graph);

		nodeMap.forEach((nodeName, properties) -> {
			graph.addNode(nodeName);
		});

		relationshipMap.forEach((edgeName, fromAndToTable) -> {
			String start = fromAndToTable.get(0);
			String end = fromAndToTable.get(1);
			String edge = edgeName + "-" + start + "-" + end;
			graph.addEdge(edge, start, end);
		});

		computeLayout(layout);

		return graph;
	}

	/**
	 * The number of properties of each node type, which sets its height.
	 *
	 * @return the property count, by node name
	 */
	private static Map<String, Integer> getNodeSizes(Map<String, Map<String, SemossDataType>> nodeMap) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

		nodeMap.forEach((nodeName, properties) -> {
			nodeSizes.put(nodeName, properties.size());
		});

		return nodeSizes;
	}

	// for external jdbc schema reactor

	/**
	 * Lays out every table of an external database's schema, read through JDBC.
	 *
	 * @param databaseTables each table, with its {@code table} name and
	 *                       {@code columns}
	 * @param databaseJoins  each foreign key, with its {@code fromTable},
	 *                       {@code fromCol}, {@code toTable} and {@code toCol}
	 * @return the {@code top} and {@code left} of each table, by name
	 */
	public static Map<String, Map<String, Double>> generateMetamodelLayoutForExternal(
			List<Map<String, Object>> databaseTables, List<Map<String, String>> databaseJoins) {
		Rectangles fixRectangles = new Rectangles();
		Graph graph = addNodesToGraph(databaseTables, databaseJoins);
		Map<String, Integer> nodeSizes = getNodeSizes(databaseTables);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}

	/**
	 * The positions of a database's tables after a schema sync. A table with a
	 * saved position keeps it, so the layout the user arranged stays as it is. When
	 * no table has one, every table is laid out; otherwise only the new tables are
	 * placed, in rows below the saved ones, and no layout runs.
	 *
	 * @param databaseTables each table, with its {@code table} name and
	 *                       {@code columns}
	 * @param databaseJoins  the joins between the tables
	 * @param savedPositions the saved {@code top} and {@code left} of each table,
	 *                       by name, or null for none
	 * @return the {@code top} and {@code left} of each table, by name
	 */
	public static Map<String, Map<String, Double>> generateMetamodelLayoutForExternal(
			List<Map<String, Object>> databaseTables, List<Map<String, String>> databaseJoins,
			Map<String, Object> savedPositions) {
		Map<String, Map<String, Double>> positions = new HashMap<>();
		List<Map<String, Object>> newTables = new ArrayList<>();
		double left = Double.MAX_VALUE;
		double bottom = 0;
		for (Map<String, Object> table : databaseTables) {
			String tableName = (String) table.get("table");
			Map<String, Double> saved = savedPosition(savedPositions == null ? null : savedPositions.get(tableName));
			if (saved == null) {
				newTables.add(table);
				continue;
			}
			positions.put(tableName, saved);
			left = Math.min(left, saved.get("left"));
			bottom = Math.max(bottom, saved.get("top") + tableHeight(table));
		}
		if (positions.isEmpty()) {
			return generateMetamodelLayoutForExternal(databaseTables, databaseJoins);
		}

		// new tables go in rows below the saved ones, in a roughly square grid
		int perRow = Math.max(1, (int) Math.ceil(Math.sqrt(newTables.size())));
		double x = left;
		double y = bottom + TABLE_GAP_PX;
		double rowHeight = 0;
		int inRow = 0;
		for (Map<String, Object> table : newTables) {
			if (inRow == perRow) {
				x = left;
				y += rowHeight + TABLE_GAP_PX;
				rowHeight = 0;
				inRow = 0;
			}
			Map<String, Double> position = new HashMap<>();
			position.put("top", y);
			position.put("left", x);
			positions.put((String) table.get("table"), position);
			rowHeight = Math.max(rowHeight, tableHeight(table));
			x += TABLE_WIDTH_PX + TABLE_GAP_PX;
			inRow++;
		}
		return positions;
	}

	/**
	 * Runs the force layout until it settles, or until it has run
	 * {@value #MAX_LAYOUT_STEPS} steps or {@value #LAYOUT_TIME_LIMIT_MS}
	 * milliseconds, so a large or restless graph cannot keep the request waiting. A
	 * layout stopped early is still usable, since the tables are separated
	 * afterwards.
	 *
	 * @param layout the layout the graph sends its nodes and edges to
	 */
	private static void computeLayout(Layout layout) {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LAYOUT_TIME_LIMIT_MS);
		int steps = 0;
		while (steps < MAX_LAYOUT_STEPS && layout.getStabilization() < LAYOUT_STABILIZATION) {
			layout.compute();
			steps++;
			if (System.nanoTime() - deadline >= 0) {
				break;
			}
		}
		if (layout.getStabilization() < LAYOUT_STABILIZATION) {
			classLogger.info("Stopped the metamodel layout after {} steps, before it settled", steps);
		}
	}

	/**
	 * A saved position, read from the map of {@code top} and {@code left} the page
	 * saves.
	 *
	 * @param value the saved entry for a table
	 * @return the position, or null when it is missing or not a pair of numbers
	 */
	private static Map<String, Double> savedPosition(Object value) {
		if (!(value instanceof Map<?, ?> saved)) {
			return null;
		}
		Double top = toDouble(saved.get("top"));
		Double left = toDouble(saved.get("left"));
		if (top == null || left == null) {
			return null;
		}
		Map<String, Double> position = new HashMap<>();
		position.put("top", top);
		position.put("left", left);
		return position;
	}

	/**
	 * A saved coordinate as a number. The local master database returns numbers,
	 * and a cached map can hold them as text.
	 *
	 * @param value the saved coordinate
	 * @return the number, or null when it is missing or not a number
	 */
	private static Double toDouble(Object value) {
		if (value instanceof Number number) {
			return number.doubleValue();
		}
		if (value instanceof String text) {
			try {
				return Double.parseDouble(text.trim());
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return null;
	}

	/**
	 * Roughly how tall a table is drawn on the metamodel page: its header and a row
	 * for each column.
	 *
	 * @param table the table, with its {@code columns}
	 * @return the height, in page pixels
	 */
	private static double tableHeight(Map<String, Object> table) {
		Object columns = table.get("columns");
		int columnCount = columns instanceof List<?> list ? list.size() : 0;
		return TABLE_HEADER_PX + TABLE_ROW_PX * columnCount;
	}

	// external jdbc schema reactor helper functions

	/**
	 * Builds the graph of an external database's schema, a node for each table and
	 * an edge for each foreign key, and runs the force layout on it. A table or a
	 * foreign key listed twice is laid out once.
	 *
	 * @return the laid out graph
	 */
	private static Graph addNodesToGraph(List<Map<String, Object>> databaseTables,
			List<Map<String, String>> databaseJoins) {
		Graph graph = new MultiGraph(Utility.getRandomString(6));
		Layout layout = new SpringBox(false);
		graph.addSink(layout);
		layout.addAttributeSink(graph);

		for (Map<String, Object> nodes : databaseTables) {
			String tableName = (String) nodes.get("table");
			try {
				graph.addNode(tableName);
			} catch (org.graphstream.graph.IdAlreadyInUseException e) {
				classLogger.warn("Table {} is listed more than once, so the metamodel layout places it once",
						Utility.cleanLogString(tableName));
			}
		}

		for (Map<String, String> relations : databaseJoins) {
			String start = relations.get("fromTable");
			String end = relations.get("toTable");
			String edge = relations.get("fromCol") + "_" + relations.get("toCol") + "-" + start + "-" + end;
			try {
				graph.addEdge(edge, start, end);
			} catch (org.graphstream.graph.IdAlreadyInUseException e) {
				classLogger.warn("Foreign key {} is listed more than once, so the metamodel layout draws it once",
						Utility.cleanLogString(edge));
			}
		}

		computeLayout(layout);

		return graph;
	}

	/**
	 * The number of columns of each table, which sets its height.
	 *
	 * @return the column count, by table name, for each table whose columns were
	 *         read
	 */
	private static Map<String, Integer> getNodeSizes(List<Map<String, Object>> databaseTables) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

		for (Map<String, Object> nodes : databaseTables) {
			List<String> columns = (List) nodes.get("columns");
			String tableName = (String) nodes.get("table");
			// a table whose columns could not be read has no size
			if (columns != null) {
				nodeSizes.put(tableName, columns.size());
			}
		}

		return nodeSizes;
	}

	// for predict metamodel reactor

	/**
	 * Lays out a predicted metamodel: tables with their properties, and the joins
	 * predicted between them.
	 *
	 * @param nodePropMap     each table's properties, by table name
	 * @param relationMapList each predicted join, with its {@code fromTable},
	 *                        {@code toTable} and {@code relName}
	 * @return the {@code top} and {@code left} of each table, by name
	 */
	public static Map<String, Map<String, Double>> generateMetamodelPredictionLayout(
			Map<String, List<String>> nodePropMap, List<Map<String, Object>> relationMapList) {
		Rectangles fixRectangles = new Rectangles();
		Graph graph = addNodesToGraph(nodePropMap, relationMapList);
		Map<String, Integer> nodeSizes = getNodeSizes(nodePropMap, relationMapList);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}

	// predict metamodel reactor helper functions

	/**
	 * Builds the graph of a predicted metamodel, a node for each table in a join
	 * and an edge for each join, and runs the force layout on it.
	 *
	 * @return the laid out graph
	 */
	private static Graph addNodesToGraph(Map<String, List<String>> nodePropMap,
			List<Map<String, Object>> relationMapList) {
		Graph graph = new MultiGraph(Utility.getRandomString(6));
		Layout layout = new SpringBox(false);
		graph.addSink(layout);
		layout.addAttributeSink(graph);

		for (Map<String, Object> relations : relationMapList) {
			String nodeToInsert = relations.get("fromTable").toString();
			Node nodeExists = graph.getNode(nodeToInsert);
			if (nodeExists == null) {
				graph.addNode(nodeToInsert);
			}
			nodeToInsert = relations.get("toTable").toString();
			nodeExists = graph.getNode(nodeToInsert);
			if (nodeExists == null) {
				graph.addNode(nodeToInsert);
			}
		}

		for (Map<String, Object> relations : relationMapList) {
			String start = relations.get("fromTable").toString();
			String end = relations.get("toTable").toString();
			String edge = relations.get("relName").toString() + "-" + start + "-" + end;
			graph.addEdge(edge, start, end);
		}

		computeLayout(layout);

		return graph;
	}

	/**
	 * The number of properties of each table, which sets its height. A table that
	 * appears only in a join has none.
	 *
	 * @return the property count, by table name
	 */
	private static Map<String, Integer> getNodeSizes(Map<String, List<String>> nodePropMap,
			List<Map<String, Object>> relationMapList) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

		nodePropMap.forEach((nodeName, properties) -> {
			nodeSizes.put(nodeName, properties.size());
		});

		// go through relations map and any node without properties add to nodeSizes map
		for (Map<String, Object> relations : relationMapList) {
			String start = relations.get("fromTable").toString();
			String end = relations.get("toTable").toString();
			if (!nodeSizes.containsKey(start)) {
				nodeSizes.put(start, 0);
			}
			if (!nodeSizes.containsKey(end)) {
				nodeSizes.put(end, 0);
			}
		}

		return nodeSizes;
	}

	/**
	 * The box of each node at its laid out position, in layout units: 160 wide, and
	 * 20 tall, or 10 per property for a node with more than one property.
	 *
	 * @param graph     the laid out graph
	 * @param nodeSizes the property count of each node, by name; a node not in it
	 *                  has none
	 * @return the box of each node, by name
	 */
	private static Map<String, Rectangle2D> getRectangles(Graph graph, Map<String, Integer> nodeSizes) {
		Map<String, Rectangle2D> rectangles = new HashMap<>();
		Iterator<Node> nodeIt = graph.iterator();

		while (nodeIt.hasNext()) {
			Node node = nodeIt.next();
			String nodeName = node.getId();
			float nodeSize = 0;
			if (nodeSizes.containsKey(nodeName)) {
				nodeSize = nodeSizes.get(nodeName);
			}

			double pos[] = Toolkit.nodePosition(node);

			float heightMultiplier = 10;
			if (nodeSize > 1) {
				heightMultiplier = 5 * nodeSize;
			}

			// Float(x, y, w, h)
			rectangles.put(nodeName, new Rectangle2D.Float((float) pos[0], (float) pos[1], 160, 2 * heightMultiplier));
		}

		return rectangles;
	}

	/**
	 * Turns the separated boxes into page positions: shifted so none is negative,
	 * then scaled up, by 6 for fewer than 10 nodes, 4 for 11 to 24, and 3 for any
	 * other count. The graph is cleared afterwards.
	 *
	 * @param graph           the laid out graph
	 * @param fixedRectangles the separated box of each node, by name
	 * @return the {@code top} and {@code left} of each node, by name
	 */
	private static Map<String, Map<String, Double>> generatePositionMap(Graph graph,
			Map<String, Rectangle2D> fixedRectangles) {
		Map<String, Map<String, Double>> positionMap = new HashMap<>();

		double minx = 0;
		double miny = 0;

		for (Entry<String, Rectangle2D> fixedRectEntrySet : fixedRectangles.entrySet()) {
			Rectangle2D rectangle = fixedRectEntrySet.getValue();
			double x = rectangle.getX();
			double y = rectangle.getY();

			if (x < minx) {
				minx = x;
			}
			if (y < miny) {
				miny = y;
			}
		}

		Iterator<Node> nodeIt = graph.iterator();
		int numberOfNodes = graph.getNodeCount();
		double leftM;
		double topM;

		if (numberOfNodes < 10) {
			leftM = 6;
			topM = 6;
		} else if (numberOfNodes > 10 && numberOfNodes < 25) {
			leftM = 4;
			topM = 4;
		} else {
			leftM = 3;
			topM = 3;
		}

		while (nodeIt.hasNext()) {
			Node node = nodeIt.next();
			String nodeName = node.getId();
			Rectangle2D nodePosition = fixedRectangles.get(nodeName);
			double upperLeftX = nodePosition.getX();
			double upperLeftY = nodePosition.getY();
			Map<String, Double> posMap = new HashMap<>();

			if (minx < 0) {
				posMap.put("left", leftM * (upperLeftX + (-1 * minx)));
			} else {
				posMap.put("left", leftM * upperLeftX);
			}

			if (miny < 0) {
				posMap.put("top", topM * (upperLeftY + (-1 * miny)));
			} else {
				posMap.put("top", topM * upperLeftY);
			}

			positionMap.put(nodeName, posMap);
		}

		// clean up the graph
		graph.clearSinks();
		graph.clear();

		return positionMap;
	}

}
