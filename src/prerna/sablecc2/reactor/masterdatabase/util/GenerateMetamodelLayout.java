package prerna.sablecc2.reactor.masterdatabase.util;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.layout.Layout;
import org.graphstream.ui.layout.springbox.implementations.SpringBox;

import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateMetamodelLayout {
	
	private static final Logger classLogger = LogManager.getLogger(GenerateMetamodelLayout.class);
	
	public static void generateLayout(String engineId) {
		String smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);
		String owlFileLocation = SmssUtilities.getOwlFile(prop).getAbsolutePath();
		File owlF = new File(owlFileLocation);

		// owl is stored as RDF/XML file
		RDFFileSesameEngine rfse = null;
		try {
			rfse = new RDFFileSesameEngine();
			rfse.openFile(owlFileLocation, null, null);
            rfse.setEngineId(engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
			// we create the meta helper to facilitate querying the engine OWL
			MetaHelper helper = new MetaHelper(rfse, null, null);
	
			long startTimer = System.currentTimeMillis();
	
			Graph graph = new MultiGraph(Utility.getRandomString(6));
			Layout layout = new SpringBox(false);
			graph.addSink(layout);
			layout.addAttributeSink(graph);
	
			List<String> nodes = helper.getPhysicalConcepts();
			Map<String, Integer> nodeSizes = new HashMap<String, Integer>();
	
			for (String node : nodes) {
				String nodeName = Utility.getInstanceName(node);
				List<String> properties = helper.getPropertyUris4PhysicalUri(node);
				nodeSizes.put(nodeName, properties.size());
				graph.addNode(Utility.getInstanceName(node));
			}
	
			List<String[]> relationships = helper.getPhysicalRelationships();
			for (String[] relation : relationships) {
				String start = Utility.getInstanceName(relation[0]);
				String end = Utility.getInstanceName(relation[1]);
				String edge = Utility.getInstanceName(relation[2]) + "-" + start + "-" + end;
				graph.addEdge(edge, start, end);
			}
	
			while (layout.getStabilization() < 0.9) {
				layout.compute();
			}
	
			Map<String, Rectangle2D> rectangles = GenerateMetamodelLayout.getRectangles(graph, nodeSizes);
			Rectangles fixRectangles = new Rectangles();
			// get map of correctly spaced nodes/rectangles for metamodel
			Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
			Map<String, Map<String, Double>> positionMap = GenerateMetamodelLayout.generatePositionMap(graph, fixedRectangles);
	
			long endTimer = System.currentTimeMillis();
			System.out.println("Compute time = " + (endTimer - startTimer) + " ms");
	
			// now write the file
			String baseFolder = owlF.getParent();
			String positionJson = baseFolder + "/" + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
			FileWriter writer = null;
			try {
				writer = new FileWriter(positionJson);
				writer.write(new GsonBuilder().setPrettyPrinting().create().toJson(positionMap));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(writer != null) {
					try {
						writer.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		} finally {
			if(rfse != null) {
				try {
					rfse.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/////////////////////////////////////////// for owl metamodel reactor ///////////////////////////////////////////

	public static Map<String, Map<String, Double>> generateOWLMetamodelLayout(Map<String, Collection<String>> databaseTables, List<Map<String, String>> databaseJoins) {
		Rectangles fixRectangles = new Rectangles();
		Graph graph = addNodesToGraphOwl(databaseTables, databaseJoins);
		Map<String, Integer> nodeSizes = getNodeSizesOwl(databaseTables);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}
	
	/////////////////////////////////////////// owl metamodel reactor helper functions ///////////////////////////////////////////

	private static Graph addNodesToGraphOwl(Map<String, Collection<String>> databaseTables, List<Map<String, String>> databaseJoins) {
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

		while (layout.getStabilization() < 0.9) {
			layout.compute();
		}

		return graph;
	}

	private static Map<String, Integer> getNodeSizesOwl(Map<String, Collection<String>> databaseTables) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();
		
		databaseTables.forEach((conceptName, properties) -> {
			nodeSizes.put(conceptName, properties.size());
		});

		return nodeSizes;
	}

	/////////////////////////////////////////// for graph metamodel reactors ///////////////////////////////////////////

	public static Map<String, Map<String, Double>> generateMetamodelLayoutForGraphDBs(Map<String, Object> graphMap) {
		Rectangles fixRectangles = new Rectangles();
		Map<String, Map<String, SemossDataType>> nodeMap = (Map<String, Map<String, SemossDataType>>) graphMap.get("nodes");
		Map<String, List<String>> relationshipMap = (Map<String, List<String>>) graphMap.get("edges");
		Graph graph = addNodesToGraph(nodeMap, relationshipMap);
		Map<String, Integer> nodeSizes = getNodeSizes(nodeMap);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);
		
		return nodePositionMap;
	}

	/////////////////////////////////////////// graph metamodel reactors helper functions ///////////////////////////////////////////

	private static Graph addNodesToGraph(Map<String, Map<String, SemossDataType>> nodeMap, Map<String, List<String>> relationshipMap) {
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

		while (layout.getStabilization() < 0.9) {
			layout.compute();
		}

		return graph;
	}

	private static Map<String, Integer> getNodeSizes(Map<String, Map<String, SemossDataType>> nodeMap) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

		nodeMap.forEach((nodeName, properties) -> {
			nodeSizes.put(nodeName, properties.size());
		});

		return nodeSizes;
	}

	/////////////////////////////////////////// for external jdbc schema reactor ///////////////////////////////////////////

	public static Map<String, Map<String, Double>> generateMetamodelLayoutForExternal(List<Map<String, Object>> databaseTables, List<Map<String, String>> databaseJoins) {
		Rectangles fixRectangles = new Rectangles();
		Graph graph = addNodesToGraph(databaseTables, databaseJoins);
		Map<String, Integer> nodeSizes = getNodeSizes(databaseTables);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}

	/////////////////////////////////////////// external jdbc schema reactor helper functions ///////////////////////////////////////////

	private static Graph addNodesToGraph(List<Map<String, Object>> databaseTables, List<Map<String, String>> databaseJoins) {
		Graph graph = new MultiGraph(Utility.getRandomString(6));
		Layout layout = new SpringBox(false);
		graph.addSink(layout);
		layout.addAttributeSink(graph);

		for (Map<String, Object> nodes : databaseTables) {
			String tableName = (String) nodes.get("table");
			try {
				graph.addNode(tableName);
			} catch(org.graphstream.graph.IdAlreadyInUseException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		for (Map<String, String> relations : databaseJoins) {
			String start = relations.get("fromTable");
			String end = relations.get("toTable");
			String edge = relations.get("fromCol") + "_" + relations.get("toCol") + "-" + start + "-" + end;
			try {
				graph.addEdge(edge, start, end);
			} catch(org.graphstream.graph.IdAlreadyInUseException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		while (layout.getStabilization() < 0.9) {
			layout.compute();
		}

		return graph;
	}
	
	private static Map<String, Integer> getNodeSizes(List<Map<String, Object>> databaseTables) {
		Map<String, Integer> nodeSizes = new HashMap<String, Integer>();

		for (Map<String, Object> nodes: databaseTables) {
			List<String> columns = (List) nodes.get("columns");
			String tableName = (String) nodes.get("table");
			// if no columns, just why....
			if(columns != null) {
				nodeSizes.put(tableName, columns.size());
			}
		}

		return nodeSizes;
	}
	
	/////////////////////////////////////////// for predict metamodel reactor ///////////////////////////////////////////

	public static Map<String, Map<String, Double>> generateMetamodelPredictionLayout(Map<String, List<String>> nodePropMap, List<Map<String, Object>> relationMapList) {
		Rectangles fixRectangles = new Rectangles();
		Graph graph = addNodesToGraph(nodePropMap, relationMapList);
		Map<String, Integer> nodeSizes = getNodeSizes(nodePropMap, relationMapList);
		Map<String, Rectangle2D> rectangles = getRectangles(graph, nodeSizes);
		Map<String, Rectangle2D> fixedRectangles = fixRectangles.fix(rectangles);
		Map<String, Map<String, Double>> nodePositionMap = generatePositionMap(graph, fixedRectangles);

		return nodePositionMap;
	}

	/////////////////////////////////////////// predict metamodel reactor helper functions ///////////////////////////////////////////

	private static Graph addNodesToGraph(Map<String, List<String>> nodePropMap, List<Map<String, Object>> relationMapList) {
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

		while (layout.getStabilization() < 0.9) {
			layout.compute();
		}

		return graph;
	}

	private static Map<String, Integer> getNodeSizes(Map<String, List<String>> nodePropMap, List<Map<String, Object>> relationMapList) {
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
				heightMultiplier = 5*nodeSize;
			}

			// Float(x, y, w, h)
			rectangles.put(nodeName, new Rectangle2D.Float((float) pos[0], (float) pos[1], 160, 2*heightMultiplier));
		}
		
		return rectangles;
	}
	
	private static Map<String, Map<String, Double>> generatePositionMap(Graph graph, Map<String, Rectangle2D> fixedRectangles) {
		Map<String, Map<String, Double>> positionMap = new HashMap<>();
		
		double minx = 0;
		double miny = 0;

		for (Entry<String, Rectangle2D> fixedRectEntrySet: fixedRectangles.entrySet()) {
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
	
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//
//		String tapCoreSmss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data__133db94b-4371-4763-bff9-edf7e5ed021b.smss";
//		BigDataEngine engine = new BigDataEngine();
//		engine.open(tapCoreSmss);
//
//		generateLayout("133db94b-4371-4763-bff9-edf7e5ed021b");
//	}
}
