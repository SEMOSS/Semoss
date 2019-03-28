package prerna.engine.impl.tinker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.MyGraphIoMappingBuilder;
import prerna.util.Utility;

public class TinkerEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(TinkerEngine.class.getName());

	private Graph g = null;
	private Map<String, String> typeMap = new HashMap<String, String>();
	private Map<String, String> nameMap = new HashMap<String, String>();
	
	public void openDB(String propFile) {
		super.openDB(propFile);
		String fileLocation = SmssUtilities.getTinkerFile(prop).getAbsolutePath();
		LOGGER.info("Opening graph:  " + fileLocation);
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.valueOf(prop.getProperty(Constants.TINKER_DRIVER));
		
		// get type map
		String typeMapStr = this.prop.getProperty("TYPE_MAP");
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// get the name map
		String nameMapStr = this.prop.getProperty("NAME_MAP");
		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
			try {
				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (tinkerDriver == TINKER_DRIVER.NEO4J) {
			g = Neo4jGraph.open(fileLocation);
		} else {
			g = TinkerGraph.open();
			// create index for default semoss types
			if (this.typeMap != null) {
				((TinkerGraph) g).createIndex(TinkerFrame.TINKER_TYPE, Vertex.class);
				((TinkerGraph) g).createIndex(TinkerFrame.TINKER_ID, Vertex.class);
				((TinkerGraph) g).createIndex(T.label.toString(), Edge.class);
				((TinkerGraph) g).createIndex(TinkerFrame.TINKER_ID, Edge.class);
			}
			try {
				if (tinkerDriver == TINKER_DRIVER.TG) {
					// user kyro to de-serialize the cached graph
					Builder<GryoIo> builder = GryoIo.build();
					builder.graph(g);
					builder.onMapper(new MyGraphIoMappingBuilder());
					GryoIo reader = builder.create();
					reader.readGraph(fileLocation);
				} else if (tinkerDriver == TINKER_DRIVER.JSON) {
					// user kyro to de-serialize the cached graph
					Builder<GraphSONIo> builder = GraphSONIo.build();
					builder.graph(g);
					builder.onMapper(new MyGraphIoMappingBuilder());
					GraphSONIo reader = builder.create();
					reader.readGraph(fileLocation);
				} else if (tinkerDriver == TINKER_DRIVER.XML) {
					Builder<GraphMLIo> builder = GraphMLIo.build();
					builder.graph(g);
					builder.onMapper(new MyGraphIoMappingBuilder());
					GraphMLIo reader = builder.create();
					reader.readGraph(fileLocation);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.TINKER;
	}

	@Override
	public Vector<Object> getEntityOfType(String typeUri) {
		Vector<Object> columnsFromResult = new Vector<Object>();
		String conceptType = Utility.getInstanceName(typeUri);
		GraphTraversal<Vertex, Object> vertIt = null;
		// case for property
		if (typeUri.contains("http://semoss.org/ontologies/Relation/Contains")) {
			String propName = Utility.getClassName(typeUri);
			vertIt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, conceptType).values(propName);
		}
		// case for concept
		else {
			vertIt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, conceptType).values(TinkerFrame.TINKER_NAME);
		}
		while (vertIt.hasNext()) {
			columnsFromResult.addElement(vertIt.next());
		}
		return columnsFromResult;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub

	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new GremlinInterpreter(this.g.traversal(), this.typeMap, this.nameMap);
	}

	@Override
	public void commit() {
		try {
			long startTime = System.currentTimeMillis();
			TINKER_DRIVER tinkerDriver = TINKER_DRIVER.valueOf(prop.getProperty(Constants.TINKER_DRIVER));
			String fileLocation = SmssUtilities.getTinkerFile(prop).getAbsolutePath();
			if (tinkerDriver == TINKER_DRIVER.TG) {
				// user kyro to de-serialize the cached graph
				Builder<GryoIo> builder = GryoIo.build();
				builder.graph(g);
				builder.onMapper(new MyGraphIoMappingBuilder());
				GryoIo writer = builder.create();
				writer.writeGraph(fileLocation);
			} else if (tinkerDriver == TINKER_DRIVER.JSON) {
				// user kyro to de-serialize the cached graph
				Builder<GraphSONIo> builder = GraphSONIo.build();
				builder.graph(g);
				builder.onMapper(new MyGraphIoMappingBuilder());
				GraphSONIo  writer = builder.create();
				writer.writeGraph(fileLocation);
			} else if (tinkerDriver == TINKER_DRIVER.XML) {
				Builder<GraphMLIo> builder = GraphMLIo.build();
				builder.graph(g);
				builder.onMapper(new MyGraphIoMappingBuilder());
				GraphMLIo  writer = builder.create();
				writer.writeGraph(fileLocation);
			} else if (tinkerDriver == TINKER_DRIVER.NEO4J) {
				g.tx().commit();
			}
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved graph to file: " + fileLocation + "(" + (endTime - startTime) + " ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Generate the tinker vertex for a specific instance
	 * 
	 * @param type
	 *            The type of the vertex
	 * @param data
	 *            The instance value
	 * @return
	 */
	public Vertex upsertVertex(Object[] args) {
		String type = args[0] + "";
		Object data = args[1];

		Vertex retVertex = null;
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_ID, type + ":" + data);
		if(!this.typeMap.keySet().contains(type)) {
			this.typeMap.put(type, TinkerFrame.TINKER_TYPE);
			this.nameMap.put(type, TinkerFrame.TINKER_NAME);
		}
		if (gt.hasNext()) {
			retVertex = gt.next();
		} else {
			retVertex = g.addVertex(T.label, type, TinkerFrame.TINKER_ID, type + ":" + data, TinkerFrame.TINKER_TYPE, type, TinkerFrame.TINKER_NAME, data);
		}
		return retVertex;
	}

	/**
	 * Generate the tinker edge for a specific instance
	 * 
	 * @param type
	 *            The type of the vertex
	 * @param data
	 *            The instance value
	 * @return
	 */
	public Edge upsertEdge(Object[] args) {
		Vertex fromVertex = (Vertex) args[0];
		String fromVertexUniqueName = (String) args[1];
		Vertex toVertex = (Vertex) args[2];
		String toVertexUniqueName = (String) args[3];
		Edge retEdge = null;
		Hashtable<String, Object> propHash = (Hashtable<String, Object>) args[4];

		String type = fromVertexUniqueName + TinkerFrame.EDGE_LABEL_DELIMETER + toVertexUniqueName;
		String edgeID = type + "/" + fromVertex.value(TinkerFrame.TINKER_NAME) + ":" + toVertex.value(TinkerFrame.TINKER_NAME);

		retEdge = fromVertex.addEdge(type, toVertex);
		retEdge.property(TinkerFrame.TINKER_ID, edgeID);
		Set<String> edgeProp = retEdge.keys();
		for (String key : propHash.keySet()) {
			if (!edgeProp.contains(key)) {
				retEdge.property(key, propHash.get(key));
			}
		}

		return retEdge;
	}

	public Graph getGraph() {
		return g;
	}
	
	public Map<String, String> getTypeMap() {
		return this.typeMap;
	}
	
	public Map<String, String> getNameMap() {
		return this.nameMap;
	}
}
