package prerna.engine.impl.tinker;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.query.interpreters.GremlinNoEdgeBindInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.MyGraphIoMappingBuilder;
import prerna.util.Utility;

public class TinkerEngine extends AbstractDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(TinkerEngine.class);

	protected Graph g = null;
	protected Map<String, String> typeMap = new HashMap<>();
	protected Map<String, String> nameMap = new HashMap<>();
	protected boolean useLabel = false;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// get type map
		String typeMapStr = this.smssProp.getProperty("TYPE_MAP");
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		// get the name map
		String nameMapStr = this.smssProp.getProperty("NAME_MAP");
		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
			try {
				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		if (smssProp.containsKey(Constants.TINKER_USE_LABEL)) {
			String booleanStr = smssProp.get(Constants.TINKER_USE_LABEL).toString();
			useLabel = Boolean.parseBoolean(booleanStr);
		}

		// open normal tinker engine
		String fileLocation = SmssUtilities.getTinkerFile(smssProp).getAbsolutePath();
		File fileL = new File(fileLocation);
		classLogger.info("Opening graph:  " + Utility.cleanLogString(fileLocation));
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.valueOf(smssProp.getProperty(Constants.TINKER_DRIVER));
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
			if(!fileL.exists() || !fileL.isFile()) {
				classLogger.info(SmssUtilities.getUniqueName(this.engineName, this.engineId) + " is an empty Tinker Engine");
			} else {
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
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.TINKER;
	}

	@Override
	public Vector<Object> getEntityOfType(String typeUri) {
		Vector<Object> columnsFromResult = new Vector<>();
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
		GremlinNoEdgeBindInterpreter interp = new GremlinNoEdgeBindInterpreter(this.g.traversal(), this.typeMap, this.nameMap, this);
		interp.setUseLabel(useLabel);
		return interp;
	}

	@Override
	public void commit() {
		try {
			long startTime = System.currentTimeMillis();
			TINKER_DRIVER tinkerDriver = TINKER_DRIVER.valueOf(smssProp.getProperty(Constants.TINKER_DRIVER));
			String fileLocation = SmssUtilities.getTinkerFile(smssProp).getAbsolutePath();
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
			classLogger.info("Successfully saved graph to file: " + Utility.normalizePath(fileLocation) + "(" + (endTime - startTime) + " ms)");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		try {
			this.g.close();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IOException("Error occurred closing the Graph", e);
		}
	}
	
	@Override
	public boolean holdsFileLocks() {
		return true;
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
	
	/**
	 * Set this to query the graph using the label() method
	 * @param useLabel
	 */
	public void setUseLabel(boolean useLabel) {
		this.useLabel = useLabel;
	}
}
