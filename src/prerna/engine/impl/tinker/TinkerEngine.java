package prerna.engine.impl.tinker;

import java.io.IOException;
import java.util.Hashtable;
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
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.query.interpreters.GremlinInterpreter2;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class TinkerEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(BigDataEngine.class.getName());

	private Graph g = null;
	private boolean isNeo4j = false;

	public void openDB(String propFile) {
		super.openDB(propFile);

		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String fileName = prop.getProperty(Constants.TINKER_FILE);
		fileName = fileName.replace("@" + Constants.BASE_FOLDER + "@", baseFolder);
		fileName = fileName.replaceAll("@" + Constants.ENGINE + "@", this.engineName);
		LOGGER.info("Opening graph: " + fileName);
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.valueOf(prop.getProperty(Constants.TINKER_DRIVER));
		if (tinkerDriver == TINKER_DRIVER.NEO4J) {
			isNeo4j = true;
		}
		if (isNeo4j) {
			g = Neo4jGraph.open(fileName);
		} else {
			g = TinkerGraph.open();
			((TinkerGraph) g).createIndex(TinkerFrame.TINKER_TYPE, Vertex.class);
			((TinkerGraph) g).createIndex(TinkerFrame.TINKER_ID, Vertex.class);
			((TinkerGraph) g).createIndex(T.label.toString(), Edge.class);
			((TinkerGraph) g).createIndex(TinkerFrame.TINKER_ID, Edge.class);
			try {
				if (tinkerDriver == TINKER_DRIVER.TG) {
					// user kyro to de-serialize the cached graph
					Builder<GryoIo> builder = IoCore.gryo();
					builder.graph(this.g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GryoIo yes = builder.create();
					yes.readGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.JSON) {
					// user kyro to de-serialize the cached graph
					Builder<GraphSONIo> builder = IoCore.graphson();
					builder.graph(this.g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GraphSONIo yes = builder.create();
					yes.readGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.XML) {
					Builder<GraphMLIo> builder = IoCore.graphml();
					builder.graph(this.g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GraphMLIo yes = builder.create();
					yes.readGraph(fileName);
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
		return new TinkerQueryInterpreter(this);
	}
	
	@Override
	public IQueryInterpreter2 getQueryInterpreter2() {
		return new GremlinInterpreter2(this.g);
	}

	@Override
	public void commit() {
		try {
			long startTime = System.currentTimeMillis();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			TINKER_DRIVER tinkerDriver = TINKER_DRIVER.valueOf(prop.getProperty(Constants.TINKER_DRIVER));
			String fileName = prop.getProperty(Constants.TINKER_FILE);
			fileName = fileName.replace("@" + Constants.BASE_FOLDER + "@", baseFolder);
			fileName = fileName.replaceAll("@" + Constants.ENGINE + "@", this.engineName);
			if (!isNeo4j) {
				if (tinkerDriver == TINKER_DRIVER.TG) {
					Builder<GryoIo> builder = IoCore.gryo();
					builder.graph(g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GryoIo yes = builder.create();
					yes.writeGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.JSON) {
					Builder<GraphSONIo> builder = IoCore.graphson();
					builder.graph(g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GraphSONIo yes = builder.create();
					yes.writeGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.XML) {
					Builder<GraphMLIo> builder = IoCore.graphml();
					builder.graph(g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GraphMLIo yes = builder.create();
					yes.writeGraph(fileName);
				}

				long endTime = System.currentTimeMillis();
				LOGGER.info("Successfully saved TinkerFrame to file: " + fileName + "(" + (endTime - startTime) + " ms)");

			} else {
				g.tx().commit();
				long endTime = System.currentTimeMillis();
				LOGGER.info("Successfully saved Neo4jFrame to file: " + fileName + "(" + (endTime - startTime) + " ms)");
			}
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

		if (gt.hasNext()) {
			retVertex = gt.next();
		} else {
			retVertex = g.addVertex(TinkerFrame.TINKER_ID, type + ":" + data, TinkerFrame.TINKER_TYPE, type,
					TinkerFrame.TINKER_NAME, data);// push the actual value as
			// well who knows when you
			// would need it
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
		String edgeID = type + "/" + fromVertex.value(TinkerFrame.TINKER_NAME) + ":"
				+ toVertex.value(TinkerFrame.TINKER_NAME);

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
}
