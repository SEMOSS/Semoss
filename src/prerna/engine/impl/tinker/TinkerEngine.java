package prerna.engine.impl.tinker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerQueryEngineInterpreter;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class TinkerEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(BigDataEngine.class.getName());

	private TinkerGraph g = null;
	private QueryStruct queryStruct;

	public void openDB(String propFile) {
		g = TinkerGraph.open();
		g.createIndex(TinkerFrame.TINKER_TYPE, Vertex.class);
		g.createIndex(TinkerFrame.TINKER_ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(TinkerFrame.TINKER_ID, Edge.class);
		try {
			super.openDB(propFile);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			// String fileName = baseFolder + "/" +
			// prop.getProperty(Constants.TINKER_FILE);
			String fileName = baseFolder + "/db/" + this.engineName + "/" + this.engineName + ".tg";

			// user kyro to de-serialize the cached graph
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(this.g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.readGraph(fileName);

		} catch (IOException e) {

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
		return new TinkerQueryEngineInterpreter(this);
	}

	@Override
	public void commit() {
		try {
			long startTime = System.currentTimeMillis();

			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			// String fileName = baseFolder + "/" +
			// prop.getProperty("tinker.file");
			String fileName = baseFolder + "/db/" + this.engineName + "/" + this.engineName + ".tg";

			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(g);
			IoRegistry kryo = new MyGraphIoRegistry();
			;
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.writeGraph(fileName);

			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved TinkerFrame to file: " + fileName + "(" + (endTime - startTime) + " ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Vector<Object> getCleanSelect(String query) {
		// TODO Auto-generated method stub
		return null;
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
		// String edgeID = type + "/" +
		// fromVertex.value(TinkerFrame.TINKER_NAME) + ":"
		// + toVertex.value(TinkerFrame.TINKER_NAME);

		retEdge = fromVertex.addEdge(type, toVertex);
		for (String key : propHash.keySet()) {
			retEdge.property(key, propHash.get(key));
		}

		// return retEdge;

		return retEdge;
	}

	public TinkerGraph getGraph() {
		return g;
	}

	public QueryStruct getQueryStruct() {
		return queryStruct;
	}

	public void setQueryStruct(QueryStruct queryStruct) {
		this.queryStruct = queryStruct;
	}

}
