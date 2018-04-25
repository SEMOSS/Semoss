package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.MyGraphIoRegistry;

public class GetGraphMetaModelReactor extends AbstractReactor {

	public GetGraphMetaModelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * Get Inputs
		 */
		organizeKeys();
		String fileName = this.keyValue.get(this.keysToGet[0]);
		HashMap<String, Object> retMap = new HashMap<String, Object>();
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.NEO4J;
		if (fileName.contains(".")) {
			String fileExtension = fileName.substring(fileName.indexOf(".") + 1);
			tinkerDriver = TINKER_DRIVER.valueOf(fileExtension.toUpperCase());
		}
		Graph g = null;
		/*
		 * Open Graph
		 */
		if (tinkerDriver == TINKER_DRIVER.NEO4J) {
			g = Neo4jGraph.open(fileName);
		} else {
			g = TinkerGraph.open();
			try {
				if (tinkerDriver == TINKER_DRIVER.TG) {
					// user kyro to de-serialize the cached graph
					Builder<GryoIo> builder = IoCore.gryo();
					builder.graph(g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GryoIo yes = builder.create();
					yes.readGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.JSON) {
					// user kyro to de-serialize the cached graph
					Builder<GraphSONIo> builder = IoCore.graphson();
					builder.graph(g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GraphSONIo yes = builder.create();
					yes.readGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.XML) {
					Builder<GraphMLIo> builder = IoCore.graphml();
					builder.graph(g);
					IoRegistry kryo = new MyGraphIoRegistry();
					builder.registry(kryo);
					GraphMLIo yes = builder.create();
					yes.readGraph(fileName);
				} else {

				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			Map<String, ArrayList<String>> edgeMap = new HashMap<>();
			Map<String, HashMap<String, Map<String, Object>>> nodes = new HashMap<>();
			
			/*
			 * Get Nodes and Edges
			 */
			if (g != null) {
				// get concepts and properties
				GraphTraversal gtTest = g.traversal().V().label().dedup();

				while (gtTest.hasNext()) {
					String vLabel = (String) gtTest.next();
					GraphTraversal propTraversal = g.traversal().V().hasLabel(vLabel).valueMap();
					HashMap<String, Map<String, Object>> propMap = new HashMap<>();
					int i = 0;
					int limit = 25;
					boolean next = true;
					while (propTraversal.hasNext() && next) {
						Map<Object, Object> propsList = (Map<Object, Object>) propTraversal.next();

						for (Object key : propsList.keySet()) {
							Map<String, Object> propHash = new HashMap<>();
							// TODO get type of property
							Object value = propsList.get(key);
							String propType = "String";
							propHash.put("type", propType);
							propMap.put((String) key, propHash);
						}
						i++;
						if (i <= limit) {
							if (!propTraversal.hasNext()) {
								next = false;
							}
						}
						if (i == limit) {
							next = false;

						}
					}
					nodes.put(vLabel, propMap);
				}

				Iterator<Edge> edges = g.edges();
				while (edges.hasNext()) {
					Edge e = edges.next();
					String edgeLabel = e.label();
					Vertex outV = e.outVertex();
					Vertex inV = e.inVertex();
					String outVLabel = outV.label();
					String inVLabel = inV.label();
					if (!edgeMap.containsKey(edgeLabel)) {
						ArrayList<String> vertices = new ArrayList<>();
						vertices.add(outVLabel);
						vertices.add(inVLabel);
						// check if edge nodes is added to node map
						if (nodes.keySet().contains(outVLabel) && nodes.keySet().contains(inVLabel)) {
							edgeMap.put(edgeLabel, vertices);
						}
					}
				}
			}
			retMap.put("nodes", nodes);
			retMap.put("edges", edgeMap);
		}

		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
