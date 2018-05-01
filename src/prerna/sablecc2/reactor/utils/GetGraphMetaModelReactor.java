package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class GetGraphMetaModelReactor extends AbstractReactor {

	public GetGraphMetaModelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), "type" };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * Get Inputs
		 */
		organizeKeys();
		String fileName = this.keyValue.get(this.keysToGet[0]);
		String prop = this.keyValue.get(this.keysToGet[1]);

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
			File f = new File(fileName);
			if (f.exists() && f.isDirectory()) {
				g = Neo4jGraph.open(fileName);
			}
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

		}
		Map<String, ArrayList<String>> edgeMap = new HashMap<>();
		Map<String, Map<String, String>> nodes = new HashMap<>();

		/*
		 * Get Nodes and Edges
		 */
		if (g != null) {
			// get concepts and properties
			GraphTraversal<Vertex, Map<Object, Object>> gtTest = g.traversal().V().has(prop).group().by(__.values(prop));

			// get the types from the specified prop key
			Set<Object> types = null;
			while (gtTest.hasNext()) {
				Map<Object, Object> v = gtTest.next();
				types = v.keySet();
			}
			if (types != null) {
				for (Object t : types) {
					// get the properties for each type
					GraphTraversal<Vertex, String> x = g.traversal().V().has(prop, t).properties().key().dedup();
					Map<String, String> propMap = new HashMap<>();
					while (x.hasNext()) {
						String nodeProp = x.next();
						// determine data types
						GraphTraversal<Vertex, Object> testType = g.traversal().V().has(prop, t).has(nodeProp).values(nodeProp);
						int i = 0;
						int limit = 50;
						SemossDataType[] smssTypes = new SemossDataType[limit];
						// might need to default to string
						boolean isString = false;
						boolean next = true;
						while (testType.hasNext() && next) {
							Object value = testType.next();
							Object[] valueType = Utility.findTypes(value.toString());
							SemossDataType smssType = SemossDataType.convertStringToDataType(valueType[0].toString().toUpperCase());
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

			Iterator<Edge> edges = g.edges();
			while (edges.hasNext()) {
				Edge e = edges.next();
				String edgeLabel = e.label();
				Vertex outV = e.outVertex();
				Set<String> outVKeys = outV.keys();
				Vertex inV = e.inVertex();
				Set<String> inVKeys = inV.keys();
				if (outVKeys.contains(prop) && inVKeys.contains(prop)) {
					Object outVLabel = outV.value(prop);
					Object inVLabel = inV.value(prop);
					if (!edgeMap.containsKey(edgeLabel)) {
						ArrayList<String> vertices = new ArrayList<>();
						vertices.add(outVLabel.toString());
						vertices.add(inVLabel.toString());
						edgeMap.put(edgeLabel, vertices);
					}
				}
			}
		}
		if (tinkerDriver == TINKER_DRIVER.NEO4J) {
			try {
				g.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		if (g != null) {
			retMap.put("nodes", nodes);
			retMap.put("edges", edgeMap);
		}
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
