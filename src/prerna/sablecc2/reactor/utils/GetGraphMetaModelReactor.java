package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
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
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), "type" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String fileName = this.keyValue.get(this.keysToGet[0]);
		String type = this.keyValue.get(this.keysToGet[1]);
		HashMap<String, Object> retMap = new HashMap<String, Object>();

		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.NEO4J;
		if (fileName.contains(".")) {
			String fileExtension = fileName.substring(fileName.indexOf(".") + 1);
			tinkerDriver = TINKER_DRIVER.valueOf(fileExtension.toUpperCase());
		}
		Graph g = null;

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
			Map<String, HashMap<String, String>> conceptProps = new HashMap<String, HashMap<String,String>>();
			Map<String, ArrayList<String>> edgeMap = new HashMap<>();

			if (g != null) {
				// get concepts and properties
				Iterator<Vertex> vert = g.vertices();
				while (vert.hasNext()) {
					Vertex v = vert.next();
					if (v.keys().contains(type)) {
						String concept = v.value(type);
						HashMap<String, String> propTypes = new HashMap<String, String>();
						Set<String> props = v.keys();
						for(String property: props) {
							// TODO get vertex property types
							String propertyType = "STRING";
							propTypes.put(property, propertyType);
						}
						
						conceptProps.put(concept, propTypes);
					}
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
				if (outVKeys.contains(type) && inVKeys.contains(type)) {
					String outVLabel = outV.value(type);
					String inVLabel = inV.value(type);
					if (!edgeMap.containsKey(edgeLabel)) {
						ArrayList<String> vertices = new ArrayList<>();
						vertices.add(outVLabel);
						vertices.add(inVLabel);
						edgeMap.put(edgeLabel, vertices);
					}
				}
			}
			
			retMap.put("vertices", conceptProps);
			retMap.put("edges", edgeMap);
		}


		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
