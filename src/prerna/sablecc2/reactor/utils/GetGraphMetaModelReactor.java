package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
import prerna.util.GraphUtility;
import prerna.util.MyGraphIoRegistry;

public class GetGraphMetaModelReactor extends AbstractReactor {

	public GetGraphMetaModelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * Get Inputs
		 */
		organizeKeys();
		String fileName = this.keyValue.get(this.keysToGet[0]);
		String graphTypeId = this.keyValue.get(this.keysToGet[1]);

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
		
		if (g != null) {
			retMap = GraphUtility.getMetamodel(g.traversal(), graphTypeId);
		}
		if (tinkerDriver == TINKER_DRIVER.NEO4J) {
			try {
				g.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
