package prerna.sablecc2.reactor.app.upload.gremlin.external;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GraphUtility;
import prerna.util.MyGraphIoMappingBuilder;

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
		if (fileName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires fileName to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String graphTypeId = this.keyValue.get(this.keysToGet[1]);
		if (graphTypeId == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graphTypeId to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		HashMap<String, Object> retMap = new HashMap<String, Object>();
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.NEO4J;
		if (new File(fileName).isFile() && fileName.contains(".")) {
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
			} else {
				SemossPixelException exception = new SemossPixelException(new NounMetadata("Invalid Neo4j path", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		} else {
			g = TinkerGraph.open();
			try {
				File f = new File(fileName);
				if (!f.exists() ) {
					SemossPixelException exception = new SemossPixelException(new NounMetadata("Invalid graph path", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
					exception.setContinueThreadOfExecution(false);
					throw exception;
				}
				if (tinkerDriver == TINKER_DRIVER.TG) {
					// user kyro to de-serialize the cached graph
					Builder<GryoIo> builder = GryoIo.build();
					builder.graph(g);
					builder.onMapper(new MyGraphIoMappingBuilder());
					GryoIo reader = builder.create();
					reader.readGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.JSON) {
					// user kyro to de-serialize the cached graph
					Builder<GraphSONIo> builder = GraphSONIo.build();
					builder.graph(g);
					builder.onMapper(new MyGraphIoMappingBuilder());
					GraphSONIo reader = builder.create();
					reader.readGraph(fileName);
				} else if (tinkerDriver == TINKER_DRIVER.XML) {
					Builder<GraphMLIo> builder = GraphMLIo.build();
					builder.graph(g);
					builder.onMapper(new MyGraphIoMappingBuilder());
					GraphMLIo reader = builder.create();
					reader.readGraph(fileName);
				} else {
					throw new IllegalArgumentException("Can only process .tg, .json, and .xml files");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (g != null) {
			retMap = GraphUtility.getMetamodel(g.traversal(), graphTypeId);
			try {
				g.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
