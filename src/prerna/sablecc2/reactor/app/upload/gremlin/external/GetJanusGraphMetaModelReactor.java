package prerna.sablecc2.reactor.app.upload.gremlin.external;

import java.io.File;
import java.util.HashMap;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GraphUtility;

public class GetJanusGraphMetaModelReactor extends AbstractReactor {

	public GetJanusGraphMetaModelReactor() {
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
			String msg = "Requires fileName to get graph metamodel.";
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata(msg, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		String graphTypeId = this.keyValue.get(this.keysToGet[1]);
		if (graphTypeId == null) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Requires graphTypeId to get graph metamodel.", PixelDataType.CONST_STRING,
							PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		Graph g = null;
		/*
		 * Open Graph
		 */
		File f = new File(fileName);
		if (f.exists()) {
			g = JanusGraphFactory.open(fileName);
		} else {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Invalid janusgraph conf path",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		HashMap<String, Object> retMap = new HashMap<String, Object>();

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
