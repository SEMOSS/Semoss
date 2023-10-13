package prerna.reactor.database.upload.gremlin.external;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
		
		boolean useLabel = useLabel();
		String graphTypeId = this.keyValue.get(this.keysToGet[1]);
		if(!useLabel) {
			if (graphTypeId == null) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Requires graphTypeId to get graph metamodel.", PixelDataType.CONST_STRING,
								PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
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

		Map<String, Object> retMap = new HashMap<String, Object>();
		if (g != null) {
			if (useLabel) {
				retMap = GraphUtility.getMetamodel(g.traversal());
			} else {
				retMap = GraphUtility.getMetamodel(g.traversal(), graphTypeId);
			}
			try {
				g.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return new NounMetadata(retMap, PixelDataType.MAP);
	}
	
	/**
	 * Query the external db with a label to get the node
	 * 
	 * @return
	 */
	private boolean useLabel() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.USE_LABEL.getKey());
		if (grs != null && !grs.isEmpty()) {
			return (boolean) grs.get(0);
		}
		return false;
	}
}
