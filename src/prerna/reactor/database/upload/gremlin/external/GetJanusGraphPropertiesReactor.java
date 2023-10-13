package prerna.reactor.database.upload.gremlin.external;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.GraphUtility;

public class GetJanusGraphPropertiesReactor extends AbstractReactor {

	public GetJanusGraphPropertiesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * Get Inputs
		 */
		organizeKeys();
		String fileName = this.keyValue.get(this.keysToGet[0]);
		if (fileName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires fileName to get graph properties.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		Graph g = null;
		
		File f = new File(fileName);
		if (f.exists()) {
			g = JanusGraphFactory.open(fileName);
		} else {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Invalid janusgraph conf path", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		List<String> properties = new ArrayList<>();
		/*
		 * Open Graph
		 */

		// get graph properties
		if (g != null) {
			properties = GraphUtility.getAllNodeProperties(g.traversal());
			try {
				g.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return new NounMetadata(properties, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
