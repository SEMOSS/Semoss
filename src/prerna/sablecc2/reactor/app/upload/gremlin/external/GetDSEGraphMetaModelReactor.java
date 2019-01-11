package prerna.sablecc2.reactor.app.upload.gremlin.external;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GraphUtility;

public class GetDSEGraphMetaModelReactor extends AbstractReactor {

	public GetDSEGraphMetaModelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HOST.getKey(), ReactorKeysEnum.PORT.getKey(),
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.GRAPH_NAME.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// pixel inputs
		organizeKeys();
		String host = this.keyValue.get(this.keysToGet[0]);
		if (host == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires host to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String port = this.keyValue.get(this.keysToGet[1]);
		if (port == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires port to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String username = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);
		String graphName = this.keyValue.get(this.keysToGet[4]);
		if (graphName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String graphTypeId = this.keyValue.get(this.keysToGet[5]);
		if (graphTypeId == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		Map<String, Object> retMap = new HashMap<>();
		// dse connection
		DseCluster dseCluster = null;

		if (username != null && password != null) {
			dseCluster = DseCluster.builder().addContactPoint(host).withCredentials(username, password)
					.withPort(Integer.parseInt(port)).withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		} else {
			dseCluster = DseCluster.builder().addContactPoint(host).withPort(Integer.parseInt(port))
					.withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		}
		if (dseCluster != null) {
			DseSession dseSession = dseCluster.connect();
			GraphTraversalSource gts = DseGraph.traversal(dseSession);
			retMap = GraphUtility.getMetamodel(gts, graphTypeId);
			dseSession.close();
		} else {
			throw new SemossPixelException(new NounMetadata("Unable to establish connection", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		return new NounMetadata(retMap, PixelDataType.MAP);

	}

}
