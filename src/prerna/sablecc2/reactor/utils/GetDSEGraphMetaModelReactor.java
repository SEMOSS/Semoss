package prerna.sablecc2.reactor.utils;

import java.util.HashMap;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GraphUtility;

public class GetDSEGraphMetaModelReactor extends AbstractReactor {

	public GetDSEGraphMetaModelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HOST.getKey(), ReactorKeysEnum.PORT.getKey(),
				ReactorKeysEnum.GRAPH_NAME.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// pixel inputs
		organizeKeys();
		String host = this.keyValue.get(this.keysToGet[0]);
		String port = this.keyValue.get(this.keysToGet[1]);
		String graphName = this.keyValue.get(this.keysToGet[2]);
		String graphTypeId = this.keyValue.get(this.keysToGet[3]);

		// dse connection
		DseCluster dseCluster = DseCluster.builder().addContactPoint(host).withPort(Integer.parseInt(port)).withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		DseSession dseSession = dseCluster.connect();
		GraphTraversalSource gts = DseGraph.traversal(dseSession);
		HashMap<String, Object> retMap = GraphUtility.getMetamodel(gts, graphTypeId);
		dseSession.close();
		return new NounMetadata(retMap, PixelDataType.MAP);

	}

}
