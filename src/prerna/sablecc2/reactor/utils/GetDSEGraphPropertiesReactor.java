package prerna.sablecc2.reactor.utils;

import java.util.List;

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

public class GetDSEGraphPropertiesReactor extends AbstractReactor {

	public GetDSEGraphPropertiesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HOST.getKey(), ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.GRAPH_NAME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// pixel inputs
		organizeKeys();
		String host = this.keyValue.get(this.keysToGet[0]);
		String port = this.keyValue.get(this.keysToGet[1]);
		String graphName = this.keyValue.get(this.keysToGet[2]);
		// dse connection
		DseCluster dseCluster = DseCluster.builder().addContactPoint(host).withPort(Integer.parseInt(port)).withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		DseSession dseSession = dseCluster.connect();
		GraphTraversalSource gts = DseGraph.traversal(dseSession);
		List<String> properties = GraphUtility.getAllNodeProperties(gts);
		dseCluster.close();
		return new NounMetadata(properties, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
