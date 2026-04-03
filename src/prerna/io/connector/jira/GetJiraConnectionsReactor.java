package prerna.io.connector.jira;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetJiraConnectionsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> jiraConnections = SecurityExternalConnectorsUtils.getJiraConnections();
		return new NounMetadata(jiraConnections, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	@Override
	public String getReactorDescription() {
		return "Lists Jira connections already configured in SEMOSS. Use to discover available connection ids and aliases. Does not authenticate with Jira or return secrets. Returns a list of id and alias values.";
	}

}
