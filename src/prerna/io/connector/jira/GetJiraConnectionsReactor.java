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
		return "Lists the Jira connection records already configured in SEMOSS. Use this when an agent needs to discover available Jira connector aliases or ids before a user or admin selects one. This reactor does not authenticate with Jira and does not return secrets. Returns a list of lightweight maps containing id and alias. If no Jira connections have been configured, the list is empty.";
	}

}
