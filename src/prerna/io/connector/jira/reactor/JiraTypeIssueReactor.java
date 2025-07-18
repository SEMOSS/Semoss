package prerna.io.connector.jira.reactor;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.JiraHelper;

public class JiraTypeIssueReactor extends AbstractReactor{

	@Override
	public NounMetadata execute() {
		return JiraHelper.issueType();
	}

}
