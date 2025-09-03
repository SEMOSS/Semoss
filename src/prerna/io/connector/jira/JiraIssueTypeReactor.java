package prerna.io.connector.jira;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraIssueTypeReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return JiraHelper.issueType();
	}

	@Override
	public String getReactorDescription() {
		return "This reactor retrieves all available Jira issue types.";
	}
}
