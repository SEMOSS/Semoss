package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class JiraIssueTypeReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(JiraIssueTypeReactor.class);

	public JiraIssueTypeReactor() {
		
	}	

	@Override
	public NounMetadata execute() {
		try {
			return JiraHelper.issueType();
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;	
		} catch (Exception e) {
			throw new RuntimeException("An error occurred while retrieving Jira issue types. Error message: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor retrieves all available Jira issue types.";
	}
}
