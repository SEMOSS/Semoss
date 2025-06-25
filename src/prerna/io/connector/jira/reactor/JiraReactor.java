package prerna.io.connector.jira.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(JiraReactor.class);

	public JiraReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.USERID.getKey(),
				ReactorKeysEnum.SUMMARY.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(),
				ReactorKeysEnum.ISSUETYPE.getKey(), ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.JIRAID.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String command = this.keyValue.get(this.keysToGet[0]);
		String userId = this.keyValue.get(this.keysToGet[1]);
		String summary = this.keyValue.get(this.keysToGet[2]);
		String description = this.keyValue.get(this.keysToGet[3]);
		String issuetype = this.keyValue.get(this.keysToGet[4]);
		String project = this.keyValue.get(this.keysToGet[5]);
		String jiraId = this.keyValue.get(this.keysToGet[6]);
		if (this.keyValue.get(this.keysToGet[2]) != null && this.keyValue.get(this.keysToGet[2]) != "") {
			summary = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[3]) != null && this.keyValue.get(this.keysToGet[3]) != "") {
			description = this.keyValue.get(this.keysToGet[3]);
		}
		if (this.keyValue.get(this.keysToGet[4]) != null && this.keyValue.get(this.keysToGet[4]) != "") {
			issuetype = this.keyValue.get(this.keysToGet[4]);
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && this.keyValue.get(this.keysToGet[5]) != "") {
			project = this.keyValue.get(this.keysToGet[5]);
		}
		if (this.keyValue.get(this.keysToGet[6]) != null && this.keyValue.get(this.keysToGet[6]) != "") {
			jiraId = this.keyValue.get(this.keysToGet[6]);
		}
		try {
			switch (command.trim().replaceAll("\\s+", " ").toLowerCase()) {
			case "list all tickets":
				return JiraHelper.listIssue(project, userId);

			case "create new ticket":
				return JiraHelper.createIssue(summary, description, issuetype, project, userId);

			case "delete jira ticket":
				return JiraHelper.deleteIssue(jiraId, userId);

			case "truncate data":
				return JiraHelper.truncateData(userId);

			case "get all projects":
				return JiraHelper.getAllProjects(userId); 
				
			case "delete record for userid":
				return JiraHelper.deleteRecordForUser(userId);
				
			case "type of issue":
				return JiraHelper.issueType(userId);

			}
		} catch (Exception e) {	
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Issue with input");
		}
		return new NounMetadata("Please provide valid command", PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor navigates to implementation of different jira commands";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "Commands to perform for different jira operations" + ReactorKeysEnum.COMMAND.getKey();
		}else if (key.equals(ReactorKeysEnum.USERID.getKey())) {
			return "Unique self incremented user id of the user stored in db" + ReactorKeysEnum.USERID.getKey();
		}else if (key.equals(ReactorKeysEnum.SUMMARY.getKey())) {
			return "Summary of the Jira" + ReactorKeysEnum.SUMMARY.getKey();
		}else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
			return "Description of the Jira" + ReactorKeysEnum.DESCRIPTION.getKey();
		}else if (key.equals(ReactorKeysEnum.ISSUETYPE.getKey())) {
			return "Issue type of the Jira" + ReactorKeysEnum.ISSUETYPE.getKey();
		}else if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "Project name for performing jira operations" + ReactorKeysEnum.PROJECT.getKey();
		}else if (key.equals(ReactorKeysEnum.JIRAID.getKey())) {
			return "Jira id" + ReactorKeysEnum.JIRAID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

	
	
}
