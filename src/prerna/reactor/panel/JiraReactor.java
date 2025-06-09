package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.JiraHelper;

public class JiraReactor extends AbstractReactor {

	public JiraReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(),ReactorKeysEnum.USERNAME.getKey() 
				, ReactorKeysEnum.SUMMARY.getKey(),ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.ISSUETYPE.getKey(),
				ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.JIRAID.getKey()};
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String command = this.keyValue.get(this.keysToGet[0]);
		String userName=this.keyValue.get(this.keysToGet[1]);
		String summary=this.keyValue.get(this.keysToGet[2]);
		String description=this.keyValue.get(this.keysToGet[3]);
		String issuetype=this.keyValue.get(this.keysToGet[4]);
		String project=this.keyValue.get(this.keysToGet[5]);
		String jiraId=this.keyValue.get(this.keysToGet[6]);
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
				return JiraHelper.listIssue(project,userName);

			case "create new jira":
				return JiraHelper.createIssue(summary, description, issuetype, project,userName);

			case "delete jira ticket":
				return JiraHelper.deleteIssue(jiraId,userName);

			}
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input");
		}
		return new NounMetadata("Please provide valid command", PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

}
