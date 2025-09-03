package prerna.io.connector.jira;

public class Fields {

	private Project project;
	private String summary;
	private String description;
	private IssueType issuetype;

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public IssueType getIssuetype() {
		return issuetype;
	}

	public void setIssuetype(IssueType issuetype) {
		this.issuetype = issuetype;
	}

	@Override
	public String toString() {
		return "Fields [project=" + project + ", summary=" + summary + ", description=" + description + ", issuetype="
				+ issuetype + "]";
	}

}
