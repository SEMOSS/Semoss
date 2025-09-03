package prerna.io.connector.jira;

//This pojo is to return id and link of Jira issues after creation
public class JiraTicketDetails {

	private String id;
	private String link;
	private String summary;

	public JiraTicketDetails(String id, String link, String summary) {
		this.id = id;
		this.link = link;
		this.summary = summary;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}
}
