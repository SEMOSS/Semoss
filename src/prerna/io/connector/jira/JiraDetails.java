package prerna.io.connector.jira;

//POJO to insert data in Jira user related data in DB
public class JiraDetails {

	private String url;
	private String userId;
	private String createdBy;
	private String dateCreated;
	private String dateLastUsed;
	private String primaryId;
	private String keyName;
	private String project;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getDateCreated() {
		return dateCreated;
	}

	public void setDateCreated(String dateCreated) {
		this.dateCreated = dateCreated;
	}

	public String getDateLastUsed() {
		return dateLastUsed;
	}

	public void setDateLastUsed(String dateLastUsed) {
		this.dateLastUsed = dateLastUsed;
	}

	public String getPrimaryId() {
		return primaryId;
	}

	public void setPrimaryId(String primaryId) {
		this.primaryId = primaryId;
	}

	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}
}
