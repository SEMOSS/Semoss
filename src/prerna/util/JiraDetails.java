package prerna.util;

import java.sql.Timestamp;

public class JiraDetails {
	
	private String apiKey;
	private String userId;
	private String jiraPrimaryId;
	private String url;
	private String dateCreated;
	private String lastUsed;
	public String getApiKey() {
		return apiKey;
	}
	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getJiraPrimaryId() {
		return jiraPrimaryId;
	}
	public void setJiraPrimaryId(String jiraPrimaryId) {
		this.jiraPrimaryId = jiraPrimaryId;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDateCreated() {
		return dateCreated;
	}
	public void setDateCreated(String dateCreated) {
		this.dateCreated = dateCreated;
	}
	public String getLastUsed() {
		return lastUsed;
	}
	public void setLastUsed(String lastUsed) {
		this.lastUsed = lastUsed;
	}
	@Override
	public String toString() {
		return "JiraDetails [apiKey=" + apiKey + ", userId=" + userId + ", jiraPrimaryId=" + jiraPrimaryId + ", url="
				+ url + ", dateCreated=" + dateCreated + ", lastUsed=" + lastUsed + "]";
	}
	
	
	
}
