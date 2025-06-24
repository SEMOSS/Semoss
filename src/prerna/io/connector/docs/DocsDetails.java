package prerna.io.connector.docs;

public class DocsDetails {
	
	private String id;
	private String name;
	private String datecreated;
	private String lastupadteddate;
	private String servicejson;
	private String docid;
	private String useremail;
	
	public String getUserName() {
		return name;
	}
	public void setUserName(String name) {
		this.name = name;
	}
	public String getUserEmail() {
		return useremail;
	}
	public void setUserEmail(String email) {
		this.useremail = name;
	}
	public String getDocId() {
		return docid;
	}
	public void setDocId(String docId) {
		this.docid = docId;
	}
	public String getUserId() {
		return id;
	}
	public void setUserId(String userId) {
		this.id = userId;
	}
	public String getDateCreated() {
		return datecreated;
	}
	public void setDateCreated(String dateCreated) {
		this.datecreated = dateCreated;
	}
	public String getLastUpdatedDate() {
		return lastupadteddate;
	}
	public void setLastUpdatedDate(String lastUsed) {
		this.lastupadteddate = lastUsed;
	}
	public String getJson() {
		return servicejson;
	}
	public void setJson(String json) {
		this.servicejson = json;
	}
	@Override
	public String toString() {
		return "DocsDetails [Name=" + name + ", userId=" + id + ", docId=" + docid + ", userEmail=" + useremail + ", dateCreated=" + datecreated + ", lastUsed=" + lastupadteddate + ", ServiceJson=" + servicejson + "]";
	}
	
}
