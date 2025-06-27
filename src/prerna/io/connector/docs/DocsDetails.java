package prerna.io.connector.docs;

import java.sql.Timestamp;

public class DocsDetails {

	private String id;
	private String name;
	private Timestamp datecreated;
	private Timestamp lastupadteddate;
	private String servicejson;
	private String docid;
	private String docname;
	private String username;
	private String useremail;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getUserName() {
		return username;
	}

	public void setUserName(String name) {
		this.username = name;
	}
	
	public String getDocName() {
		return docname;
	}

	public void setDocName(String name) {
		this.docname = name;
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

	public String getId() {
		return id;
	}

	public void setId(String userId) {
		this.id = userId;
	}

	public Timestamp getDateCreated() {
		return datecreated;
	}

	public void setDateCreated(Timestamp dateCreated) {
		this.datecreated = dateCreated;
	}

	public Timestamp getLastUpdatedDate() {
		return lastupadteddate;
	}

	public void setLastUpdatedDate(Timestamp lastUsed) {
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
		return "DocsDetails [id=" + id + ", name=" + name + ", datecreated=" + datecreated + ", lastupadteddate="
				+ lastupadteddate + ", servicejson=" + servicejson + ", docid=" + docid + ", docname=" + docname
				+ ", username=" + username + ", useremail=" + useremail + "]";
	}

}
