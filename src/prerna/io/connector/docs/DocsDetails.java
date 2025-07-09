package prerna.io.connector.docs;

import java.sql.Timestamp;

public class DocsDetails {

	private String id;
	private String name;
	private Timestamp datecreated;
	private String docid;
	private String username;
	private String useremail;
	private String title;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Timestamp getDatecreated() {
		return datecreated;
	}

	public void setDatecreated(Timestamp datecreated) {
		this.datecreated = datecreated;
	}

	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getUseremail() {
		return useremail;
	}

	public void setUseremail(String useremail) {
		this.useremail = useremail;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	@Override
	public String toString() {
		return "DocsDetails [id=" + id + ", name=" + name + ", datecreated=" + datecreated + ", docid=" + docid
				+ ", username=" + username + ", useremail=" + useremail + ", title=" + title + "]";
	}

}
