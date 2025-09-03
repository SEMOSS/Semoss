package prerna.io.connector.jira;

public class IssueType {

	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "IssueType [name=" + name + "]";
	}

}
