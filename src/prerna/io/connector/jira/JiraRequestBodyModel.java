package prerna.io.connector.jira;

public class JiraRequestBodyModel {

	private Fields fields;

	public Fields getFields() {
		return fields;
	}

	public void setFields(Fields fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		return "JiraRequestBodyModel [fields=" + fields + "]";
	}

}
