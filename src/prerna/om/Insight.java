package prerna.om;

public class Insight {
	
	// ID of the question
	String id = null;
	
	// name of the question
	String label = null;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	//sparql for the question
	String sparql = null;
	
	// database id where this insight is
	// this may be a URL
	// in memory
	// or a file
	String databaseID = null;
	
	// type of entity this insight has
	String entityType = null;
	
	// the layout this insight uses to render itself
	String output = null;
	
	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getSparql() {
		return sparql;
	}

	public void setSparql(String sparql) {
		this.sparql = sparql;
	}

	public String getDatabaseID() {
		return databaseID;
	}

	public void setDatabaseID(String databaseID) {
		this.databaseID = databaseID;
	}

	// type of database where it is
	public enum DB_TYPE {MEMORY, FILE, REST};
	
	
}
