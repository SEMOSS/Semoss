package prerna.om;

import java.util.Hashtable;

public class Insight extends Hashtable {
	

	String labelKey = "label";	// Label of the question
	
	Hashtable<String, String> propHash = new Hashtable<String, String>();
	String idKey = "id";	// ID of the question
	String engineKey = "engine";	// Engine question is associated with
	String outputKey = "output";	// Output type of the question
	String sparqlKey = "sparql";	// Sparql of the question
	String descrKey = "description";	// Sparql of the question
	String orderKey = "order";	// order of the question

	// database id where this insight is
	// this may be a URL
	// in memory
	// or a file
	String databaseIDkey = "databaseID";
	
	public Insight(){
		this.put("propHash", propHash);
	}
	
	public String getId() {
		return this.propHash.get(this.idKey);
	}
	public void setId(String id) {
		this.propHash.put(this.idKey, id);
	}
	
	public String getOutput() {
		return this.propHash.get(this.outputKey);
	}

	public void setOutput(String output) {
		this.propHash.put(this.outputKey, output);
	}

	public String getLabel() {
		return (String) this.get(this.labelKey);
	}

	public void setLabel(String label) {
		this.put(this.labelKey, label);
	}

	public String getSparql() {
		return this.propHash.get(this.sparqlKey);
	}

	public void setSparql(String sparql) {
		this.propHash.put(this.sparqlKey, sparql);
	}

	public String getEngine() {
		return this.propHash.get(this.engineKey);
	}

	public void setEngine(String engine) {
		this.propHash.put(this.engineKey, engine);
	}

	public String getDescription() {
		return this.propHash.get(this.descrKey);
	}

	public void setDescription(String descr) {
		this.propHash.put(this.descrKey, descr);
	}

	public String getDatabaseID() {
		return this.propHash.get(this.databaseIDkey);
	}

	public void setDatabaseID(String databaseID) {
		this.propHash.put(this.databaseIDkey, databaseID);
	}
	
	public void setOrder (String order) {
		this.propHash.put(this.orderKey, order);
	}

	public String getOrder() {
		return this.propHash.get(this.orderKey);
	}
	
	// type of database where it is
	public enum DB_TYPE {MEMORY, FILE, REST};
	
	
}
