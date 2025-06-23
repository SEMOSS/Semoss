package prerna.engine.impl.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


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
