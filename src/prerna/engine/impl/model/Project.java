package prerna.engine.impl.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


public class Project {

	private String key;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "Project [key=" + key + "]";
	}

}
