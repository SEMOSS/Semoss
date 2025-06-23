package prerna.engine.impl.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


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
