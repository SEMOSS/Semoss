package prerna.om;

import java.util.Vector;

public class SEMOSSParam {

	String name = null;
	String query = null;
	String type = null;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type.replace("\"","").trim();
	}

	String depends = "false";
	Vector<String> dependVars = new Vector<String>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name.replace("\"","").trim();
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query.replace("\"","").trim();
	}
	public String isDepends() {
		return depends;
	}
	public void setDepends(String depends) {
		this.depends = depends.replace("\"","").trim();
	}
	public void addDependVar(String dependVar)
	{
		dependVars.addElement(dependVar.replace("\"","").trim());
	}
	
	public Vector<String> getDependVars()
	{
		return this.dependVars;
	}
	
}
