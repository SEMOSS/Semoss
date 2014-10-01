package prerna.om;

import java.util.StringTokenizer;
import java.util.Vector;

public class SEMOSSParam {

	String name = null;
	String query = null;
	String type = null;
	Vector<String> options = new Vector<String>();
	Boolean hasQuery = true;
	
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
		this.hasQuery = true;
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
	
	public void setOptions(String optionString) {
		optionString = optionString.replaceAll("\"", "");
		StringTokenizer st = new StringTokenizer(optionString, ";");
		while(st.hasMoreElements()) {
			options.add((String)st.nextElement());
		}
		this.hasQuery=false;
	}
	public Vector<String> getOptions() {
		return options;
	}
	public Boolean isQuery() {
		return hasQuery;
	}
	
}
