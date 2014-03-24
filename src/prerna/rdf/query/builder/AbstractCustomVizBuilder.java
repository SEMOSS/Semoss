package prerna.rdf.query.builder;

import java.util.Hashtable;

import prerna.rdf.query.util.SEMOSSQuery;

public class AbstractCustomVizBuilder implements ICustomVizBuilder{
	public final static String vizTypeKey = "visualizationType";
	SEMOSSQuery semossQuery = new SEMOSSQuery();

	public Hashtable<String, Object> allJSONHash = new Hashtable<String, Object>();
	
	public String visualType = "";
	@Override
	public void setVisualType(String visualType) {
		this.visualType = visualType;
	}

	@Override
	public String getVisualType() {
		return visualType;
	}

	@Override
	public void setJSONDataHash(Hashtable<String, Object> allJSONHash) {
		this.allJSONHash = allJSONHash;
	}

	@Override
	public Hashtable<String, Object> getJSONDataHash() {
		return allJSONHash;
	}

	@Override
	public String getQuery() {
		return semossQuery.getQuery();
	}

	@Override
	public void buildQuery() {
		
	}
}
