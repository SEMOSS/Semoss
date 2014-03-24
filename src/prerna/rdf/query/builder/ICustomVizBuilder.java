package prerna.rdf.query.builder;

import java.util.Hashtable;


public interface ICustomVizBuilder {
	

	public void setVisualType(String visualType);
	
	public String getVisualType();
	
	public void setJSONDataHash(Hashtable<String,Object> allJSONHash);
	
	public Hashtable<String, Object> getJSONDataHash();
	
	public String getQuery();
	
	public void buildQuery();
	

}
