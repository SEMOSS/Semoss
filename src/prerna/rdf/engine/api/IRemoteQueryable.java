package prerna.rdf.engine.api;

public interface IRemoteQueryable {

	public void setRemoteID(String id);
	
	public String getRemoteID();
	
	// this is the remote URL to get the next etc from
	public void setRemoteAPI(String engine);
	
	public String getRemoteAPI();
	
}
