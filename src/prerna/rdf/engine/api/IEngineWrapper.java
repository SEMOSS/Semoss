package prerna.rdf.engine.api;


public interface IEngineWrapper {

	public void setQuery(String query);
	
	public void setEngine(IEngine engine);
	
	public void execute();
	
	public boolean hasNext();
}
