package prerna.rdf.engine.api;

public interface ISelectWrapper extends IEngineWrapper {

	public ISelectStatement next();
	
	public String [] getVariables();
}
