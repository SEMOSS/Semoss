package prerna.engine.api;

public interface IFunctionEngine extends IEngine {

	/**
	 * 
	 * @param args
	 * @return
	 */
	Object execute(Object[] args);
	
}
