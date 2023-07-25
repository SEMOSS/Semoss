package prerna.engine.api;

public interface IEngine {

	/**
	 * Sets the unique id for the engine 
	 * @param engineId - id to set the engine 
	 */
	void setEngineId(String engineId);
	
	/**
	 * Gets the engine name for this engine	
	 * @return Name of the engine
	 */
	String getEngineId();

	/**
	 * Sets the name of the engine. This may be a lot of times the same as the Repository Name
	 * @param engineName - Name of the engine that this is being set to 
	 */
	void setEngineName(String engineName);
	
	/**
	 * Gets the engine name for this engine	
	 * @return Name of the engine
	 */
	String getEngineName();
	
	/**
	 * 
	 * @param smssFilePath
	 */
	void setSmssFilePath(String smssFilePath);
	
	/**
	 * 
	 * @return
	 */
	String getSmssFilePath();
	
}
