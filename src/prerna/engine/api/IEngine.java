package prerna.engine.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

public interface IEngine extends Closeable {

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
	
	/**
	 * Reloads the prop file
	 * @param prop
	 */
	void setSmssProp(Properties smssProp);

	/**
	 * Return the prop file
	 * @return
	 */
	Properties getSmssProp();

	/**
	 * Get the original prop file content - w/o additional alterations during opening db
	 * @return
	 */
	Properties getOrigSmssProp();
	
	/**
	 * 
	 * @return
	 */
	String getCatalogType();
	
	/**
	 * 
	 * @return
	 */
	String getCatalogSubType(Properties smssProp);
	
	/**
	 * Deletes the engine and any stored configuration
	 */
	void delete();
	
	/**
	 * Closes the connection associated with the engine.  This will prevent further changes from being made and 
	 * ends any transactions
	 */
	@Override
	void close() throws IOException;
	
//	/**
//	 * Load the SMSS file and create the connection to the engine
//	 * @param smssFilePath
//	 */
//	void connect(String smssFilePath) throws Exception;
//	
//	/**
//	 * Use the properties in the prop file (the in-memory representation of the smss file) 
//	 * to connec to the engine
//	 * @param prop
//	 */
//	void connect(Properties prop) throws Exception;
	
}
