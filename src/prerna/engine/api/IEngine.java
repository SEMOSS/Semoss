package prerna.engine.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

public interface IEngine extends Closeable {

	String METADATA_FILE_SUFFIX = "_metadata.json";
	
	enum CATALOG_TYPE {
		DATABASE,
		STORAGE,
		MODEL,
		VECTOR,
		FUNCTION,
		VENV,
		// special kind for IProject
		PROJECT,
		IMAGE,
	};
	
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
	 * Opens an engine as defined by its properties file.  
	 * What is included in the properties file is dependent on the type of engine that is being initiated.  
	 * It also includes the ENGINE and ENGINE_ALIAS which coincide with the engineId and engineName
	 * This is the function that first initializes the connection to the engine or at least defines how to connect if done in lazy fashion.
	 * 
	 * @param smssFilePath 			The file path to the smss file containing the engine connection details 
	 */
	void open(String smssFilePath) throws Exception;
	
	/**
	 * Opens an engine as defined by its properties file.  
	 * What is included in the properties file is dependent on the type of engine that is being initiated.  
	 * It also includes the ENGINE and ENGINE_ALIAS which coincide with the engineId and engineName
	 * This is the function that first initializes the connection to the engine or at least defines how to connect if done in lazy fashion.

	 * @param smssProp				The properties object loaded from the smss file containing the engine connection details
	 */
	void open(Properties smssProp) throws Exception;
	
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
	 * Sets the properties object
	 * @param prop
	 */
	void setSmssProp(Properties smssProp);

	/**
	 * Return the prop file
	 * @return
	 */
	Properties getSmssProp();

	/**
	 * Get the original prop file content - w/o additional alterations during opening 
	 * (change primarily happens in H2 Server DB where we alter the connection URL to tcp with dynamic open port)
	 * @return
	 */
	Properties getOrigSmssProp();
	
	/**
	 * 
	 * @return
	 */
	CATALOG_TYPE getCatalogType();
	
	/**
	 * 
	 * @return
	 */
	String getCatalogSubType(Properties smssProp);
	
	/**
	 * Deletes the engine and any stored configuration
	 * @throws IOException 
	 */
	void delete() throws IOException;
	
	/**
	 * Does this engine hold any file locks that would require a close to export/perform other operations
	 * @return
	 */
	boolean holdsFileLocks();
	
}
