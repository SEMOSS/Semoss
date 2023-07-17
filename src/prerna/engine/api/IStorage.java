package prerna.engine.api;

import java.util.List;
import java.util.Properties;

public interface IStorage {

	/**
	 * 
	 * @param smssProp
	 */
	void connect(Properties smssProp);

	
	/**
	 * 
	 * @param engineId
	 */
	void setEngineId(String engineId);
	
	/**
	 * 
	 * @return
	 */
	String getEngineId();

	/**
	 * 
	 * @param engineName
	 */
	void setEngineName(String engineName);
	
	/**
	 * 
	 * @return
	 */
	String getEngineName();

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception 
	 */
	List<String> list(String path) throws Exception;
	
	/**
	 * 
	 * @param localFilePath
	 * @param storageFolderPath
	 * @throws Exception
	 */
	void copyToStorage(String localFilePath, String storageFolderPath) throws Exception;
	
	/**
	 * 
	 * @param storageFilePath
	 * @param localFolderPath
	 * @throws Exception
	 */
	void copyToLocal(String storageFilePath, String localFolderPath) throws Exception;

	/**
	 * 
	 * @param storageFilePath
	 * @throws Exception
	 */
	void deleteFromStorage(String storageFilePath) throws Exception;

	/**
	 * 
	 * @param storageFilePath
	 * @param leaveFolderStructure
	 * @throws Exception
	 */
	void deleteFromStorage(String storageFilePath, boolean leaveFolderStructure) throws Exception;
	
}
