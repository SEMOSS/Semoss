package prerna.engine.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface IStorage extends IEngine {

	String CATALOG_TYPE = "STORAGE";

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String STORAGE_TYPE = "STORAGE_TYPE";
	
	/**
	 * 
	 * @return
	 */
	StorageTypeEnum getStorageType();
	
	/**
	 * 
	 * @param smssProp
	 */
	void open(String smssFilePath) throws Exception;
	
	/**
	 * 
	 * @param smssProp
	 */
	void open(Properties smssProp) throws Exception;

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception 
	 */
	List<String> list(String path) throws Exception;
	
	/**
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	List<Map<String, Object>> listDetails(String path) throws Exception;
	
	/**
	 * 
	 * @param localFilePath
	 * @param storagePath
	 * @throws Exception
	 */
	void syncLocalToStorage(String localPath, String storagePath) throws Exception;
	
	/**
	 * 
	 * @param storagePath
	 * @param localPath
	 * @throws Exception
	 */
	void syncStorageToLocal(String storagePath, String localPath) throws Exception;
	
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
	void deleteFromStorage(String storagePath) throws Exception;

	/**
	 * 
	 * @param storagePath
	 * @param leaveFolderStructure
	 * @throws Exception
	 */
	void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception;

	/**
	 * 
	 * @param storageFolderPath
	 * @throws Exception
	 */
	void deleteFolderFromStorage(String storageFolderPath) throws Exception;
	
}
