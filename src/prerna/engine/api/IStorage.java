package prerna.engine.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface IStorage {

	enum STORAGE_TYPE
	{
		AMAZON_S3,
		GOOGLE_CLOUD_STORAGE,
		GOOGLE_DRIVE,
		MICROSOFT_AZURE_BLOB_STORAGE,
		MINIO,
		SFTP,
	}
	
	/**
	 * 
	 * @return
	 */
	STORAGE_TYPE getStorageType();
	
	/**
	 * 
	 * @param smssProp
	 */
	void connect(Properties smssProp) throws Exception;

	/**
	 * 
	 */
	void disconnect();
	
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
	 * @param smssFilePath
	 */
	void setSmssFilePath(String smssFilePath);
	
	/**
	 * 
	 * @return
	 */
	String getSmssFilePath();
	
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
