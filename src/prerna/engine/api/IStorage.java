package prerna.engine.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface IStorage extends IEngine {

	String CATALOG_TYPE = "STORAGE";

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
	void deleteFromStorage(String storagePath) throws Exception;

	/**
	 * 
	 * @param storageFilePath
	 * @param leaveFolderStructure
	 * @throws Exception
	 */
	void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception;

	
}
