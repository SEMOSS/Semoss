package prerna.cluster.util.clients;

import java.io.IOException;
import java.util.List;

import prerna.util.sql.RdbmsTypeEnum;

public interface ICloudClient {

	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Database
	 */
	
	/**
	 * 
	 * @param databaseId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushDatabase(String databaseId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param databaseId
	 * @param dbType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushLocalDatabaseFile(String databaseId, RdbmsTypeEnum dbType) throws IOException, InterruptedException;

	/**
	 * 
	 * @param databaseId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullDatabase(String databaseId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param databaseId
	 * @param databaseAlreadyLoaded
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullDatabase(String databaseId, boolean databaseAlreadyLoaded) throws IOException, InterruptedException; 
	
	/**
	 * 
	 * @param databaseId
	 * @param rdbmsType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullLocalDatabaseFile(String databaseId, RdbmsTypeEnum rdbmsType) throws IOException, InterruptedException;

	/**
	 * Push only the SMSS file for a database
	 * 
	 * @param databaseId
	 * @throws Exception 
	 */
	void pushDatabaseSmss(String databaseId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param databaseId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushOwl(String databaseId) throws IOException, InterruptedException;

	/**
	 * 
	 * @param databaseId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullOwl(String databaseId) throws IOException, InterruptedException;

	/**
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullDatabaseImageFolder() throws IOException, InterruptedException;

	/**
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushDatabaseImageFolder() throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param databaseId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteDatabase(String databaseId) throws IOException, InterruptedException; 
	
	/**
	 * 
	 * @param databaseId
	 * @param localAbsoluteFilePath
	 * @param storageRelativePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushDatabaseFolder(String databaseId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException;

	/**
	 * 
	 * @param databaseId
	 * @param localAbsoluteFilePath
	 * @param storageRelativePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullDatabaseFolder(String databaseId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException;
	
	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Project
	 */
	
	/**
	 * 
	 * @param projectId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushProject(String projectId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param projectId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullProject(String projectId) throws IOException, InterruptedException;

	/**
	 * 
	 * @param projectId
	 * @param projectAlreadyLoaded
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullProject(String projectId, boolean projectAlreadyLoaded) throws IOException, InterruptedException; 
	
	/**
	 * Push only the smss file for a project
	 * 
	 * @param projectId
	 * @throws Exception 
	 */
	void pushProjectSmss(String projectId) throws IOException, InterruptedException; 
	
	/**
	 * Pull only the smss file for a project
	 * 
	 * @param projectId
	 * @throws Exception
	 */
	void pullProjectSmss(String projectId) throws IOException, InterruptedException; 

	
	/**
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullProjectImageFolder() throws IOException, InterruptedException;

	/**
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushProjectImageFolder() throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param projectId
	 * @throws Exception 
	 */
	void deleteProject(String projectId) throws IOException, InterruptedException; 
	
	/**
	 * 
	 * @param projectId
	 * @throws Exception 
	 */
	void pullInsightsDB(String projectId) throws IOException, InterruptedException; 
	
	/**
	 * 
	 * @param projectId
	 * @throws Exception 
	 */
	void pushInsightDB(String projectId) throws IOException, InterruptedException; 

	/**
	 * 
	 * @param projectId
	 * @param localAbsoluteFilePath
	 * @param storageRelativePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushProjectFolder(String projectId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException;

	/**
	 * 
	 * @param projectId
	 * @param localAbsoluteFilePath
	 * @param storageRelativePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullProjectFolder(String projectId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException;


	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Insight
	 */
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @throws Exception 
	 */
	void pushInsight(String projectId, String insightId) throws IOException, InterruptedException; 

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @throws Exception 
	 */
	void pullInsight(String projectId, String insightId) throws IOException, InterruptedException; 

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param oldImageFileName
	 * @param newImageFileName
	 * @throws Exception 
	 */
	void pushInsightImage(String projectId, String insightId, String oldImageFileName, String newImageFileName) throws IOException, InterruptedException; 
	
	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * User
	 */
	
	/**
	 * 
	 * @param projectId
	 * @param isAsset
	 * @param projectAlreadyLoaded
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullUserAssetOrWorkspace(String projectId, boolean isAsset, boolean projectAlreadyLoaded) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param projectId
	 * @param isAsset
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushUserAssetOrWorkspace(String projectId, boolean isAsset) throws IOException, InterruptedException;

	
	///////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Storage
	 */
	
	/**
	 * 
	 * @param storageId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushStorage(String storageId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param storageId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullStorage(String storageId) throws IOException, InterruptedException;

	/**
	 * 
	 * @param storageId
	 * @param storageAlreadyLoaded
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullStorage(String storageId, boolean storageAlreadyLoaded) throws IOException, InterruptedException; 
	
	/**
	 * Push only the smss file for a storage
	 * 
	 * @param projectId
	 * @throws Exception 
	 */
	void pushStorageSmss(String projectId) throws IOException, InterruptedException; 
	
	/**
	 * Pull only the smss file for a storage
	 * 
	 * @param storageId
	 * @throws Exception
	 */
	void pullStorageSmss(String storageId) throws IOException, InterruptedException; 

	
	///////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Model
	 */
	
	/**
	 * 
	 * @param modelId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushModel(String modelId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param modelId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullModel(String modelId) throws IOException, InterruptedException;

	/**
	 * 
	 * @param modelId
	 * @param modelAlreadyLoaded
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullModel(String modelId, boolean modelAlreadyLoaded) throws IOException, InterruptedException; 
	
	/**
	 * Push only the smss file for a model
	 * 
	 * @param modelId
	 * @throws Exception 
	 */
	void pushModelSmss(String modelId) throws IOException, InterruptedException; 
	
	/**
	 * Pull only the smss file for a model
	 * 
	 * @param modelId
	 * @throws Exception
	 */
	void pullModelSmss(String modelId) throws IOException, InterruptedException; 
	

	///////////////////////////////////////////////////////////////////////////////////

	
	/*
	 * Legacy
	 */
	
	
	/**
	 * This is temporary - to fix old cloud deployments so the structure has the split between db and project
	 * @param appId
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Deprecated
	void fixLegacyDbStructure(String appId) throws IOException, InterruptedException;
	
	/**
	 * This is temporary - to fix old cloud deployments so the structure has the split between db and project
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Deprecated
	void fixLegacyImageStructure() throws IOException, InterruptedException;
	
	/**
	 * This is temporary - to fix old cloud deployments so the structure has the split between db and project
	 * @param appId
	 * @param isAsset
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Deprecated
	void fixLegacyUserAssetStructure(String appId, boolean isAsset) throws IOException, InterruptedException;

	
	@Deprecated
	// TODO: need to make sep for db and project
	List<String> listAllBlobContainers() throws IOException, InterruptedException; 

	/**
	 * 
	 * @param containerId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteContainer(String containerId) throws IOException, InterruptedException; 
}
