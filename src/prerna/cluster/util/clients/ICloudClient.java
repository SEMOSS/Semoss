package prerna.cluster.util.clients;

import java.io.IOException;
import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.util.sql.RdbmsTypeEnum;

public interface ICloudClient {

	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Moving away from specific types and going to engine interface
	 * 
	 */
	
	/**
	 * 
	 * @param engineId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushEngine(String engineId) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param engineId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullEngine(String engineId) throws IOException, InterruptedException;

	/**
	 * 
	 * @param engineId
	 * @param engineType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullEngine(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param engineId
	 * @param engineType
	 * @param engineAlreadyLoaded
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullEngine(String engineId, IEngine.CATALOG_TYPE engineType, boolean engineAlreadyLoaded) throws IOException, InterruptedException;
	
	/**
	 * Push only the smss file for a engine
	 * 
	 * @param engineId
	 * @throws Exception 
	 */
	void pushEngineSmss(String engineId) throws IOException, InterruptedException; 
	
	/**
	 * Push only the smss file for a engine
	 * 
	 * @param engineId
	 * @param engineType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushEngineSmss(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException; 
	
	/**
	 * Pull only the smss file for a engine
	 * 
	 * @param engineId
	 * @throws Exception
	 */
	void pullEngineSmss(String engineId) throws IOException, InterruptedException;
	
	/**
	 * Pull only the smss file for a engine
	 * 
	 * @param engineId
	 * @param engineType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullEngineSmss(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException; 
	
	/**
	 * Delete the engine from cloud storage
	 * 
	 * @param engineId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteEngine(String engineId) throws IOException, InterruptedException;

	/**
	 * Delete the engine from cloud storage
	 * 
	 * @param engineId
	 * @param engineType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteEngine(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException;
	
	/**
	 * 
	 * @param engineType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullEngineImageFolder(CATALOG_TYPE engineType) throws IOException, InterruptedException;

	/**
	 * 
	 * @param engineType
	 * @param fileName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pushEngineImage(CATALOG_TYPE engineType, String fileName) throws IOException, InterruptedException;

	/**
	 * 
	 * @param engineType
	 * @param fileName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteEngineImage(CATALOG_TYPE engineType, String fileName) throws IOException, InterruptedException;
	
	/**
	 * Copy the engine local file to the corresponding storage location
	 * 
	 * @param engineId
	 * @param engineType
	 * @param localFilePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void copyLocalFileToEngineCloudFolder(String engineId, CATALOG_TYPE engineType, String localFilePath) throws IOException, InterruptedException;

	
	/**
	 * Copy engine file path to local file path, based on where the corresponding storage location should be
	 * 
	 * @param engineId
	 * @param engineType
	 * @param storageRelativePath
	 * @param localFilePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void copyEngineCloudFileToLocalFile(String engineId, CATALOG_TYPE engineType, String localFilePath) throws IOException, InterruptedException;
	
	
	/**
	 * Delete where the corresponding local file path would be from the corresponding storage location
	 * 
	 * @param engineId
	 * @param engineType
	 * @param localFilePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void deleteEngineCloudFile(String engineId, CATALOG_TYPE engineType, String localFilePath) throws IOException, InterruptedException;
	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Database
	 */
	
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
	 * @param rdbmsType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void pullLocalDatabaseFile(String databaseId, RdbmsTypeEnum rdbmsType) throws IOException, InterruptedException;

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
	 * Legacy
	 */
	
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
