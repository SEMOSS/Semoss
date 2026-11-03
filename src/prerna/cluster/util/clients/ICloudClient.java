/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.cluster.util.clients;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.sql.RdbmsTypeEnum;

public interface ICloudClient {

	/**
	 * Uploads an engine's folder and its smss file to cloud storage. An engine that
	 * holds file locks, such as an H2 database, is closed for the upload and opened
	 * again after.
	 *
	 * @param engineId the engine to upload
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushEngine(String engineId) throws IOException, InterruptedException;

	/**
	 * Downloads an engine's folder and its smss file from cloud storage and
	 * catalogs the engine on this node, for an engine that is not open here. The
	 * engine's type is read from the security database.
	 *
	 * @param engineId the engine to download
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngine(String engineId) throws IOException, InterruptedException;

	/**
	 * Downloads an engine's folder and its smss file from cloud storage and
	 * catalogs the engine on this node, for an engine that is not open here.
	 *
	 * @param engineId   the engine to download
	 * @param engineType the engine's type, which places its folders
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngine(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException;

	/**
	 * Downloads an engine's folder and its smss file from cloud storage. An engine
	 * open on this node is closed for the download and opened again after, so it
	 * reads the new files; one that is not open is cataloged on this node. Nothing
	 * is downloaded for an engine the security database does not have.
	 *
	 * @param engineId            the engine to download
	 * @param engineType          the engine's type, or null to read it from the
	 *                            security database
	 * @param engineAlreadyLoaded true if the engine is open on this node
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngine(String engineId, IEngine.CATALOG_TYPE engineType, boolean engineAlreadyLoaded)
			throws IOException, InterruptedException;

	/**
	 * Uploads only an engine's smss file to cloud storage. The engine's type is
	 * read from the security database.
	 *
	 * @param engineId the engine whose smss file to upload
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushEngineSmss(String engineId) throws IOException, InterruptedException;

	/**
	 * Uploads only an engine's smss file to cloud storage.
	 *
	 * @param engineId   the engine whose smss file to upload
	 * @param engineType the engine's type, which places its folders
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushEngineSmss(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException;

	/**
	 * Downloads only an engine's smss file from cloud storage. The engine's type is
	 * read from the security database.
	 *
	 * @param engineId the engine whose smss file to download
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngineSmss(String engineId) throws IOException, InterruptedException;

	/**
	 * Downloads only an engine's smss file from cloud storage.
	 *
	 * @param engineId   the engine whose smss file to download
	 * @param engineType the engine's type, which places its folders
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngineSmss(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException;

	/**
	 * Uploads one folder of an engine, making the folder at a path in the engine's
	 * cloud folder match the local folder.
	 *
	 * @param engineId              the engine
	 * @param localAbsoluteFilePath the absolute path of the local folder to upload
	 * @param storageRelativePath   the folder's path in the engine's cloud folder,
	 *                              or null for the engine's cloud folder itself
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushEngineFolder(String engineId, String localAbsoluteFilePath, String storageRelativePath)
			throws IOException, InterruptedException;

	/**
	 * Downloads one folder of an engine, making the local folder match the folder
	 * at a path in the engine's cloud folder.
	 *
	 * @param engineId              the engine
	 * @param localAbsoluteFilePath the absolute path of the local folder to
	 *                              download into
	 * @param storageRelativePath   the folder's path in the engine's cloud folder
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngineFolder(String engineId, String localAbsoluteFilePath, String storageRelativePath)
			throws IOException, InterruptedException;

	/**
	 * Deletes an engine's folder, its smss file and its images from cloud storage.
	 * The engine's type is read from the security database.
	 *
	 * @param engineId the engine to delete
	 * @throws IOException          if the delete fails
	 * @throws InterruptedException if the delete is interrupted
	 */
	void deleteEngine(String engineId) throws IOException, InterruptedException;

	/**
	 * Deletes an engine's folder, its smss file and its images from cloud storage.
	 *
	 * @param engineId   the engine to delete
	 * @param engineType the engine's type, which places its folders
	 * @throws IOException          if the delete fails
	 * @throws InterruptedException if the delete is interrupted
	 */
	void deleteEngine(String engineId, IEngine.CATALOG_TYPE engineType) throws IOException, InterruptedException;

	/**
	 * Uploads one file or folder of an engine to the place in the engine's cloud
	 * folder that matches its place in the engine's local folder.
	 *
	 * @param engineId      the engine
	 * @param engineType    the engine's type, which places its folders
	 * @param localFilePath the absolute path of a file or folder in the engine's
	 *                      local folder
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void copyLocalFileToEngineCloudFolder(String engineId, CATALOG_TYPE engineType, String localFilePath)
			throws IOException, InterruptedException;

	/**
	 * Downloads one file or folder of an engine to a local path, from the place in
	 * the engine's cloud folder that matches the path's place in the engine's local
	 * folder.
	 *
	 * @param engineId      the engine
	 * @param engineType    the engine's type, which places its folders
	 * @param localFilePath the absolute path in the engine's local folder to
	 *                      download to
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void copyEngineCloudFileToLocalFile(String engineId, CATALOG_TYPE engineType, String localFilePath)
			throws IOException, InterruptedException;

	/**
	 * Deletes one file or folder of an engine from cloud storage: the one at the
	 * place in the engine's cloud folder that matches a local path's place in the
	 * engine's local folder.
	 *
	 * @param engineId      the engine
	 * @param engineType    the engine's type, which places its folders
	 * @param localFilePath the absolute path in the engine's local folder whose
	 *                      cloud copy to delete
	 * @throws IOException          if the delete fails
	 * @throws InterruptedException if the delete is interrupted
	 */
	void deleteEngineCloudFile(String engineId, CATALOG_TYPE engineType, String localFilePath)
			throws IOException, InterruptedException;

	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Images
	 */

	/**
	 * Downloads every image of one catalog type, such as databases or projects,
	 * from cloud storage into the local image folder for that type.
	 *
	 * @param engineType the catalog type whose images to download
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullEngineAndProjectImageFolder(CATALOG_TYPE engineType) throws IOException, InterruptedException;

	/**
	 * Uploads one image from the local image folder of a catalog type to cloud
	 * storage.
	 *
	 * @param engineType the catalog type the image belongs to
	 * @param fileName   the image's file name in the local image folder
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushEngineAndProjectImage(CATALOG_TYPE engineType, String fileName) throws IOException, InterruptedException;

	/**
	 * Deletes one image of a catalog type from cloud storage.
	 *
	 * @param engineType the catalog type the image belongs to
	 * @param fileName   the image's file name
	 * @throws IOException          if the delete fails
	 * @throws InterruptedException if the delete is interrupted
	 */
	void deleteEngineAndProjectImage(CATALOG_TYPE engineType, String fileName) throws IOException, InterruptedException;

	/**
	 * Deletes the images of one engine or project, the files in the local image
	 * folder whose names start with its id, from cloud storage and from the local
	 * folder.
	 *
	 * @param engineType the catalog type of the engine or project
	 * @param engineId   the id of the engine or project whose images to delete
	 * @throws IOException          if the delete fails
	 * @throws InterruptedException if the delete is interrupted
	 */
	void deleteEngineAndProjectImageById(CATALOG_TYPE engineType, String engineId)
			throws IOException, InterruptedException;

	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Database
	 */

	/**
	 * Uploads the database file of an H2 or SQLite database to cloud storage. The
	 * database is closed for the upload and opened again after.
	 *
	 * @param databaseId the database
	 * @param dbType     the database's type, H2 or SQLite
	 * @throws IllegalArgumentException if the database is not H2 or SQLite, or is
	 *                                  not found
	 * @throws IOException              if the transfer fails
	 * @throws InterruptedException     if the transfer is interrupted
	 */
	void pushLocalDatabaseFile(String databaseId, RdbmsTypeEnum dbType) throws IOException, InterruptedException;

	/**
	 * Downloads the database file of an H2 or SQLite database from cloud storage.
	 * The database is closed for the download and opened again after.
	 *
	 * @param databaseId the database
	 * @param rdbmsType  the database's type, H2 or SQLite
	 * @throws IllegalArgumentException if the database is not H2 or SQLite, or is
	 *                                  not found
	 * @throws IOException              if the transfer fails
	 * @throws InterruptedException     if the transfer is interrupted
	 */
	void pullLocalDatabaseFile(String databaseId, RdbmsTypeEnum rdbmsType) throws IOException, InterruptedException;

	/**
	 * Uploads a database's OWL file, and its positions file when it has one, to
	 * cloud storage.
	 *
	 * @param databaseId the database
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushOwl(String databaseId) throws IOException, InterruptedException;

	/**
	 * Uploads a database's OWL file, and its positions file when it has one, to
	 * cloud storage. The upload reads the files on disk.
	 *
	 * @param databaseId the database
	 * @param owlEngine  the write OWL engine the caller holds, if any
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushOwl(String databaseId, WriteOWLEngine owlEngine) throws IOException, InterruptedException;

	/**
	 * Downloads a database's OWL file and its positions file from cloud storage,
	 * then reloads the OWL.
	 *
	 * @param databaseId the database
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullOwl(String databaseId) throws IOException, InterruptedException;

	/**
	 * Downloads a database's OWL file and its positions file from cloud storage.
	 * The write OWL engine is closed for the download and reloads the OWL after.
	 *
	 * @param databaseId the database
	 * @param owlEngine  the write OWL engine the caller holds, or null to open one
	 *                   for the download and close it after
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullOwl(String databaseId, WriteOWLEngine owlEngine) throws IOException, InterruptedException;

	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Project
	 */

	/**
	 * Uploads a project's folder and its smss file to cloud storage. The project is
	 * closed for the upload and opened again after.
	 *
	 * @param projectId the project to upload
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushProject(String projectId) throws IOException, InterruptedException;

	/**
	 * Downloads a project's folder and its smss file from cloud storage and
	 * catalogs the project on this node, for a project that is not open here.
	 *
	 * @param projectId the project to download
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullProject(String projectId) throws IOException, InterruptedException;

	/**
	 * Downloads a project's folder and its smss file from cloud storage. A project
	 * open on this node is closed for the download and opened again after; one that
	 * is not open is cataloged on this node. Nothing is downloaded for a project
	 * the security database does not have.
	 *
	 * @param projectId            the project to download
	 * @param projectAlreadyLoaded true if the project is open on this node
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullProject(String projectId, boolean projectAlreadyLoaded) throws IOException, InterruptedException;

	/**
	 * Uploads only a project's smss file to cloud storage.
	 *
	 * @param projectId the project whose smss file to upload
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushProjectSmss(String projectId) throws IOException, InterruptedException;

	/**
	 * Downloads only a project's smss file from cloud storage.
	 *
	 * @param projectId the project whose smss file to download
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullProjectSmss(String projectId) throws IOException, InterruptedException;

	/**
	 * Deletes a project's folder, its smss file and its images from cloud storage.
	 *
	 * @param projectId the project to delete
	 * @throws IOException          if the delete fails
	 * @throws InterruptedException if the delete is interrupted
	 */
	void deleteProject(String projectId) throws IOException, InterruptedException;

	/**
	 * Downloads a project's insights database file from cloud storage. The insights
	 * database is closed for the download and opened again after, whether or not
	 * the download works.
	 *
	 * @param projectId the project
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullInsightsDB(String projectId) throws IOException, InterruptedException;

	/**
	 * Uploads a project's insights database file to cloud storage. The insights
	 * database is closed for the upload and opened again after, whether or not the
	 * upload works.
	 *
	 * @param projectId the project
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushInsightDB(String projectId) throws IOException, InterruptedException;

	/**
	 * Uploads one folder of a project, making the folder at a path in the project's
	 * cloud folder match the local folder.
	 *
	 * @param projectId             the project
	 * @param localAbsoluteFilePath the absolute path of the local folder to upload
	 * @param storageRelativePath   the folder's path in the project's cloud folder,
	 *                              or null for the project's cloud folder itself
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushProjectFolder(String projectId, String localAbsoluteFilePath, String storageRelativePath)
			throws IOException, InterruptedException;

	/**
	 * Downloads one folder of a project, making the local folder match the folder
	 * at a path in the project's cloud folder.
	 *
	 * @param projectId             the project
	 * @param localAbsoluteFilePath the absolute path of the local folder to
	 *                              download into
	 * @param storageRelativePath   the folder's path in the project's cloud folder
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullProjectFolder(String projectId, String localAbsoluteFilePath, String storageRelativePath)
			throws IOException, InterruptedException;

	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Insight
	 */

	/**
	 * Uploads one insight's folder of a project to cloud storage.
	 *
	 * @param projectId the project the insight belongs to
	 * @param rdbmsId   the id of the insight
	 * @throws IOException          if the upload fails
	 * @throws InterruptedException if the upload is interrupted
	 */
	void pushInsight(String projectId, String rdbmsId) throws IOException, InterruptedException;

	/**
	 * Downloads one insight's folder of a project from cloud storage.
	 *
	 * @param projectId the project the insight belongs to
	 * @param rdbmsId   the id of the insight
	 * @throws IOException          if the download fails
	 * @throws InterruptedException if the download is interrupted
	 */
	void pullInsight(String projectId, String rdbmsId) throws IOException, InterruptedException;

	/**
	 * Replaces an insight's image in cloud storage: deletes the old image file by
	 * name, since the new one can have another extension, then uploads the new one
	 * from the insight's local folder.
	 *
	 * @param projectId        the project the insight belongs to
	 * @param insightId        the id of the insight
	 * @param oldImageFileName the file name of the image to delete, or null when
	 *                         there is none
	 * @param newImageFileName the file name of the image to upload, or null to
	 *                         upload none
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushInsightImage(String projectId, String insightId, String oldImageFileName, String newImageFileName)
			throws IOException, InterruptedException;

	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * User
	 */

	/**
	 * Downloads a user's asset project folder and its smss file from cloud storage.
	 * An asset project open on this node is closed for the download and opened
	 * again after.
	 *
	 * @param projectId            the id of the user's asset project
	 * @param projectAlreadyLoaded true if the asset project is open on this node
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pullUserAsset(String projectId, boolean projectAlreadyLoaded) throws IOException, InterruptedException;

	/**
	 * Uploads a user's asset project folder and its smss file to cloud storage. The
	 * project is closed for the upload and opened again after.
	 *
	 * @param projectId the id of the user's asset project
	 * @throws IOException          if the transfer fails
	 * @throws InterruptedException if the transfer is interrupted
	 */
	void pushUserAsset(String projectId) throws IOException, InterruptedException;

	/**
	 * Lists what each engine and project bucket in cloud storage holds.
	 *
	 * @return the entries of each bucket, keyed by the bucket's name
	 * @throws IOException          if the listing fails
	 * @throws InterruptedException if the listing is interrupted
	 */
	Map<String, List<String>> listAllContainersByBucket() throws IOException, InterruptedException;

}
