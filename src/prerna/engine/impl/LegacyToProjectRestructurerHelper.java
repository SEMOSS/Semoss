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
package prerna.engine.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.UserAssetUtils;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * 
 * The intention of this class is to build the new project structure from legacy
 * old db folders. Assumption is that this code just runs once. It will read the
 * existing app data from the existing baseFolder/db/ folder and populate the
 * baseFolder/project/ folder. Below is how the new folder structure will
 * be(might need changes).
 * 
 * baseFolder | |- db | |- Name1__UUID | |- Name2__UUID | |- project | |-
 * Project1__UUID | | |- version | | |- insights.db | | | |- Project2_UUID | |
 * |- version | | |- insights.db | |- user | |- Asset__UUID | | |- version | |-
 * Asset__UUID | | |- version
 * 
 */

public class LegacyToProjectRestructurerHelper {

	private static final Logger classLogger = LogManager.getLogger(LegacyToProjectRestructurerHelper.class);

	private String baseFolder;
	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String ENGINE_DIRECTORY;
	public static final String PROJECT_DIRECTORY;
	public static final String USER_DIRECTORY;
	static {
		ENGINE_DIRECTORY = DIR_SEPARATOR + Constants.DATABASE_FOLDER + DIR_SEPARATOR;
		PROJECT_DIRECTORY = DIR_SEPARATOR + Constants.PROJECT_FOLDER + DIR_SEPARATOR;
		USER_DIRECTORY = DIR_SEPARATOR + Constants.USER_FOLDER + DIR_SEPARATOR;
	}

	public void init() {
		// Create the project dir.
		String baseFolder = Utility.getBaseFolder();
		createFolder(baseFolder + PROJECT_DIRECTORY);
		createFolder(baseFolder + USER_DIRECTORY);
	}

	public void executeRestructure() {
		System.out.println("STARTING APP/PROJECT RESTRUCTURE");
		this.baseFolder = Utility.getBaseFolder();
		init();
		// this.engine = engine;
		String dbDir = baseFolder + ENGINE_DIRECTORY;
		String projectDir = baseFolder + PROJECT_DIRECTORY;
		String userDir = baseFolder + USER_DIRECTORY;
		createProjectStructure(dbDir, projectDir, userDir);
		System.out.println("DONE APP/PROJECT RESTRUCTURE");
	}

	public void createProjectStructure(String dbDir, String projDir, String userDir) {
		if (!folderExists(dbDir)) {
			throw new IllegalStateException("There is no DB folder to copy data from.");
		}
		// Get all the existing db folders inside the /db folder
		// by querying the database. Then do all the stuff.
		// At last delete the old folders inside the /db folder.

		File dbFolder = new File(this.baseFolder + ENGINE_DIRECTORY);
		String[] smssFiles = dbFolder.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".smss");
			}
		});

		for (String smssFileName : smssFiles) {
			String folderName = smssFileName.replace(".smss", "");
			try {
				// ignore security, localmaster, etc.
				if (!folderName.contains("__")) {
					System.out.println("\tIGNORE " + folderName);
					continue;
				}
				String[] split = folderName.split("__");
				String appName = split[0];
				String appId = split[1];

				if (UserAssetUtils.isAssetProject(appId)) {
					System.out.println("\tSTART REFACTORING " + appName + " at " + folderName);
					userCopyDataToNewFolderStructure(folderName, userDir, dbDir, true);
					System.out.println("\tDONE REFACTORING " + appName + " at " + folderName);
				} else {
					Properties prop = Utility.loadProperties(dbFolder + "/" + folderName + ".smss");
					if (Boolean.parseBoolean(prop.getProperty(Constants.IS_ASSET_APP))
							|| AbstractSecurityUtils.ignoreDatabase(appId)) {
						System.out.println("\tIS AN ASSET - IGNORE " + folderName);
						continue;
					}
					if (prop.get(Constants.RDBMS_INSIGHTS_TYPE) == null
							&& prop.get(Constants.RDBMS_INSIGHTS_TYPE) == null) {
						System.out.println("\tNOT A LEGACY DB - IGNORE " + folderName);
						continue;
					}
					System.out.println("\tSTART REFACTORING " + appName + " at " + folderName);
					copyDataToNewFolderStructure(folderName, projDir, dbDir);
//					deleteOldFiles(dbDir, folderName);
					System.out.println("\tDONE REFACTORING " + appName + " at " + folderName);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	// Copy the version and the insights db inside the project folder.
	public void userCopyDataToNewFolderStructure(String dbFolderName, String baseProjFolder, String dbDir,
			boolean isAsset) throws IOException {
		String projectName = dbFolderName.split("__")[0];// Keep project name same.
		String projectId = dbFolderName.split("__")[1]; // Keep the project id the same as well
		String newUserFolder = baseProjFolder + SmssUtilities.getUniqueName(projectName, projectId);
		String dbFolder = dbDir + dbFolderName;

		File pFolder = new File(Utility.normalizePath(newUserFolder));
		if (pFolder.exists() && pFolder.isDirectory()) {
			System.out.println("\tALREADY REFACTORED... IGNORING");
			return;
		}

		// Create the new project folder.
		createFolder(newUserFolder);
		// Start the copy.
		userScanAndCopyVersionsIntoNewProjectFolder(newUserFolder, dbFolder, isAsset);
		userScanAndCopyInsightsDatabaseIntoNewProjectFolder(newUserFolder, dbFolder, isAsset);
		// Create the smss.
		userCreateSmssFiles(dbDir, newUserFolder, dbFolderName, projectName, projectId, isAsset);
	}

	// Helper method to copy versions to the project folder.
	public void userScanAndCopyVersionsIntoNewProjectFolder(String newUserFolder, String dbFolder, boolean isAsset)
			throws IOException {
		String newVersionPath = newUserFolder + DIR_SEPARATOR + "app_root" + DIR_SEPARATOR + "version";
		String copyToFile = newUserFolder + DIR_SEPARATOR + "app_root" + DIR_SEPARATOR + "version";
		String oldVersionPath = dbFolder + DIR_SEPARATOR + "version";
		if (!folderExists(oldVersionPath)) {
			copyToFile = newUserFolder + DIR_SEPARATOR + "app_root";
			oldVersionPath = dbFolder + DIR_SEPARATOR + "app_root";
			if (!folderExists(oldVersionPath)) {
				return;
			}
		}
		// Create the version folder in the project folder first.
		createFolder(newVersionPath);
		File newVersionPathFile = new File(Utility.normalizePath(copyToFile));
		File oldVersionPathFile = new File(Utility.normalizePath(oldVersionPath));
		FileUtils.copyDirectory(oldVersionPathFile, newVersionPathFile);
	}

	// Helper method to copy insights db to the project folder.
	public void userScanAndCopyInsightsDatabaseIntoNewProjectFolder(String newUserFolder, String dbFolder,
			boolean isAsset) throws IOException {
		String oldInsightsDatabase_mv_db = dbFolder + DIR_SEPARATOR + "insights_database.mv.db";
		String newInsightsDatabase_mv_db = newUserFolder + DIR_SEPARATOR + "insights_database.mv.db";
		if (!fileExists(oldInsightsDatabase_mv_db)) {
			oldInsightsDatabase_mv_db = dbFolder + DIR_SEPARATOR + "insights_database.sqlite";
			newInsightsDatabase_mv_db = newUserFolder + DIR_SEPARATOR + "insights_database.sqlite";
			if (!fileExists(oldInsightsDatabase_mv_db)) {
				return;
			}
		}
		File newInsightDBFile = new File(Utility.normalizePath(newInsightsDatabase_mv_db));
		File oldInsightDBFile = new File(Utility.normalizePath(oldInsightsDatabase_mv_db));
		FileUtils.copyFile(oldInsightDBFile, newInsightDBFile);
	}

	// Create the project smss file. Do we need to modify the existing smss under
	// /db? For now leaving that modification.
	private void userCreateSmssFiles(String dbDir, String newProjectFolder, String dbSmssFileName, String projectName,
			String projectId, boolean isAsset) throws IOException {
		String dbSmssFile = dbDir + dbSmssFileName + ".smss";
		if (!fileExists(dbSmssFile)) {
			throw new IllegalArgumentException("No old smss file found for the db - " + dbSmssFileName);
		}

		userCreateProjectSmssFile(projectName, projectId, isAsset, dbSmssFile);
//		modifyOldDBSmss(dbSmssFile, dbSmssFileName);
	}

	// Create the project smss
	private void userCreateProjectSmssFile(String projectName, String projectId, boolean isAsset, String dbSmssFile)
			throws IOException {
		RdbmsTypeEnum existingRdbmsType = null;
		try {
			Properties prop = Utility.loadProperties(dbSmssFile);
			existingRdbmsType = RdbmsTypeEnum.valueOf(prop.get(Constants.RDBMS_INSIGHTS_TYPE) + "");
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		File tempProjectSmss = SmssUtilities.createTemporaryAssetAndWorkspaceSmss(projectId, projectName, isAsset,
				existingRdbmsType);
		File smssFile = new File(tempProjectSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempProjectSmss, smssFile);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		tempProjectSmss.delete();
	}

	// Routine to remove any insight related properties from the old smss. Not used
	// right now.
	private void modifyOldDBSmss(String oldSmssFilePath, String oldSmssFileName) throws IOException {
		String newSmssPath = this.baseFolder + ENGINE_DIRECTORY + oldSmssFileName + ".temp_app_breakout";
		File newSmssFile = new File(newSmssPath);
		newSmssFile.createNewFile();

		try (FileReader fReader = new FileReader(oldSmssFilePath);
				BufferedReader bReader = new BufferedReader(fReader);
				FileWriter fWriter = new FileWriter(newSmssFile);
				BufferedWriter bWriter = new BufferedWriter(fWriter);) {
			String line = bReader.readLine();
			while (line != null) {
				if (line.startsWith("RDBMS_INSIGHTS") || line.startsWith("RDBMS_INSIGHTS_TYPE")) {
					line = bReader.readLine();
					continue;
				}
				bWriter.write(line);
				bWriter.newLine();
				line = bReader.readLine();
			}
		}

		// delete the old smss
		File oldSmssFile = new File(oldSmssFilePath);
		oldSmssFile.delete();
		// rename to the original name
		newSmssFile.renameTo(oldSmssFile);
	}

	// Delete the version folders and the insights db from under /db. Runs only if
	// the copy went good.
	private void deleteOldFiles(String dbDir, String folderName) throws IOException {
		String pathToFiles = dbDir + folderName;
		File versionDir = new File(pathToFiles + DIR_SEPARATOR + "version");
		File insightsDatabase = new File(pathToFiles + DIR_SEPARATOR + "insights_database.mv.db");
		FileUtils.deleteDirectory(versionDir);
		insightsDatabase.delete();
	}

	/*
	 * 
	 * User Asset / Workspace methods
	 * 
	 */

	// Copy the version and the insights db inside the project folder.
	public void copyDataToNewFolderStructure(String dbFolderName, String baseProjFolder, String dbDir)
			throws IOException {
		String projectName = dbFolderName.split("__")[0];// Keep project name same.
//		String projectId = UUID.randomUUID().toString(); // create a new project id.
		String projectId = dbFolderName.split("__")[1]; // Keep the project id the same as well
		String newProjectFolder = baseProjFolder + SmssUtilities.getUniqueName(projectName, projectId);
		String dbFolder = dbDir + dbFolderName;

		File pFolder = new File(Utility.normalizePath(newProjectFolder));
		if (pFolder.exists() && pFolder.isDirectory()) {
			System.out.println("\tALREADY REFACTORED... IGNORING");
			return;
		}

		// Create the new project folder.
		createFolder(newProjectFolder);
		// Start the copy.
		scanAndCopyVersionsIntoNewProjectFolder(newProjectFolder, dbFolder);
		scanAndCopyInsightsDatabaseIntoNewProjectFolder(newProjectFolder, dbFolder);
		// Create the smss.
		createSmssFiles(dbDir, newProjectFolder, dbFolderName, projectName, projectId);
	}

	// Helper method to copy versions to the project folder.
	private void scanAndCopyVersionsIntoNewProjectFolder(String newProjectFolder, String dbFolder) throws IOException {
		String newVersionPath = newProjectFolder + DIR_SEPARATOR + "app_root" + DIR_SEPARATOR + "version";
		String copyToFile = newProjectFolder + DIR_SEPARATOR + "app_root" + DIR_SEPARATOR + "version";
		String oldVersionPath = dbFolder + DIR_SEPARATOR + "version";
		if (!folderExists(oldVersionPath)) {
			copyToFile = newProjectFolder + DIR_SEPARATOR + "app_root";
			oldVersionPath = dbFolder + DIR_SEPARATOR + "app_root";
			if (!folderExists(oldVersionPath)) {
				return;
			}
		}
		// Create the version folder in the project folder first.
		createFolder(newVersionPath);
		File newVersionPathFile = new File(Utility.normalizePath(copyToFile));
		File oldVersionPathFile = new File(Utility.normalizePath(oldVersionPath));
		FileUtils.copyDirectory(oldVersionPathFile, newVersionPathFile);
	}

	// Helper method to copy insights db to the project folder.
	private void scanAndCopyInsightsDatabaseIntoNewProjectFolder(String newProjectFolder, String dbFolder)
			throws IOException {
		String oldInsightsDatabase_mv_db = dbFolder + DIR_SEPARATOR + "insights_database.mv.db";
		String newInsightsDatabase_mv_db = newProjectFolder + DIR_SEPARATOR + "insights_database.mv.db";
		if (!fileExists(oldInsightsDatabase_mv_db)) {
			oldInsightsDatabase_mv_db = dbFolder + DIR_SEPARATOR + "insights_database.sqlite";
			newInsightsDatabase_mv_db = newProjectFolder + DIR_SEPARATOR + "insights_database.sqlite";
			if (!fileExists(oldInsightsDatabase_mv_db)) {
				return;
			}
		}
		File newInsightDBFile = new File(Utility.normalizePath(newInsightsDatabase_mv_db));
		File oldInsightDBFile = new File(Utility.normalizePath(oldInsightsDatabase_mv_db));
		FileUtils.copyFile(oldInsightDBFile, newInsightDBFile);
	}

	// Create the project smss file. Do we need to modify the existing smss under
	// /db? For now leaving that modification.
	private void createSmssFiles(String dbDir, String newProjectFolder, String dbSmssFileName, String projectName,
			String projectId) throws IOException {
		String dbSmssFile = dbDir + dbSmssFileName + ".smss";
		if (!fileExists(dbSmssFile)) {
			throw new IllegalArgumentException("No old smss file found for the db - " + dbSmssFileName);
		}

		createProjectSmssFile(projectName, projectId, dbSmssFile);
//		modifyOldDBSmss(dbSmssFile, dbSmssFileName);
	}

	// Create the project smss
	private void createProjectSmssFile(String projectName, String projectId, String dbSmssFile) throws IOException {
		Properties prop = null;
		RdbmsTypeEnum existingRdbmsType = null;
		IProject.PROJECT_TYPE projectEnumType = IProject.PROJECT_TYPE.INSIGHTS;
		boolean hasPortal = false;
		String portalName = null;
		String projectGitProvider = null;
		String projectGitCloneUrl = null;
		try {
			prop = Utility.loadProperties(dbSmssFile);
			existingRdbmsType = RdbmsTypeEnum.valueOf(prop.get(Constants.RDBMS_INSIGHTS_TYPE) + "");
			hasPortal = Boolean.parseBoolean(prop.getProperty(Settings.PUBLIC_HOME_ENABLE));
			portalName = prop.getProperty(Settings.PORTAL_NAME);
			projectGitProvider = prop.getProperty(Constants.PROJECT_GIT_PROVIDER);
			projectGitCloneUrl = prop.getProperty(Constants.PROJECT_GIT_CLONE);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File tempProjectSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, projectEnumType,
				hasPortal, portalName, projectGitProvider, projectGitCloneUrl, existingRdbmsType);
		File smssFile = new File(tempProjectSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempProjectSmss, smssFile);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		tempProjectSmss.delete();
	}

	/**
	 * Helper methods.
	 * 
	 */

	private boolean folderExists(String dir) {
		if (dir == null || (dir = dir.trim()).isEmpty()) {
			throw new IllegalArgumentException("Path " + dir + " is not valid. Check config file.");
		}
		File file = new File(dir);
		if (file.isDirectory()) {
			return true;
		}
		return false;
	}

	private void createFolder(String dir) {
		if (dir == null || (dir = dir.trim()).isEmpty()) {
			throw new IllegalArgumentException("Path " + dir + " is not valid. Check config file.");
		}
		File file = new File(Utility.normalizePath(dir));
		if (!file.exists() || !file.isDirectory()) {
			file.mkdirs();
		}
	}

	private boolean fileExists(String filePath) {
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new IllegalArgumentException("Filepath is empty.");
		}
		File file = new File(filePath);
		if (file.isFile()) {
			return true;
		}
		return false;
	}

}
