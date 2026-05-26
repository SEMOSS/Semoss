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
package prerna.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class ExportProjectAppReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExportProjectAppReactor.class);

	private static final String CLASS_NAME = ExportProjectAppReactor.class.getName();
	
	public ExportProjectAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Checking project information and user permissions.");
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		User user = this.insight.getUser();
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			boolean isOwner = SecurityProjectUtils.userIsOwner(user, projectId);
			if (!isOwner) {
				throw new IllegalArgumentException("Project " + projectId + "does not exist or user does not have access to export.");
			}
		}

		logger.info("Exporting project now...");
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");
		if(!baseFolder.endsWith("/")) {
			baseFolder += "/";
		}
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		String projectNameAndId = SmssUtilities.getUniqueName(projectName, projectId);
		String baseProjectDir = baseFolder + Constants.PROJECT_FOLDER;
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectName, projectId);

		String outputDir = this.insight.getInsightFolder();
		String zipFilePath = outputDir + "/" + getFileName(projectNameAndId);

		// since we do not include the insights database and it is auto generated
		// we dont need to lock anymore
		
//		ReentrantLock lock = null;
//		if(project.holdsFileLocks()) {
//			lock = ProjectSyncUtility.getProjectLock(projectId);
//			lock.lock();
//		}
//		boolean closed = false;
		try {
			// zip project
			ZipOutputStream zos = null;
			try {
//				if(lock != null) {
//					logger.info("Stopping the engine... ");
//					DIHelper.getInstance().removeProjectProperty(projectId);
//					try {
//						project.close();
//						closed = true;
//					} catch (IOException e) {
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				} else {
					logger.info("Can export this project w/o closing... ");
//				}
				
				// zip project folder
				logger.info("Zipping project app files...");
				zos = ZipUtils.zipFolder(projectAssetFolder, zipFilePath, null, null);
				logger.info("Done zipping project app files...");
				
				// zip up the project metadata
				{
					logger.info("Grabbing project metadata to write to temporary file to zip...");
					Map<String, Object> projectMeta = SecurityProjectUtils.getAggregateProjectMetadata(projectId, null, false);
					ZipUtils.zipObjectToFile(zos, null, outputDir+"/"+projectName+IEngine.METADATA_FILE_SUFFIX, projectMeta);
					logger.info("Done zipping project metadata...");
				}
				
				// zip up the project dependencies
				{
					logger.info("Grabbing project dependencies to write to temporary file to zip...");
					List<Map<String, Object>> projectDependencies = SecurityProjectUtils.getProjectDependencyDetails(projectId, false);
					ZipUtils.zipObjectToFile(zos, null, outputDir+"/"+projectName+IProject.DEPENDENCIES_FILE_SUFFIX, projectDependencies);
					logger.info("Done zipping project dependencies...");
				}
				
				// add smss file
				logger.info("Zipping project smss...");
				File smss = new File(baseProjectDir + "/" + projectNameAndId + ".smss");
				ZipUtils.addToZipFile(smss, zos);
				logger.info("Done zipping project smss files...");
				logger.info("Zipping Complete");
			} catch (Exception e) {
				logger.info("Error occurred zipping up project");
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException("Error occurred generating zip file. Detailed message = " + e.getMessage());
			} finally {
				try {
					if (zos != null) {
						zos.flush();
						zos.close();
					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		} finally {
			// since we do not include the insights database and it is auto generated
			// we dont need to lock anymore
			
//			lock.unlock();
//			// open it back up
//			try {
//				if(closed) {
//					logger.info("Opening the project again ... ");
//					Utility.getProject(projectId);
//					logger.info("Opened the project");
//				}
//			} finally {
//				if(lock != null) {
//					// in case opening up causing an issue - we always want to unlock
//					lock.unlock();
//				}
//			}
		}

		// Generate a new key for the name of the zip file.
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(zipFilePath);
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}
	
	/**
	 * 
	 * @param projectNameAndId
	 * @return
	 */
	protected String getFileName(String projectNameAndId) {
		return projectNameAndId + "_app.zip";
	}
	
	@Override
	public String getReactorDescription() {
	    return "Export an app as a single zip file";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
	        return "This is a required value containing the id of the app that is being exported";
	    }
	    return super.getDescriptionForKey(key);
	}
	
}
