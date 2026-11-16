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
package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.AgentProjectArchiveUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;

/**
 * Exports a project as a zip file for an admin, whatever permission the admin
 * holds on it. The archive holds the project's folder, its metadata, its
 * dependencies, the agent files of an agent, and its smss file; in a cluster
 * the insights database of a project that holds insights is generated for the
 * archive. The project stays open while it is zipped. The export is recorded
 * in the audit trail once the archive is built. This is the admin counterpart
 * to {@link prerna.reactor.utils.ExportProjectReactor}, which exports a
 * project its user owns.
 */
public class AdminExportProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminExportProjectReactor.class);
	private static final String CLASS_NAME = AdminExportProjectReactor.class.getName();

	/** Whether the archive keeps the project's version history */
	private static final String KEEP_GIT_KEY = "keepGit";

	/**
	 * Creates the reactor with the project key and the version history key.
	 */
	public AdminExportProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), KEEP_GIT_KEY };
	}

	/**
	 * Builds the archive of the project in the insight folder, once the user is
	 * known to be an admin and the project is known to exist, and registers it with
	 * the insight for download.
	 *
	 * @return the key the archive is downloaded with
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		boolean keepGit = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]));
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Need to define the project");
		}
		if (!SecurityProjectUtils.projectExists(projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist");
		}

		logger.info("Exporting project now...");
		String baseFolder = Utility.getDIHelperProperty(Constants.BASE_FOLDER).replace("\\", "/");
		if (!baseFolder.endsWith("/")) {
			baseFolder += "/";
		}
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		String projectNameAndId = SmssUtilities.getUniqueName(projectName, projectId);
		String baseProjectDir = baseFolder + Constants.PROJECT_FOLDER;
		String thisProjectDir = baseProjectDir + "/" + projectNameAndId;

		String outputDir = this.insight.getInsightFolder();
		String zipFilePath = outputDir + "/" + projectNameAndId + "_project.zip";

		try {
			logger.info("Can export this project w/o closing... ");

			List<String> ignoreDirs = new ArrayList<>();
			if (!keepGit) {
				ignoreDirs.add(
						projectNameAndId + "/" + Constants.APP_ROOT_FOLDER + "/" + Constants.VERSION_FOLDER + "/.git");
			}

			List<String> ignoreFiles = new ArrayList<>(
					Arrays.asList(projectNameAndId + "/" + projectName + IEngine.METADATA_FILE_SUFFIX,
							projectNameAndId + "/" + projectName + IProject.DEPENDENCIES_FILE_SUFFIX,
							projectNameAndId + "/" + projectName + AgentProjectArchiveUtils.FILE_SUFFIX));

			File insightsFile = null;
			if (ClusterUtil.IS_CLUSTER && project.getProjectType().holdsInsights()) {
				logger.info("Creating insight database ...");
				try {
					insightsFile = SecurityProjectUtils.createInsightsDatabase(projectId, outputDir);
				} catch (Exception e) {
					classLogger.error("Failed to generate the insights database for project {} during export",
							projectId, e);
					throw new IllegalArgumentException(
							"Error occurred attempting to generate the insights database for this project");
				}
				logger.info("Done creating insight database ...");
				ignoreFiles.add(projectNameAndId + "/" + FilenameUtils.getName(insightsFile.getAbsolutePath()));
			}

			logger.info("Zipping project files...");
			try (ZipOutputStream zos = ZipUtils.zipFolder(thisProjectDir, zipFilePath, ignoreDirs, ignoreFiles)) {
				logger.info("Done zipping project files...");

				if (insightsFile != null) {
					logger.info("Zipping insight database ...");
					ZipUtils.addToZipFile(insightsFile, zos, projectNameAndId);
					logger.info("Done zipping insight database...");
				}

				logger.info("Grabbing project metadata to write to temporary file to zip...");
				Map<String, Object> projectMeta = SecurityProjectUtils.getAggregateProjectMetadata(projectId, null,
						false);
				ZipUtils.zipObjectToFile(zos, projectNameAndId,
						outputDir + "/" + projectName + IEngine.METADATA_FILE_SUFFIX, projectMeta);
				logger.info("Done zipping project metadata...");

				logger.info("Grabbing project dependencies to write to temporary file to zip...");
				List<Map<String, Object>> projectDependencies = SecurityProjectUtils
						.getProjectDependencyDetails(projectId, false);
				ZipUtils.zipObjectToFile(zos, projectNameAndId,
						outputDir + "/" + projectName + IProject.DEPENDENCIES_FILE_SUFFIX, projectDependencies);
				logger.info("Done zipping project dependencies...");

				AgentProjectArchiveUtils.exportAgent(zos, project, projectNameAndId);

				logger.info("Zipping project smss...");
				File smss = new File(baseProjectDir + "/" + projectNameAndId + ".smss");
				ZipUtils.addToZipFile(smss, zos);
				logger.info("Done zipping project smss files...");
				logger.info("Zipping Complete");
			}
		} catch (Exception e) {
			classLogger.error("Failed to export project {} to archive {}", projectId, zipFilePath, e);
			throw new SemossPixelException("Error occurred generating zip file. Detailed message = " + e.getMessage());
		}

		IProject.PROJECT_TYPE projectType = project.getProjectType();
		UserAuditTrailUtils.recordProjectExport(user, projectId, projectName,
				projectType == null ? null : projectType.name(), getPixelName(), keepGit);

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(zipFilePath);
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	/**
	 * Describes the reactor, its keys and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Exports a project as a zip file regardless of the caller's project permissions. The \
				calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to ExportProject, which exports a project the caller owns.

				The archive holds the project's folder, its metadata, its dependencies, the agent files of an agent, \
				and its smss file. The export is recorded in the audit trail.

				Inputs:
				  project (required) - the project id.
				  keepGit (optional) - true to keep the project's version history in the archive; defaults to false.

				Returns the key (FILE_DOWNLOAD/CONST_STRING) the archive is downloaded with from this insight.
				""").key(ReactorKeysEnum.PROJECT, "Id of the project to export")
				.key(KEEP_GIT_KEY, key -> key.type(Boolean.class).description(
						"True to keep the project's version history in the archive; defaults to false"))
				.output(String.class, "The key the archive is downloaded with from this insight");
	}

}
