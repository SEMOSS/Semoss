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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

/**
 * Exports an engine as a zip file for an admin, whatever permission the admin
 * holds on it. The archive holds the engine's folder, its metadata, the model
 * metadata of a model, and its smss file. An engine that holds file locks is
 * closed under its engine lock while it is zipped and opened again after. The
 * export is recorded in the audit trail once the archive is built. This is the
 * admin counterpart to {@link prerna.reactor.utils.ExportEngineReactor}, which
 * exports an engine its user owns.
 */
public class AdminExportEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminExportEngineReactor.class);
	private static final String CLASS_NAME = AdminExportEngineReactor.class.getName();

	/** Whether the archive keeps the engine's version history */
	private static final String KEEP_GIT_KEY = "keepGit";

	/**
	 * Creates the reactor with the engine key and the version history key.
	 */
	public AdminExportEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), KEEP_GIT_KEY };
	}

	/**
	 * Builds the archive of the engine in the insight folder, once the user is
	 * known to be an admin and the engine is known to exist, and registers it with
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
		String engineId = this.keyValue.get(this.keysToGet[0]);
		boolean keepGit = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]));
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Need to define the engine");
		}
		if (!SecurityEngineUtils.engineExists(engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist");
		}

		IEngine engine = Utility.getEngine(engineId);
		logger.info("Exporting engine... ");

		String engineName = engine.getEngineName();
		String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);
		String outputDir = this.insight.getInsightFolder();
		String thisEngineDir = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engineId, engineName);
		String zipFilePath = outputDir + "/" + engineNameAndId + "_engine.zip";

		ReentrantLock lock = null;
		if (engine.holdsFileLocks()) {
			lock = EngineSyncUtility.getEngineLock(engineId);
			lock.lock();
		}
		boolean closed = false;
		try {
			if (lock != null) {
				logger.info("Stopping the engine... ");
				DIHelper.getInstance().removeEngineProperty(engineId);
				try {
					engine.close();
					closed = true;
				} catch (IOException e) {
					classLogger.error("Failed to close engine {} before exporting it", engineId, e);
				}
			} else {
				logger.info("Can export this engine w/o closing... ");
			}

			List<String> ignoreDirs = new ArrayList<>();
			if (!keepGit) {
				ignoreDirs.add(
						engineNameAndId + "/" + Constants.APP_ROOT_FOLDER + "/" + Constants.VERSION_FOLDER + "/.git");
			}
			List<String> ignoreFiles = Arrays.asList(engineNameAndId + "/" + engineName + IEngine.METADATA_FILE_SUFFIX,
					engineNameAndId + "/" + engineName + IEngine.MODEL_METADATA_FILE_SUFFIX);

			try (ZipOutputStream zos = openArchive(thisEngineDir, zipFilePath, ignoreDirs, ignoreFiles, logger)) {
				logger.info("Grabbing engine metadata to write to temporary file to zip...");
				Map<String, Object> engineMeta = SecurityEngineUtils.getAggregateEngineMetadata(engineId, null, false);
				ZipUtils.zipObjectToFile(zos, engineNameAndId,
						outputDir + "/" + engineName + IEngine.METADATA_FILE_SUFFIX, engineMeta);
				logger.info("Done zipping engine metadata...");

				if (IEngine.CATALOG_TYPE.MODEL == engine.getCatalogType()) {
					logger.info("Grabbing model metadata to write to temporary file to zip...");
					Map<String, Object> modelMeta = SecurityModelMetadataUtils.getModelMetadata(engineId);
					if (modelMeta == null || modelMeta.isEmpty()) {
						logger.info("No model metadata to zip");
					} else {
						ZipUtils.zipObjectToFile(zos, engineNameAndId,
								outputDir + "/" + engineName + IEngine.MODEL_METADATA_FILE_SUFFIX, modelMeta);
						logger.info("Done zipping model metadata...");
					}
				}

				File smss = new File(engine.getSmssFilePath());
				logger.info("Adding smss file...");
				ZipUtils.addToZipFile(smss, zos);
				logger.info("Done adding smss file");
			} catch (Exception e) {
				logger.info("Error occurred zipping up engine");
				classLogger.error("Failed to export engine {} to archive {}", engineId, zipFilePath, e);
				throw new SemossPixelException(
						"Error occurred generating zip file. Detailed message = " + e.getMessage(), e);
			}
			logger.info("Finished creating zip");
		} finally {
			try {
				if (closed) {
					logger.info("Opening the engine again...");
					Utility.getEngine(engineId);
					logger.info("Opened the engine");
				}
			} finally {
				if (lock != null) {
					lock.unlock();
				}
			}
		}

		UserAuditTrailUtils.recordEngineExport(user, AuditTargetType.ofEngine(engine.getCatalogType()), engineId,
				engineName, getPixelName(), keepGit);

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(zipFilePath);
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	/**
	 * Opens the export archive: a copy of the engine's folder, less the ignored
	 * folders and files, or an empty archive when the engine has no folder. The
	 * caller closes the archive, which writes its directory, so a close that fails
	 * fails the export.
	 *
	 * @param engineDir   the engine's folder
	 * @param zipFilePath where the archive is written
	 * @param ignoreDirs  folders left out of the copy
	 * @param ignoreFiles files left out of the copy, which the caller writes again
	 * @param logger      the export's progress log
	 * @return the open archive, for the caller to add to and close
	 * @throws FileNotFoundException when the archive cannot be created
	 */
	private static ZipOutputStream openArchive(String engineDir, String zipFilePath, List<String> ignoreDirs,
			List<String> ignoreFiles, Logger logger) throws FileNotFoundException {
		if (!new File(engineDir).exists()) {
			logger.info("No engine folder to zip");
			return new ZipOutputStream(new FileOutputStream(zipFilePath));
		}
		logger.info("Zipping engine files...");
		ZipOutputStream zos = ZipUtils.zipFolder(engineDir, zipFilePath, ignoreDirs, ignoreFiles);
		logger.info("Done zipping engine folder");
		return zos;
	}

	/**
	 * Describes the reactor, its keys and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Exports an engine as a zip file regardless of the caller's engine permissions. The \
				calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to ExportEngine, which exports an engine the caller owns.

				The archive holds the engine's folder, its metadata, the model metadata of a model, and its smss \
				file. The export is recorded in the audit trail.

				Inputs:
				  engine  (required) - the engine id.
				  keepGit (optional) - true to keep the engine's version history in the archive; defaults to false.

				Returns the key (FILE_DOWNLOAD/CONST_STRING) the archive is downloaded with from this insight.
				""").key(ReactorKeysEnum.ENGINE, "Id of the engine to export")
				.key(KEEP_GIT_KEY, key -> key.type(Boolean.class).description(
						"True to keep the engine's version history in the archive; defaults to false"))
				.output(String.class, "The key the archive is downloaded with from this insight");
	}

}
