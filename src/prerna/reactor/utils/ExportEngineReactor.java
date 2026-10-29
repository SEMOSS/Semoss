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

public class ExportEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExportEngineReactor.class);
	private static final String CLASS_NAME = ExportEngineReactor.class.getName();

	/** Whether the archive keeps the engine's version history */
	private static final String KEEP_GIT_KEY = "keepGit";

	public ExportEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), KEEP_GIT_KEY };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		boolean keepGit = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]));

		// security
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			boolean isOwner = SecurityEngineUtils.userIsOwner(user, engineId);
			if (!isOwner) {
				throw new IllegalArgumentException("Engine " + engineId
						+ " does not exist or user does not have permissions to engine. User must be the owner to perform this function.");
			}
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

			// determine if we keep or ignore the git
			List<String> ignoreDirs = new ArrayList<>();
			if (!keepGit) {
				ignoreDirs.add(
						engineNameAndId + "/" + Constants.APP_ROOT_FOLDER + "/" + Constants.VERSION_FOLDER + "/.git");
			}
			// the current metadata files are left out, since they are written again below
			List<String> ignoreFiles = Arrays.asList(engineNameAndId + "/" + engineName + IEngine.METADATA_FILE_SUFFIX,
					engineNameAndId + "/" + engineName + IEngine.MODEL_METADATA_FILE_SUFFIX);

			// closing the archive writes its directory, so a close that fails fails the
			// export instead of handing out a broken zip
			try (ZipOutputStream zos = openArchive(thisEngineDir, zipFilePath, ignoreDirs, ignoreFiles, logger)) {
				// zip up the engine metadata
				logger.info("Grabbing engine metadata to write to temporary file to zip...");
				Map<String, Object> engineMeta = SecurityEngineUtils.getAggregateEngineMetadata(engineId, null, false);
				ZipUtils.zipObjectToFile(zos, engineNameAndId,
						outputDir + "/" + engineName + IEngine.METADATA_FILE_SUFFIX, engineMeta);
				logger.info("Done zipping engine metadata...");

				// zip up the model metadata so the MODELMETADATA values travel with the engine
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

				// add smss file
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
			// open it back up
			try {
				if (closed) {
					logger.info("Opening the engine again...");
					Utility.getEngine(engineId);
					logger.info("Opened the engine");
				}
			} finally {
				if (lock != null) {
					// in case opening up causing an issue - we always want to unlock
					lock.unlock();
				}
			}
		}

		// recorded once the archive is built, so an export that fails records only its
		// failure
		UserAuditTrailUtils.recordEngineExport(user, AuditTargetType.ofEngine(engine.getCatalogType()), engineId,
				engineName, getPixelName(), keepGit);

		// store it in the insight so the FE can download it
		// only from the given insight
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
	 * folders and files, or an empty archive when the engine has no folder.
	 *
	 * @param engineDir   the engine's folder
	 * @param zipFilePath where the archive is written
	 * @param ignoreDirs  folders left out of the copy
	 * @param ignoreFiles files left out of the copy
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

}
