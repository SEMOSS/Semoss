package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.InsightFile;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class ExportProjectReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExportProjectReactor.class.getName();

	public ExportProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Checking database information and user permissions.");
		organizeKeys();
		String project = keysToGet[0];
		/*
		 * Need to check with Maher if the below security works for projects as well.
		 */
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled()) {
			project = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), project);
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if (!isAdmin) {
				boolean isOwner = SecurityAppUtils.userIsOwner(user, project);
				if (!isOwner)
					throw new IllegalArgumentException("Project " + project + "does not exist.");
			}
		} else {
			project = MasterDatabaseUtility.testDatabaseIdIfAlias(project);
			if (!MasterDatabaseUtility.getAllDatabaseIds().contains(project))
				throw new IllegalArgumentException("Project " + project + " does not exist.");
		}

		logger.info("Exporting project now...");
		logger.info("Stopping the engine...");

		String zipFilePath = null;
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		IEngine engine = Utility.getEngine(project);
		String engineName = engine.getEngineName();
		String OUTPUT_PATH = baseFolder + "/export/ZIPs";
		String projectDir = baseFolder + "/project/" + SmssUtilities.getUniqueName(engineName, project);
		// String projectDir = baseFolder + "/project/" +
		// SmssUtilities.getUniqueName(engineName, project);
		zipFilePath = OUTPUT_PATH + "/" + engineName + "_project.zip";

		Lock lock = EngineSyncUtility.getEngineLock(engineName);
		lock.lock();
		try {
			DIHelper.getInstance().removeLocalProperty(project);
			engine.closeDB();
			// zip project
			ZipOutputStream zos = null;
			try {
				// zip project folder
				logger.info("Zipping project files...");
				zos = ZipUtils.zipFolder(projectDir, zipFilePath);
				logger.info("Done zipping app files...");
				// add smss file
				File smss = new File(projectDir + "/../" + SmssUtilities.getUniqueName(engineName, project) + ".smss");
				logger.info("Saving file " + smss.getName());
				ZipUtils.addToZipFile(smss, zos);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (zos != null) {
						zos.flush();
						zos.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			logger.info("Zipping Complete");
		} finally {
			// open it back up
			logger.info("Opening the engine again ... ");
			Utility.getEngine(project);
			lock.unlock();
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

}
