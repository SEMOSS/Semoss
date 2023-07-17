package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
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

public class ExportDatabaseReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExportDatabaseReactor.class.getName();

	public ExportDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		// security
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if (!isAdmin) {
				boolean isOwner = SecurityDatabaseUtils.userIsOwner(user, databaseId);
				if (!isOwner) {
					throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have permissions to database. User must be the owner to perform this function.");
				}
			}
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			if (!MasterDatabaseUtility.getAllDatabaseIds().contains(databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist");
			}
		}

		logger.info("Exporting Database Now... ");
		logger.info("Stopping the engine ... ");
		// remove the database
		
		String zipFilePath = null;
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		try {
			IEngine database = Utility.getEngine(databaseId);
			DIHelper.getInstance().removeEngineProperty(databaseId);
			database.closeDB();
			
			String databaseName = database.getEngineName();
			String OUTPUT_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/export/ZIPs";
			String engineDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + SmssUtilities.getUniqueName(databaseName, databaseId);
			zipFilePath = OUTPUT_PATH + "/" + databaseName + "_database.zip";
			
			// zip database
			ZipOutputStream zos = null;
			try {
				// zip db folder
				logger.info("Zipping database files...");
				zos = ZipUtils.zipFolder(engineDir, zipFilePath);
				logger.info("Done zipping database files...");
				// add smss file
				File smss = new File(engineDir + "/../" + SmssUtilities.getUniqueName(databaseName, databaseId) + ".smss");
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

			logger.info("Synchronize database Complete");
		} finally {
			// open it back up
			logger.info("Opening the database again ... ");
			Utility.getEngine(databaseId);
			lock.unlock();
		}

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

}
