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
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
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

public class ExportAppReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExportAppReactor.class.getName();

	public ExportAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		// security
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if (!isAdmin) {
				boolean isOwner = SecurityAppUtils.userIsOwner(user, appId);
				if (!isOwner) {
					throw new IllegalArgumentException("App " + appId + " does not exist or user does not have permissions to database. User must be the owner to perform this function.");
				}
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if (!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}

		logger.info("Exporting Database Now... ");
		logger.info("Stopping the engine ... ");
		// remove the app
		
		String zipFilePath = null;
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			IEngine engine = Utility.getEngine(appId);
			DIHelper.getInstance().removeLocalProperty(appId);
			engine.closeDB();
			
			String engineName = engine.getEngineName();
			String OUTPUT_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/export/ZIPs";
			String engineDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + SmssUtilities.getUniqueName(engineName, appId);
			zipFilePath = OUTPUT_PATH + "/" + engineName + ".zip";
			
			// zip database
			ZipOutputStream zos = null;
			try {
				// zip db folder
				logger.info("Zipping app files...");
				zos = ZipUtils.zipFolder(engineDir, zipFilePath);
				logger.info("Done zipping app files...");
				// add smss file
				File smss = new File(engineDir + "/../" + SmssUtilities.getUniqueName(engineName, appId) + ".smss");
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

			logger.info("Synchronize Database Complete");
		} finally {
			// open it back up
			logger.info("Opening the engine again ... ");
			Utility.getEngine(appId);
			lock.unlock();
		}

		// store it in the insight so the FE can download it
		// only from the given insight
		String randomKey = UUID.randomUUID().toString();
		this.insight.addExportFile(randomKey, zipFilePath);
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

}
