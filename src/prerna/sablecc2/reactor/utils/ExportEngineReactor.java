package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class ExportEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExportEngineReactor.class);
	private static final String CLASS_NAME = ExportEngineReactor.class.getName();

	public ExportEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		// security
		User user = this.insight.getUser();
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			boolean isOwner = SecurityEngineUtils.userIsOwner(user, engineId);
			if (!isOwner) {
				throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have permissions to engine. User must be the owner to perform this function.");
			}
		}

		IEngine engine = Utility.getEngine(engineId);
		logger.info("Exporting engine... ");
		String zipFilePath = null;
		ReentrantLock lock = null;
		if(engine.holdsFileLocks()) {
			lock = EngineSyncUtility.getEngineLock(engineId);
			lock.lock();
		}
		boolean closed = false;
		try {
			if(lock != null) {
				logger.info("Stopping the engine... ");
				DIHelper.getInstance().removeEngineProperty(engineId);
				try {
					engine.close();
					closed = true;
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			} else {
				logger.info("Can export this engine w/o closing... ");
			}
			
			String engineName = engine.getEngineName();
			String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);
			String outputDir = this.insight.getInsightFolder();
			String thisEngineDir = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engineId, engineName);
			File thisEngineF = new File(thisEngineDir);
			zipFilePath = outputDir + "/" + engineNameAndId + "_engine.zip";
			
			// zip database
			ZipOutputStream zos = null;
			try {
				// zip db folder
				if(thisEngineF.exists()) {
					logger.info("Zipping engine files...");
					zos = ZipUtils.zipFolder(thisEngineDir, zipFilePath);
					logger.info("Done zipping engine folder");
				} else {
					logger.info("No engine folder to zip");
				}
				
				// zip up the engine metadata
				{
					logger.info("Grabbing engine metadata to write to temporary file to zip...");
					Map<String, Object> engineMeta = SecurityEngineUtils.getAggregateEngineMetadata(engineId, null, false);
					ZipUtils.zipObjectToFile(zos, engineNameAndId, outputDir+"/"+engineName+"_metadata.json", engineMeta);
					logger.info("Done zipping engine metadata...");
				}
				
				// add smss file
				File smss = new File(engine.getSmssFilePath());
				logger.info("Adding smss file...");
				ZipUtils.addToZipFile(smss, zos);
				logger.info("Done adding smss file");
				logger.info("Finished creating zip");
			} catch (Exception e) {
				logger.info("Error occurred zipping up engine");
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
			// open it back up
			try {
				if(closed) {
					logger.info("Opening the engine again...");
					Utility.getDatabase(engineId);
					logger.info("Opened the engine");
				}
			} finally {
				if(lock != null) {
					// in case opening up causing an issue - we always want to unlock
					lock.unlock();
				}
			}
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
