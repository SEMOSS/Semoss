package prerna.reactor.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class ExportEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExportEngineReactor.class);
	private static final String CLASS_NAME = ExportEngineReactor.class.getName();

	private String keepGit = "keepGit";
	private static final String INCLUDE_DATA = "includeData";

	public ExportEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), keepGit , INCLUDE_DATA };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		boolean keepGit = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]));
		String includeData = this.keyValue.get(this.keysToGet[2]);

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
		
		String engineName = engine.getEngineName();
		String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);
		String outputDir = this.insight.getInsightFolder();
		String thisEngineDir = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engineId, engineName);
		File thisEngineF = new File(thisEngineDir);
		String zipFilePath = outputDir + "/" + engineNameAndId + "_engine.zip";

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
			
			// determine if we keep or ignore the git
			List<String> ignoreDirs = new ArrayList<>();
			if(!keepGit) {
				ignoreDirs.add(engineNameAndId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/.git");
			}
			
			// zip engine
			FileOutputStream fos = null;
			ZipOutputStream zos = null;
			try {
				// zip engine folder
				if(thisEngineF.exists()) {
					logger.info("Zipping engine files...");
					// now zip up
					zos = ZipUtils.zipFolder(thisEngineDir, zipFilePath, ignoreDirs, 
							// ignore the current metadata file
							Arrays.asList(
									engineNameAndId+"/"+engineName+IEngine.METADATA_FILE_SUFFIX
								));
					logger.info("Done zipping engine folder");
				} else {
					logger.info("No engine folder to zip");
					fos = new FileOutputStream(zipFilePath);
					zos = new ZipOutputStream(fos);
				}
				
				// zip up the engine metadata
				{
					logger.info("Grabbing engine metadata to write to temporary file to zip...");
					Map<String, Object> engineMeta = SecurityEngineUtils.getAggregateEngineMetadata(engineId, null, false);
					ZipUtils.zipObjectToFile(zos, engineNameAndId, outputDir+"/"+engineName+IEngine.METADATA_FILE_SUFFIX, engineMeta);
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
				try {
					if (fos != null) {
						fos.close();
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
					Utility.getEngine(engineId);
					logger.info("Opened the engine");
				}
			} finally {
				if(lock != null) {
					// in case opening up causing an issue - we always want to unlock
					lock.unlock();
				}
			}
		}
		
		//clear the data from database and export it while schema remains the same
		if(includeData!=null&&includeData.equals("false")) {
			try {
				clearDatabaseInZip(zipFilePath);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
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
	
	public static void clearDatabaseInZip(String zipFilePath) throws Exception {
        File zipFile = new File(zipFilePath);
 
        // checking whether database.mv.db is present or not
        String dbEntryPath = null;
        try (ZipFile zf = new ZipFile(zipFile)) {
            Enumeration<? extends ZipEntry> entries = zf.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.getName().contains("/database.mv.db")) {
                    dbEntryPath = entry.getName();
                    break;
                }
            }
        }
        if (dbEntryPath == null) {
            throw new FileNotFoundException("database.mv.db not found inside " + zipFilePath);
        }
 
        File tempDb = File.createTempFile("h2db", ".mv.db");
        tempDb.deleteOnExit();
 
        try (ZipFile zf = new ZipFile(zipFile)) {
            ZipEntry dbEntry = zf.getEntry(dbEntryPath);
            try (InputStream is = zf.getInputStream(dbEntry);
                 OutputStream os = new FileOutputStream(tempDb)) {
                is.transferTo(os);
            }
        }
 
        String dbUrl = "jdbc:h2:" + tempDb.getAbsolutePath().replace(".mv.db", "");
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "")) {
            conn.setAutoCommit(false);
            
            // extracting all the tables from database
            List<String> tables = new ArrayList<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_TYPE='TABLE'")) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            
            // removing all the data from the table while schema remains the same
            try (Statement stmt = conn.createStatement()) {
                for (String table : tables) {
                    stmt.executeUpdate("TRUNCATE TABLE " + table);
                }
            }
 
            conn.commit();
        }
 
        Path tempZipPath = Files.createTempFile("updatedZip", ".zip");
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
             ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tempZipPath.toFile()))) {
 
            ZipEntry entry;
            byte[] buffer = new byte[4096];
            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                if (name.equals(dbEntryPath)) {
                	// putting the updated files
                    zos.putNextEntry(new ZipEntry(dbEntryPath));
                    Files.copy(tempDb.toPath(), zos);
                    zos.closeEntry();
                } else {
                	// putting the unchanged files
                    zos.putNextEntry(new ZipEntry(name));
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        zos.write(buffer, 0, len);
                    }
                    zos.closeEntry();
                }
            }
        }
 
        Files.move(tempZipPath, zipFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        
    }
	
}
