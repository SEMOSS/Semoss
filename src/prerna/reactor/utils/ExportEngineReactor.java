package prerna.reactor.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
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
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

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
					if(includeData!=null&&includeData.equals("false")) {
					zos = ZipUtils.zipFolder(thisEngineDir, zipFilePath, ignoreDirs, 
							// ignore the current metadata file
							Arrays.asList(
									engineNameAndId+"/"+engineName+IEngine.METADATA_FILE_SUFFIX,
									engineNameAndId+"/"+"database.mv.db"
								));
					// creating reference database without data and adding it to the zip file
					createReferenceDatabase(engineId, thisEngineDir, zos, engineNameAndId);
					}else {
						zos = ZipUtils.zipFolder(thisEngineDir, zipFilePath, ignoreDirs, 
								// ignore the current metadata file
								Arrays.asList(
										engineNameAndId+"/"+engineName+IEngine.METADATA_FILE_SUFFIX
								
									));
					}
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
	
	public static void createReferenceDatabase(String engineId, String thisEngineDir, ZipOutputStream zos, String engineNameAndId) {

		IDatabaseEngine engine = Utility.getDatabase(engineId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}

		IRDBMSEngine rdbmsDb=(IRDBMSEngine) engine;
		AbstractSqlQueryUtil queryUtil = rdbmsDb.getQueryUtil();
		String database = queryUtil.getDatabase();
		String schema = queryUtil.getSchema();
		
		try {
			Connection connection = rdbmsDb.getConnection();
			DatabaseMetaData meta = connection.getMetaData();
			RdbmsTypeEnum driverEnum = rdbmsDb.getDbType();
			String catalogFilter = queryUtil.getDatabaseMetadataCatalogFilter();
			if(catalogFilter == null) {
				try {
					catalogFilter = connection.getCatalog();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			String schemaFilter = queryUtil.getDatabaseMetadataSchemaFilter();
			if(schemaFilter == null) {
				schemaFilter = rdbmsDb.getSchema();
			}
			Statement tableStmt = connection.createStatement();
			ResultSet tablesRs =RdbmsConnectionHelper.getTables(connection, tableStmt, meta, catalogFilter, schemaFilter, driverEnum);
			
			String[] tableKeys = RdbmsConnectionHelper.getTableKeys(driverEnum);
			final String TABLE_NAME_STR = tableKeys[0];
			final String TABLE_TYPE_STR = tableKeys[1];
			List<String> tables = new ArrayList<String>();
			// geting all the tables
				while (tablesRs.next()) {
					String table = tablesRs.getString(TABLE_NAME_STR);
					String tableType = tablesRs.getString(TABLE_TYPE_STR).toUpperCase();
					if(tableType.toUpperCase().contains("TABLE")) {
						tables.add(table);
					}
				}
                // creating tables
				ArrayList<String> newTables=new ArrayList<>();
				for(String t:tables) {
				// getting the table column and type
				LinkedHashMap<String, Map<String, Object>> s=queryUtil.getAllTableColumnTypes(connection, t, database, schema);
				String cols[]=new String[s.size()];
				String types[]=new String[s.size()];
				int i=0;
				for(Entry<String, Map<String, Object>> entry : s.entrySet()) {
					cols[i]=entry.getKey();
					types[i]=entry.getValue().get("DATA_TYPE").toString();
					i++;
				}
				 newTables.add(queryUtil.createTable(t, cols, types));
				}
				connection.close();
		  
			File engineFolder=new File(thisEngineDir);
		    // creating reference database folder
		   	File referenceFolder=new File(engineFolder,"reference");
		   	if(!referenceFolder.exists())
		   		referenceFolder.mkdir();
		
	    	String refDbPath=referenceFolder.getAbsolutePath()+File.separator+"database";
		    	
	    	String url="jdbc:h2:file:"+refDbPath;
		    File refFile=new File(refDbPath+".mv.db");
		    	
		   	Connection connn=DriverManager.getConnection(url, "sa", "");
		   	Statement smt=connn.createStatement();
		    	
		   	// adding the new tables to the reference database
		   	for(String table:newTables) {
	    		smt.execute(table);
	    	}
		    
		    smt.execute("SHUTDOWN");
		   	connn.close();
		    	
		   	if(!refFile.exists()) {
		   		throw new IllegalStateException("ref db was not created");
		   	}
		    		    	
	    	// adding the reference database to the zip file
	    	try {
				ZipUtils.addToZipFile(refFile, zos, engineNameAndId);
				// deleting the reference database folder
				if(referenceFolder.exists()) {
					Files.walk(referenceFolder.toPath()).map(Path::toFile).sorted((a,b)->-a.compareTo(b)).forEach(File::delete);
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException("Error occurred while adding database to the zip file. Detailed message = " + e.getMessage());
			}			
		    		    
	} catch (SQLException e) {
		classLogger.error(Constants.STACKTRACE, e);
		throw new SemossPixelException("Error occurred while creating the reference database. Detailed message = " + e.getMessage());
	}
		
}
		
	@Override
	public String getReactorDescription() {
		return "This reactor exports the model data in a zip file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "This is a required field containing the engine id of an engine";
		} else if (key.equals(INCLUDE_DATA)) {
			return "This is a required field contains the consent to include data or not";
		}
		return super.getDescriptionForKey(key);
	}
	
}
