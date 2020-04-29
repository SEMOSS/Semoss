package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class BackupAppReactor extends AbstractReactor {

	public BackupAppReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String dbName = this.keyValue.get(this.keysToGet[0]);
		if(dbName == null || dbName.isEmpty()) {
			throw new IllegalArgumentException("Invalid database!");
		}
		
		// get engine details
		IEngine engine = Utility.getEngine(dbName);
		if (engine == null){
			throw new IllegalArgumentException("Invalid database!");
		}
		ENGINE_TYPE dbType = engine.getEngineType();
		
		// get db directory and dates for renaming the backup file
		String dbDir = DIHelper.getInstance().getProperty("BaseFolder") + DIR_SEPARATOR + "db" + DIR_SEPARATOR + dbName;
		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyy_HHmmss");
		Date date = new Date();
		String todayDate = dateFormat.format(date);
		
		// only backup if its an RDBMS or RDF
		if (dbType == IEngine.ENGINE_TYPE.RDBMS) {
			File originalFile = new File(dbDir + DIR_SEPARATOR + "database.mv.db");
			File newFile = new File(dbDir + DIR_SEPARATOR + "backup" + DIR_SEPARATOR + "database_" + todayDate + ".mv.db");
			copyFile(originalFile, newFile);
		} else if (dbType == IEngine.ENGINE_TYPE.SESAME){
			File originalFile = new File(dbDir + DIR_SEPARATOR + dbName + ".jnl");
			File newFile = new File(dbDir + DIR_SEPARATOR + "backup" + DIR_SEPARATOR + dbName + "_" + todayDate + ".jnl");
			copyFile(originalFile, newFile);
		} else {
			throw new IllegalArgumentException("Backup failed! Note: only H2 and RDF database support backups.");
		}
		return null;
	}

	/**
	 * 
	 * @param prop
	 * @param dbDir
	 * @param originalFile
	 * @param newFile
	 */
	private void copyFile(File originalFile, File newFile) {
		if (originalFile.exists()) {
			try {
				FileUtils.copyFile(originalFile, newFile);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Database backup failed! Try again.");
			}
		}else {
			throw new IllegalArgumentException("Backup failed! Note: only H2 and RDF database support backups.");
		}
	}
}
