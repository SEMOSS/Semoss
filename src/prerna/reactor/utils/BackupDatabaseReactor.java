package prerna.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.impl.SmssUtilities;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class BackupDatabaseReactor extends AbstractReactor {

	public BackupDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Invalid database!");
		}
		
		// get engine details
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if (engine == null){
			throw new IllegalArgumentException("Invalid database!");
		}
		DATABASE_TYPE dbType = engine.getDatabaseType();
		
		// get db directory and dates for renaming the backup file
		String dbDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		dbDir += DIR_SEPARATOR + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(engine.getEngineName(), databaseId);
		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyy_HHmmss");
		Date date = new Date();
		String todayDate = dateFormat.format(date);
		
		// only backup if its an RDBMS or RDF
		if (dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
			File originalFile = new File(dbDir + DIR_SEPARATOR + "database.mv.db");
			File newFile = new File(dbDir + DIR_SEPARATOR + "backup" + DIR_SEPARATOR + "database_" + todayDate + ".mv.db");
			copyFile(originalFile, newFile);
		} else if (dbType == IDatabaseEngine.DATABASE_TYPE.SESAME){
			File originalFile = new File(dbDir + DIR_SEPARATOR + databaseId + ".jnl");
			File newFile = new File(dbDir + DIR_SEPARATOR + "backup" + DIR_SEPARATOR + databaseId + "_" + todayDate + ".jnl");
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
