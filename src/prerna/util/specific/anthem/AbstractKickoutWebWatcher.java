package prerna.util.specific.anthem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.AbstractFileWatcher;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public abstract class AbstractKickoutWebWatcher extends AbstractFileWatcher {

	protected String baseDirectory;
	protected Properties props;
	protected boolean debugMode;
	protected String tempDirectory;

	protected DateFormat dateFormatter;

	protected MVStore mvStore;
	protected MVMap<String, Date> processedMap;
	protected MVMap<String, String> allRecordsMap;
	protected MVMap<String, String> currentViewMap;
	protected MVMap<String, String> addToArchiveMap;
	protected MVMap<String, String> addedToArchiveMap;
	protected MVMap<String, Date> rawKeyFirstDateMap;
	protected MVMap<String, Date> rawKeyLastDateMap;
	protected MVMap<String, Date> errorKeyFirstDateMap;
	protected MVMap<String, Date> errorKeyLastDateMap;

	protected int considerNewThresholdDays;
	protected int currentViewDays;

	protected Date ignoreBeforeDate;

	protected String[] headerAlias;

	private String currentDbName;
	private String archiveDbName;
	private String propFilePath;
	private SQLQueryUtil.DB_TYPE dbType;
	private MessageDigest hasher;
	private String encoding;
	private int processDelay;

	private static final String PROCESSED_MAP_NAME = "processed";
	private static final String ALL_RECORDS_MAP_NAME = "allRecords";
	private static final String CURRENT_VIEW_MAP_NAME = "currentView";
	private static final String ADD_TO_ARCHIVE_MAP_NAME = "addToArchive";
	private static final String ADDED_TO_ARCHIVE_MAP_NAME = "addedToArchive";
	private static final String RAW_KEY_FIRST_DATE_MAP_NAME = "rawKeyFirstDate";
	private static final String RAW_KEY_LAST_DATE_MAP_NAME = "rawKeyLastDate";
	private static final String ERROR_KEY_FIRST_DATE_MAP_NAME = "errorKeyFirstDate";
	private static final String ERROR_KEY_LAST_DATE_MAP_NAME = "errorKeyLastDate";

	private final static char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

	protected static final Logger LOGGER = LogManager.getLogger(AbstractKickoutWebWatcher.class.getName());

	public AbstractKickoutWebWatcher(String propFileKeyInRDFMap) {
		baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

		// Load specific properties
		props = new Properties();
		String propFile = DIHelper.getInstance().getProperty(propFileKeyInRDFMap);
		try (InputStream propStream = new FileInputStream(propFile)) {
			props.load(propStream);
		} catch (IOException e) {
			LOGGER.error("Failed to load properties from " + propFile);
			e.printStackTrace();
		}
		debugMode = Boolean.parseBoolean(props.getProperty("debug.mode", "false"));
		if (debugMode) {
			LOGGER.setLevel(Level.DEBUG);
		}
		tempDirectory = props.getProperty("temp.directory", baseDirectory);
		dateFormatter = new SimpleDateFormat(props.getProperty("date.format", "yyyy-MM-dd"));

		// Don't auto commit
		mvStore = new MVStore.Builder().fileName(props.getProperty("mv.store")).autoCommitDisabled().open();

		// Open all the maps
		processedMap = mvStore.openMap(PROCESSED_MAP_NAME);
		allRecordsMap = mvStore.openMap(ALL_RECORDS_MAP_NAME);
		currentViewMap = mvStore.openMap(CURRENT_VIEW_MAP_NAME);
		addToArchiveMap = mvStore.openMap(ADD_TO_ARCHIVE_MAP_NAME);
		addedToArchiveMap = mvStore.openMap(ADDED_TO_ARCHIVE_MAP_NAME);
		rawKeyFirstDateMap = mvStore.openMap(RAW_KEY_FIRST_DATE_MAP_NAME);
		rawKeyLastDateMap = mvStore.openMap(RAW_KEY_LAST_DATE_MAP_NAME);
		errorKeyFirstDateMap = mvStore.openMap(ERROR_KEY_FIRST_DATE_MAP_NAME);
		errorKeyLastDateMap = mvStore.openMap(ERROR_KEY_LAST_DATE_MAP_NAME);

		considerNewThresholdDays = Integer.parseInt(props.getProperty("consider.new.threshold.days", "7"));
		currentViewDays = Integer.parseInt(props.getProperty("current.view.days", "7"));

		try {
			ignoreBeforeDate = dateFormatter.parse(props.getProperty("ignore.before.date", "0001-01-01"));
		} catch (ParseException e) {
			LOGGER.error("Failed to parse the ignore before date");
			e.printStackTrace();
		}

		headerAlias = props.getProperty("header.alias").split(";");

		currentDbName = props.getProperty("database.name");
		archiveDbName = props.getProperty("archive.database.name");

		propFilePath = props.getProperty("prop.file.path");

		// Determine the database type
		String dbTypeString = props.getProperty("database.type", "H2");
		if (dbTypeString.equals("MariaDB")) {
			dbType = SQLQueryUtil.DB_TYPE.MARIA_DB;
		} else if (dbTypeString.equals("SQLServer")) {
			dbType = SQLQueryUtil.DB_TYPE.SQL_Server;
		} else if (dbTypeString.equals("MySQL")) {
			dbType = SQLQueryUtil.DB_TYPE.MySQL;
		} else if (dbTypeString.equals("Oracle")) {
			dbType = SQLQueryUtil.DB_TYPE.Oracle;
		} else {
			dbType = SQLQueryUtil.DB_TYPE.H2_DB;
		}

		try {
			hasher = MessageDigest.getInstance(props.getProperty("hashing.algorithm", "SHA-1"));
		} catch (NoSuchAlgorithmException e) {
			LOGGER.error("Failed to instantiate the hasher for key generation");
			e.printStackTrace();
		}

		encoding = props.getProperty("encoding", "UTF-8");

		processDelay = Integer.parseInt(props.getProperty("process.delay.seconds", "30"));
	}

	@Override
	public void loadFirst() {
		String[] fileNames = new File(folderToWatch).list();
		for (String fileName : fileNames) {
			if (needToProcess(fileName)) {
				process(fileName);
			}
		}
		addToArchive();
		refreshCurrentView();
		// addOther();
	}

	private void addToArchive() {

		// If there is nothing to add, then return
		if (addToArchiveMap.isEmpty()) {
			return;
		}

		String tempCsvFilePath = tempDirectory + System.getProperty("file.separator") + "archive_"
				+ Utility.getRandomString(10) + ".csv";
		try {
			File tempCsv = writeToCsv(tempCsvFilePath, giveFullHeaderString(), addToArchiveMap);
			ImportOptions options = generateImportOptions(tempCsvFilePath, propFilePath, archiveDbName);

			// Create new if the database does not yet exist, otherwise add to
			// existing
			ImportDataProcessor importer = new ImportDataProcessor();
			File smssFile = new File(baseDirectory + System.getProperty("file.separator") + Constants.DATABASE_FOLDER
					+ System.getProperty("file.separator") + archiveDbName + Constants.SEMOSS_EXTENSION);
			try {
				if (smssFile.exists()) {
					waitForEngineToLoad(archiveDbName);

					// Add to existing db
					options.setImportMethod(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING);
					importer.runProcessor(options);
				} else {

					// Create new db
					options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
					importer.runProcessor(options);
				}

				// Store the records as added
				addedToArchiveMap.putAll(addToArchiveMap);

				// Clear the add to archive map so that records are not re-added
				addToArchiveMap.clear();
				mvStore.commit();
			} catch (Exception e) {
				LOGGER.error("Failed to import data into " + archiveDbName);
				e.printStackTrace();
			}

			// Delete the temporary csv unless running in debug mode
			if (!debugMode) {
				tempCsv.delete();
			}
		} catch (IOException e) {
			LOGGER.error("Failed to write archive data to " + tempCsvFilePath);
			e.printStackTrace();
		}
	}

	// TODO need to wipe the insight cache!
	// TODO do this is a job so that it doesn't get wiped in the middle of the
	// day!
	private void refreshCurrentView() {
		currentViewMap.clear();
		Date today = new Date();
		for (String errorKey : allRecordsMap.keySet()) {
			Date lastDate = errorKeyLastDateMap.get(errorKey);
			long daysSinceLastDate = TimeUnit.DAYS.convert(today.getTime() - lastDate.getTime(), TimeUnit.MILLISECONDS);
			if (daysSinceLastDate < currentViewDays) {
				currentViewMap.put(errorKey, allRecordsMap.get(errorKey));
			}
		}
		mvStore.commit();
		String tempCsvFilePath = tempDirectory + System.getProperty("file.separator") + "current_"
				+ Utility.getRandomString(10) + ".csv";

		ImportOptions options = generateImportOptions(tempCsvFilePath, propFilePath, currentDbName);
		try {
			File tempCsv = writeToCsv(tempCsvFilePath, giveFullHeaderString(), currentViewMap);
			File smssFile = new File(baseDirectory + System.getProperty("file.separator") + Constants.DATABASE_FOLDER
					+ System.getProperty("file.separator") + currentDbName + Constants.SEMOSS_EXTENSION);
			if (smssFile.exists()) {
				waitForEngineToLoad(currentDbName);
				IEngine currentDb = Utility.getEngine(currentDbName);

				// Truncate all the tables in the current db
				// Since a significant proportion of records need the last date
				// updated anyway
				String tablesQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC';";
				@SuppressWarnings("unchecked")
				Map<String, Object> result = (Map<String, Object>) currentDb.execQuery(tablesQuery);
				try (Statement statement = (Statement) result.get(RDBMSNativeEngine.STATEMENT_OBJECT)) {
					try (ResultSet rs = (ResultSet) result.get(RDBMSNativeEngine.RESULTSET_OBJECT)) {
						Set<String> tables = new HashSet<String>();
						while (rs.next()) {
							tables.add(rs.getString(1));
						}
						statement.execute("SET REFERENTIAL_INTEGRITY FALSE;");
						for (String table : tables) {
							statement.executeUpdate("TRUNCATE TABLE " + table + ";");
						}
						statement.execute("SET REFERENTIAL_INTEGRITY TRUE;");
					}
				} catch (SQLException e) {
					LOGGER.error("Failed to truncate tables in " + currentDbName);
					e.printStackTrace();
				}

				// Add to existing db
				options.setImportMethod(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING);
				ImportDataProcessor importer = new ImportDataProcessor();
				try {
					importer.runProcessor(options);
				} catch (Exception e) {
					LOGGER.error("Failed to import data into " + currentDbName);
					e.printStackTrace();
				}
			} else {

				// Create new db
				options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
				ImportDataProcessor importer = new ImportDataProcessor();
				try {
					importer.runProcessor(options);
				} catch (Exception e) {
					LOGGER.error("Failed to import data into " + currentDbName);
					e.printStackTrace();
				}
			}

			// Delete the temporary csv unless running in debug mode
			if (!debugMode) {
				tempCsv.delete();
			}
		} catch (IOException e) {
			LOGGER.error("Failed to write current view data to " + tempCsvFilePath);
			e.printStackTrace();
		}
	}

	// Implementation specific methods

	protected abstract String giveFullHeaderString();

	protected abstract void addOther();

	protected abstract boolean needToProcess(String fileName);

	// Helper methods

	// Specify the options necessary to load data from the csv file into SEMOSS
	protected ImportOptions generateImportOptions(String tempCsvFilePath, String propFilePath, String engineName) {
		ImportOptions options = new ImportOptions();
		options.setDbName(engineName);
		options.setImportType(ImportOptions.IMPORT_TYPE.CSV);
		options.setRDBMSDriverType(dbType);
		options.setDbType(ImportOptions.DB_TYPE.RDBMS);
		options.setBaseFolder(baseDirectory);
		options.setAutoLoad(false); // TODO what does this mean?
		options.setAllowDuplicates(true); // Already de-duplicated
		options.setFileLocation(tempCsvFilePath);
		options.setPropertyFiles(propFilePath);
		options.setCreateIndexes(true);
		return options;
	}

	// Helper methods only used by subclasses

	// Determine the first date
	// If existing and not considered new, then use the cached first date
	// Otherwise it is the kickout date
	protected Date determineFirstDate(String rawKey, Date kickoutDate) {
		Date firstDate = kickoutDate;
		boolean existing = rawKeyFirstDateMap.containsKey(rawKey);
		if (existing) {
			Date cachedFirstDate = rawKeyFirstDateMap.get(rawKey);
			Date cachedLastDate = rawKeyLastDateMap.get(rawKey);
			long daysSinceLastObserved = TimeUnit.DAYS.convert(kickoutDate.getTime() - cachedLastDate.getTime(),
					TimeUnit.MILLISECONDS);
			boolean considerNew = daysSinceLastObserved > considerNewThresholdDays;
			if (!considerNew) {
				firstDate = cachedFirstDate;
			}
		}
		return firstDate;
	}

	protected void putRecordData(String rawKey, String errorKey, Date firstDate, Date lastDate, String fullRecord) {
		allRecordsMap.put(errorKey, fullRecord);
		rawKeyFirstDateMap.put(rawKey, firstDate);
		rawKeyLastDateMap.put(rawKey, lastDate);
		errorKeyFirstDateMap.put(errorKey, firstDate);
		errorKeyLastDateMap.put(errorKey, lastDate);
	}

	// For all the records that have not yet been added to the archive,
	// check whether they should be archived
	protected void putArchivableData(Date kickoutDate) {
		Set<String> unarchivedRecords = new HashSet<String>();
		unarchivedRecords.addAll(allRecordsMap.keySet());
		unarchivedRecords.removeAll(addToArchiveMap.keySet());
		unarchivedRecords.removeAll(addedToArchiveMap.keySet());
		for (String errorKey : unarchivedRecords) {
			Date lastDate = errorKeyLastDateMap.get(errorKey);
			long daysSinceLastObserved = TimeUnit.DAYS.convert(kickoutDate.getTime() - lastDate.getTime(),
					TimeUnit.MILLISECONDS);

			// Archive when there is no longer the possibility of the record
			// being updated
			boolean archive = daysSinceLastObserved > considerNewThresholdDays;
			if (archive) {
				addToArchiveMap.put(errorKey, allRecordsMap.get(errorKey));
			}
		}
	}

	protected String generateKey(String text) throws UnsupportedEncodingException {
		byte[] hash = hasher.digest(text.getBytes(encoding));
		return bytesToHexString(hash);
	}

	// Static helper methods

	protected static File writeToCsv(String csvFilePath, String header, MVMap<?, String> map) throws IOException {
		File csv = new File(csvFilePath);
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csv)))) {
			writer.write(header);
			for (Object key : map.keySet()) {
				writer.write(map.get(key));
			}
		}
		return csv;
	}

	// If the database exists, then wait for SEMOSS to load the engine
	// Otherwise will throw an error when trying to add to existing database
	protected static void waitForEngineToLoad(String engineName) {
		boolean loadingEngine = true;
		while (loadingEngine) {
			LOGGER.debug("Loading the engine " + engineName);
			IEngine engine = Utility.getEngine(engineName);
			if (engine == null) {
				try {
					Thread.sleep(10000); // Wait 10s
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				loadingEngine = false;
				LOGGER.debug("The engine " + engineName + " has loaded");
			}
		}
	}

	protected static String bytesToHexString(byte[] bytes) {

		// See http://stackoverflow.com/q/9655181
		char[] hexChars = new char[bytes.length * 2];
		for (int i = 0; i < bytes.length; i++) {
			int v = bytes[i] & 0xFF;
			hexChars[i * 2] = HEX_ARRAY[v >>> 4];
			hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	// TODO clean up this code take from AbstractFileWatcher
	@SuppressWarnings({ "unused", "rawtypes" })
	@Override
	public void run() {

		loadFirst();

		// Code taken from AbstractFileWatcher
		try {
			WatchService watcher = FileSystems.getDefault().newWatchService();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			// Path dir2Watch = Paths.get(baseFolder + "/" + folderToWatch);

			Path dir2Watch = Paths.get(folderToWatch);

			WatchKey key = dir2Watch.register(watcher, StandardWatchEventKinds.ENTRY_CREATE,
					StandardWatchEventKinds.ENTRY_MODIFY);
			while (true) {
				// WatchKey key2 = watcher.poll(1, TimeUnit.MINUTES);
				WatchKey key2 = watcher.take();

				for (WatchEvent<?> event : key2.pollEvents()) {
					WatchEvent.Kind kind = event.kind();
					if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
						String newFile = event.context() + "";
						if (newFile.endsWith(extension)) {
							// Allow a delay in case the file is still being
							// held to save
							Thread.sleep(processDelay * 1000);
							try {
								if (needToProcess(newFile)) {
									process(newFile);

									// TODO
									// TODO
									// TODO schedule these as jobs
									// That way it doesn't run in the middle of
									// the day
									addToArchive();
									refreshCurrentView();
									// addOther();
								}

							} catch (RuntimeException ex) {
								ex.printStackTrace();
							}
						} else
							LOGGER.info("Ignoring File " + newFile);
					}
				}
				key2.reset();
			}
		} catch (RuntimeException ex) {
			LOGGER.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (InterruptedException ex) {
			LOGGER.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (IOException ex) {
			LOGGER.debug(ex);
			// do nothing - I will be working it in the process block
		}
		// End code taken from AbstractFileWatcher
	}

}
