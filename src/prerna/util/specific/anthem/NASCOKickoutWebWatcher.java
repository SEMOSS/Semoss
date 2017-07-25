package prerna.util.specific.anthem;

import static org.quartz.JobBuilder.newJob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.h2.mvstore.MVMap;
import org.json4s.FileInput;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector;
import prerna.notifications.TSAnomalyNotification;
import prerna.poi.main.helper.ImportOptions;
import prerna.quartz.JobChain;
import prerna.quartz.SendEmailJob;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import weka.core.logging.Logger;

public class NASCOKickoutWebWatcher extends AbstractKickoutWebWatcher {

	private String keyColName;
	private String firstDateColName;
	private String lastDateColName;
	private String errorCodeColName;

	private String fullHeaderString;

	private String timeseriesPropFilePath;
	private String timeseriesDbName;
	private String timeseriesDateColName;
	private String timeseriesTotalColName;

	private String[] systems;
	private String[] systemAliases;
	private Set<String> ignoreSystems;

	private boolean notify;

	private JobDataMap tsAnomNotifJobDataMap;

	private MVMap<Date, String> allTimeseriesMap;
	private MVMap<Date, String> addToTimeseriesMap;
	private MVMap<Date, String> addedToTimeseriesMap;

	private static final String ALL_TIMESERIES_MAP_NAME = "allTimeseries";
	private static final String ADD_TO_TIMESERIES_MAP_NAME = "addToTimeseries";
	private static final String ADDED_TO_TIMESERIES_MAP_NAME = "addedToTimeseries";

	private static final String PROP_KEY_IN_RDF_MAP = "NASCO_KO_Map";

	// Only load delta files
	// private static final String ZIP_DELTA = "DELTA";
	// private static final String REPORT_DELTA = "DTL";

	// For convenience
	// Just in case surround everything with quotes in case there is a
	// comma somewhere
	private static final String QT = "\"";
	private static final String DLMTR = "\",\"";
	private static final String NWLN = "\"\r\n";

	// For TS anomaly email notification
	private static final String TS_ANOM_NOTIF_JOB_GROUP = "tsAnomalyNotificationGroup";
	private static final String EMAIL_JOB_NAME = "emailJob";
	private static final String TS_ANOM_NOTIF_JOB_NAME = "tsAnomalyNotificationJob";

	public NASCOKickoutWebWatcher() {
		super(PROP_KEY_IN_RDF_MAP);
		keyColName = props.getProperty("key.column.name");
		firstDateColName = props.getProperty("first.date.column.name");
		lastDateColName = props.getProperty("last.date.column.name");
		errorCodeColName = props.getProperty("error.code.column.name");

		StringBuilder headerString = new StringBuilder();
		headerString.append(QT);
		headerString.append(keyColName);
		headerString.append(DLMTR);
		headerString.append(firstDateColName);
		headerString.append(DLMTR);
		headerString.append(lastDateColName);
		headerString.append(DLMTR);
		headerString.append(String.join(DLMTR, headerAlias));
		// headerString.append(DLMTR);
		// headerString.append(errorCodeColName);
		headerString.append(NWLN);
		fullHeaderString = headerString.toString();

		timeseriesPropFilePath = props.getProperty("time.series.prop.file.path");
		timeseriesDbName = props.getProperty("time.series.database.name");
		timeseriesDateColName = props.getProperty("time.series.date.column.name");
		timeseriesTotalColName = props.getProperty("time.series.total.column.name");

		systems = props.getProperty("time.series.systems").split(";");
		systemAliases = props.getProperty("time.series.system.aliases").split(";");
		ignoreSystems = new HashSet<String>(Arrays.asList(props.getProperty("ignore.systems", "NONE").split(";")));

		// TS anomaly notification setup
		notify = Boolean.parseBoolean(props.getProperty("notify", "false"));

		// No need to setup notifications if turned off
		if (notify) {
			// Create the import recipe for ts anomaly detection
			StringBuilder importRecipeString = new StringBuilder();
			importRecipeString.append("data.import ( api: ");
			importRecipeString.append(timeseriesDbName);
			importRecipeString.append(" . query ( [ c: ");
			importRecipeString.append(timeseriesDateColName);
			for (String systemAlias : systemAliases) {
				importRecipeString.append(" , c: Kickout_Date__" + systemAlias);
			}
			importRecipeString.append(" , c: Kickout_Date__" + timeseriesTotalColName);
			importRecipeString.append(" ] ) ) ; ");

			// Email params
			String smtpServer = props.getProperty("smtp.server");
			int smtpPort = Integer.parseInt(props.getProperty("smtp.port", "25"));
			String from = props.getProperty("from");
			String[] to = props.getProperty("to").split(";");
			String subject = props.getProperty("subject");
			String body;
			try {
				body = new String(Files.readAllBytes(Paths.get(props.getProperty("body.file"))),
						props.getProperty("body.file.encoding", "UTF-8"));
			} catch (IOException e) {
				body = "";
				e.printStackTrace();
			}
			boolean bodyIsHtml = Boolean.parseBoolean(props.getProperty("body.is.html"));

			// Create the email job
			LOGGER.info("Create the email job");
			JobDataMap emailDataMap = TSAnomalyNotification.generateEmailJobDataMap(smtpServer, smtpPort, from, to,
					subject, body, bodyIsHtml);
			JobDetail emailJob = newJob(SendEmailJob.class).withIdentity(EMAIL_JOB_NAME, TS_ANOM_NOTIF_JOB_GROUP)
					.usingJobData(emailDataMap).build();

			// Initialize the anomaly detector
			LOGGER.info("Initialize the anomaly detector");
			TSAnomalyNotification.Builder tsAnomalyBuilder = new TSAnomalyNotification.Builder(timeseriesDbName,
					importRecipeString.toString(), timeseriesDateColName, timeseriesTotalColName, emailJob);

			// Add optional params if present
			LOGGER.info("Adding optional params ");

			if (props.containsKey("aggregate.function")) {
				tsAnomalyBuilder.aggregateFunction(props.getProperty("aggregate.function"));
			}
			if (props.containsKey("max.anoms")) {
				tsAnomalyBuilder.maxAnoms(Double.parseDouble(props.getProperty("max.anoms")));
			}
			if (props.containsKey("direction")) {
				tsAnomalyBuilder.direction(
						AnomalyDetector.determineAnomDirectionFromStringDirection(props.getProperty("direction")));
			}
			if (props.containsKey("alpha")) {
				tsAnomalyBuilder.alpha(Double.parseDouble(props.getProperty("alpha")));
			}
			if (props.containsKey("period")) {
				tsAnomalyBuilder.period(Integer.parseInt(props.getProperty("period")));
			}
			if (props.containsKey("keep.existing.columns")) {
				tsAnomalyBuilder.keepExistingColumns(Boolean.parseBoolean(props.getProperty("keep.existing.columns")));
			}

			LOGGER.info("Added optional params ");
			TSAnomalyNotification generator = tsAnomalyBuilder.build();

			LOGGER.info("Starting generator");

			tsAnomNotifJobDataMap = generator.generateJobDataMap();

			LOGGER.info("started generator");
		}
	}

	@Override
	public void process(String fileName) {

		LOGGER.info("NASCO logs:::");

		try {
			Date kickoutDate = determineKickoutDate(fileName);

			// Store the number of critical errors per system
			Map<String, Integer> nCriticalBySystem = new HashMap<String, Integer>();

			File fileHeader = new File(folderToWatch + "\\" + fileName);

			String reportFileName = fileHeader.getName();

			// Determine if the report needs to be processed
			int extensionIndex = reportFileName.lastIndexOf(".");
			String delta = reportFileName.substring(extensionIndex - 3, extensionIndex);
			String system = reportFileName.substring(extensionIndex - 9, extensionIndex - 7);
			// String system="NA";

			boolean needToProcess = false;

			LOGGER.info("NASCO Processing " + reportFileName);
			needToProcess = true;
			// }
			int nNewCritical = 0;

			if (needToProcess) {
				InputStream stream = new FileInputStream(fileHeader);
				InputStreamReader reader = new InputStreamReader(stream);
				BufferedReader bufferedReader = new BufferedReader(reader);
				nCriticalBySystem = saveToStore(bufferedReader, kickoutDate);
			}

			putArchivableData(kickoutDate);
			putTimeseriesData(kickoutDate, nCriticalBySystem);

			processedMap.put(fileName, kickoutDate);

			// Commit the store now that everything has been processed
			mvStore.commit();
		} catch (ParseException e) {
			LOGGER.error("NASCO Failed to process " + fileName + ": Could not parse kickout date");
			e.printStackTrace();
		} catch (IOException e) {
			LOGGER.error("NASCO Failed to process " + fileName + ": Could not read data to the store");
			e.printStackTrace();
		}
	}

	protected Map<String, Integer> saveToStore(BufferedReader reader, Date kickoutDate) throws IOException {

		// Keep track of the number of critical errors as we loop through
		int nNewCritical = 0;
		Map<String, Integer> nCriticalBySystem = new HashMap<String, Integer>();
		int softErrorCount = 0;
		int hardErrorCount = 0;
		try {

			// Read in the header first and skip it
			String line = reader.readLine();
			int nCol = headerAlias.length; // 32

			// Read the file and publish
			while ((line = reader.readLine()) != null) {

				String[] splitLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				int rawLength = splitLine.length;

				String[] rawRecord = new String[nCol];
				for (int i = 0; i < nCol; i++) {

					if (i == 20 || i == 21) {
						rawRecord[i] = formatDate(splitLine[i].trim().replaceAll(",", " ").replaceAll("\"", ""));
					} else if (i < rawLength) {
						rawRecord[i] = splitLine[i].trim().replaceAll(",", " ").replaceAll("\"", "");
					} else {

						// Avoids a null pointer when calculating the error code
						rawRecord[i] = "";
					}
				}

				// Determine the error code
				String errorCode;
				boolean critical = false;
				if (rawRecord[nCol - 2].trim().equalsIgnoreCase("S")) {
					// LOGGER.info("NASCO error_code is "+rawRecord[nCol - 2]);
					softErrorCount += 1;
				} else {
					hardErrorCount += 1;
				}

				// Comma separated string with each element in quotes
				String rawRecordString = String.join(DLMTR, rawRecord);

				// Generate the record's key
				String rawKey = generateKey(rawRecordString);

				// Determine the first date
				Date firstDate = determineFirstDate(rawKey, kickoutDate);

				// The last date is always the kickout date
				Date lastDate = kickoutDate;

				// The error includes the first date for uniqueness
				String errorKey = generateKey(rawRecordString + dateFormatter.format(firstDate));

				// Write the full row (record and meta data)
				StringBuilder fullRecordString = new StringBuilder();
				fullRecordString.append(QT);
				fullRecordString.append(errorKey);
				fullRecordString.append(DLMTR);
				fullRecordString.append(dateFormatter.format(firstDate));
				fullRecordString.append(DLMTR);
				fullRecordString.append(dateFormatter.format(lastDate));
				fullRecordString.append(DLMTR);
				fullRecordString.append(rawRecordString);
				fullRecordString.append(NWLN);
				putRecordData(rawKey, errorKey, firstDate, lastDate, fullRecordString.toString());

				if (firstDate.equals(kickoutDate)) {
					nNewCritical += 1;
				}
			}
		} finally {
			reader.close();
		}

		// Commit the store
		mvStore.commit();

		// Return the number of critical errors for this file
		nCriticalBySystem.put("S", softErrorCount);
		nCriticalBySystem.put("H", hardErrorCount);

		return nCriticalBySystem;
	}

	protected static String formatDate(String date) {

		String finalString = "";
		String dateNumber = "";
		if (!date.isEmpty()) {
			try {
				dateNumber = Long.toString(new BigDecimal(date).longValue()).substring(0, 8);
				if (dateNumber.substring(6, 8).equals("00")) {
					return dateNumber = dateNumber.substring(0, 4) + "-" + dateNumber.substring(4, 6);
				}
				DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
				SimpleDateFormat newFormat = new SimpleDateFormat("yyyy-MM-dd");
				finalString = newFormat.format((Date) formatter.parse(dateNumber));
			} catch (Exception e) {
				return "";
			}
		}

		return finalString;
	}
	
	protected Date determineKickoutDate(String fileName) throws ParseException {
		DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
		int dateEndIndex = fileName.lastIndexOf(".");
		LOGGER.info(
				"NASCO Date for the File " + fileName + " is " + fileName.substring(dateEndIndex - 8, dateEndIndex));
		return dateFormatter.parse(fileName.substring(dateEndIndex - 8, dateEndIndex));
	}

	@Override
	protected String giveFullHeaderString() {
		return fullHeaderString;
	}

	@Override
	protected void addOther() {
		addToTimeseries();
	}

	@Override
	protected boolean needToProcess(String fileName) {
		try {

			// Right off the bat check whether it ends in the extension;
			// if a folder cannot determine the kickout date
			if (!fileName.endsWith(extension)) {
				LOGGER.info("NASCO Will not process " + fileName + ": The file is not a CSV file");
				return false;
			} else {
				Date kickoutDate = determineKickoutDate(fileName);
				boolean needToProcess = false;
				if (processedMap.containsKey(fileName)) {
					LOGGER.info("NASCO Will not process " + fileName + ": The file has already been processed");
				} else if (kickoutDate.before(ignoreBeforeDate)) {
					LOGGER.info("NASCO Will not sss " + fileName + ": The file was kicked out before "
							+ dateFormatter.format(ignoreBeforeDate));
				} // else if (!fileName.substring(0, 5).equals(ZIP_DELTA)) {
					// LOGGER.info("NASCO Will not process " + fileName + ": The
					// file is not a delta load");
					// }
				else {
					LOGGER.info("NASCO Processing " + fileName);
					needToProcess = true;
				}
				return needToProcess;
			}
		} catch (ParseException e) {
			LOGGER.info("NASCO Will not process " + fileName + ": Could not determine the kickout date");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected void openOtherMaps() {
		allTimeseriesMap = mvStore.openMap(ALL_TIMESERIES_MAP_NAME);
		addToTimeseriesMap = mvStore.openMap(ADD_TO_TIMESERIES_MAP_NAME);
		addedToTimeseriesMap = mvStore.openMap(ADDED_TO_TIMESERIES_MAP_NAME);
	}

	@Override
	protected void scheduleJobs() throws SchedulerException {
		// Nothing to schedule as of now
	}

	@Override
	protected void triggerJobs() throws SchedulerException {
		if (notify) {
			triggerTsAnomNotifJob();
		}
	}

	private void addToTimeseries() {

		// If there is nothing to add, then return
		if (addToTimeseriesMap.isEmpty()) {
			return;
		}

		String tempCsvFilePath = tempDirectory + System.getProperty("file.separator") + "timeseries_"
				+ Utility.getRandomString(10) + ".csv";
		StringBuilder headerString = new StringBuilder();
		headerString.append(QT);
		headerString.append(timeseriesDateColName);
		headerString.append(DLMTR);
		headerString.append(String.join(DLMTR, systemAliases));
		headerString.append(DLMTR);
		headerString.append(timeseriesTotalColName);
		headerString.append(NWLN);
		try {
			File tempCsv = writeToCsv(tempCsvFilePath, headerString.toString(), addToTimeseriesMap);

			ImportOptions options = generateImportOptions(tempCsvFilePath, timeseriesPropFilePath, timeseriesDbName);

			// Create new if the database does not yet exist, otherwise add to
			// existing
			ImportDataProcessor importer = new ImportDataProcessor();
			File smssFile = new File(baseDirectory + System.getProperty("file.separator") + Constants.DATABASE_FOLDER
					+ System.getProperty("file.separator") + timeseriesDbName + Constants.SEMOSS_EXTENSION);
			try {
				if (smssFile.exists()) {
					waitForEngineToLoad(timeseriesDbName);

					// Add to existing db
					LOGGER.info("in adding to the existing database");
					options.setImportMethod(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING);
					importer.runProcessor(options);
				} else {

					// Create new db
					LOGGER.info("in create new database");
					options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
					importer.runProcessor(options);
				}

				// Store the records as added
				// No need to keep the value at this point
				for (Date key : addToTimeseriesMap.keySet()) {
					addedToTimeseriesMap.put(key, "");
				}

				// Clear the add to archive map so that records are not re-added
				addToTimeseriesMap.clear();
				mvStore.commit();
			} catch (Exception e) {
				LOGGER.error("NASCO Failed to import data into " + timeseriesDbName);
				e.printStackTrace();
			}

			// Delete the temporary csv unless running in debug mode
			if (!debugMode) {
				tempCsv.delete();
			}
		} catch (IOException e) {
			LOGGER.error("NASCO Failed to write timeseries data to " + tempCsvFilePath);
			e.printStackTrace();
		}
	}

	private void triggerTsAnomNotifJob() throws SchedulerException {
		JobDetail tsAnomNotifJob = newJob(JobChain.class).withIdentity(TS_ANOM_NOTIF_JOB_NAME, TS_ANOM_NOTIF_JOB_GROUP)
				.usingJobData(tsAnomNotifJobDataMap).build();
		scheduler.addJob(tsAnomNotifJob, true, true);
		scheduler.triggerJob(tsAnomNotifJob.getKey());
	}

	// private void putTimeseriesData(Date kickoutDate, int nNewCritical) {
	private void putTimeseriesData(Date kickoutDate, Map<String, Integer> nCriticalBySystem) {
		String[] nCriticalArray = new String[systems.length];
		int total = 0;
		for (int i = 0; i < systems.length; i++) {
			if (nCriticalBySystem.containsKey(systems[i])) {
				int n = nCriticalBySystem.get(systems[i]);
				total += n;
				nCriticalArray[i] = Integer.toString(n);
			} else {
				nCriticalArray[i] = Integer.toString(0);
			}
		}
		StringBuilder timeseriesRowString = new StringBuilder();
		timeseriesRowString.append(QT);
		timeseriesRowString.append(dateFormatter.format(kickoutDate));
		timeseriesRowString.append(DLMTR);
		timeseriesRowString.append(String.join(DLMTR, nCriticalArray));
		timeseriesRowString.append(DLMTR);
		timeseriesRowString.append(Integer.toString(total));
		timeseriesRowString.append(NWLN);
		allTimeseriesMap.put(kickoutDate, timeseriesRowString.toString());
		LOGGER.info("NASCO TIME_SERIES count for " + kickoutDate + " is " + timeseriesRowString.toString());
		addToTimeseriesMap.put(kickoutDate, timeseriesRowString.toString());
	}

	@Override
	public String[] giveFiles() {

		HashMap<Integer, String> fileMap = new HashMap<Integer, String>();

		for (String file : new File(folderToWatch).list()) {

			if (file.toUpperCase().endsWith(".CSV")) {
				fileMap.put(Integer.parseInt(file.substring(file.lastIndexOf("_") + 1, file.lastIndexOf("."))), file);
			}
		}

		ArrayList<Integer> sortedKeys = new ArrayList<Integer>(fileMap.keySet());
		Collections.sort(sortedKeys);

		ArrayList<String> sortedFile = new ArrayList<String>();

		for (Integer date : sortedKeys) {
			sortedFile.add(fileMap.get(date));
		}
		// String[] sortedFileArray = new String[sortedFile.size()];
		// sortedFileArray = sortedFile.toArray(sortedFileArray);
		return sortedFile.toArray(new String[sortedFile.size()]);
	}

}
