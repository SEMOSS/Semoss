package prerna.util.specific.anthem;

import static org.quartz.JobBuilder.newJob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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
	//private static final String ZIP_DELTA = "DELTA";
	//private static final String REPORT_DELTA = "DTL";

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
		//headerString.append(DLMTR);
		//headerString.append(errorCodeColName);
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
			JobDataMap emailDataMap = TSAnomalyNotification.generateEmailJobDataMap(smtpServer, smtpPort, from, to,
					subject, body, bodyIsHtml);
			JobDetail emailJob = newJob(SendEmailJob.class).withIdentity(EMAIL_JOB_NAME, TS_ANOM_NOTIF_JOB_GROUP)
					.usingJobData(emailDataMap).build();

			// Initialize the anomaly detector
			TSAnomalyNotification.Builder tsAnomalyBuilder = new TSAnomalyNotification.Builder(timeseriesDbName,
					importRecipeString.toString(), timeseriesDateColName, timeseriesTotalColName, emailJob);

			// Add optional params if present
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
			TSAnomalyNotification generator = tsAnomalyBuilder.build();
			tsAnomNotifJobDataMap = generator.generateJobDataMap();
		}
	}

	@Override
	public void process(String fileName) {
		
		LOGGER.info("NASCO logs:::");

		try {
			Date kickoutDate = determineKickoutDate(fileName);

			// Store the number of critical errors per system
			//Map<String, Integer> nCriticalBySystem = new HashMap<String, Integer>();

			// Loop through the spreadsheets and stream the records out
			//ZipFile zipFile = new ZipFile(folderToWatch + "/" + fileName);
			//@SuppressWarnings("unchecked")
			//List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			//for (FileHeader fileHeader : fileHeaders) {
				File fileHeader = new File(folderToWatch + "\\" + fileName);
				
				String reportFileName = fileHeader.getName();
				
				// Determine if the report needs to be processed
				int extensionIndex = reportFileName.lastIndexOf(".");
				String delta = reportFileName.substring(extensionIndex - 3, extensionIndex);
				String system = reportFileName.substring(extensionIndex - 9, extensionIndex - 7);
				//String system="NA";
				
				boolean needToProcess = false;
				
				//if (ignoreSystems.contains(system)) {
				//	LOGGER.info("NASCO Will not process " + reportFileName + ": The file's source system, " + system
				//			+ ", is set to be ignored");
				//} //else if (!delta.equals(REPORT_DELTA)) {
				//	LOGGER.info("NASCO Will not process " + reportFileName + ": The file is not a delta report");
				//} 
				//else {
					LOGGER.info("NASCO Processing " + reportFileName);
					needToProcess = true;
				//}
				int nNewCritical=0;
				
				if (needToProcess) {
					// Get a reader for the file
					//InputStream stream = zipFile.getInputStream(fileHeader);
					//InputStreamReader reader = new InputStreamReader(stream);
					//BufferedReader bufferedReader = new BufferedReader(reader);
					InputStream stream = new FileInputStream(fileHeader);
					InputStreamReader reader = new InputStreamReader(stream);
					BufferedReader bufferedReader = new BufferedReader(reader);
					nNewCritical = saveToStore(bufferedReader, kickoutDate);
					//nCriticalBySystem.put(system, nNewCritical);
					//nCriticalBySystem.put(kickoutDate.toString(), nNewCritical);
				}
			//}
			putArchivableData(kickoutDate);
			//putTimeseriesData(kickoutDate, nCriticalBySystem);
			putTimeseriesData(kickoutDate, nNewCritical);
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

	protected int saveToStore(BufferedReader reader, Date kickoutDate) throws IOException {

		// Keep track of the number of critical errors as we loop through
		int nNewCritical = 0;
		try {

			// Read in the header first
			String line = reader.readLine();
			int nCol = headerAlias.length;

			// Read the file and publish
			while ((line = reader.readLine()) != null) {

				// Create the record and clean the entries
				// Sometimes when there is missing data at the end of the
				// record,
				// the raw length is less than the number of columns
//				String[] splitLine = line.split(",");
				String[] splitLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				int rawLength = splitLine.length;
				//LOGGER.info("NASCO NASCO_KO data input length is : "+splitLine.length);
				String[] rawRecord = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					if (i < rawLength) {
						rawRecord[i] = splitLine[i].trim().replaceAll(",", " ").replaceAll("\"", "");
					} else {

						// Avoids a null pointer when calculating the error code
						rawRecord[i] = "";
					}
				}

				// Determine the error code
				/*String errorCode;
				boolean critical = false;
				if (!rawRecord[nCol - 4].trim().isEmpty()) {

					// Critical
					errorCode = rawRecord[nCol - 4];
					critical = true;
				} else if (!rawRecord[nCol - 3].trim().isEmpty()) {

					// Review
					errorCode = rawRecord[nCol - 3];
				} else {

					// Informational
					errorCode = rawRecord[nCol - 2];
				}*/

				// Comma separated string with each element in quotes
				String rawRecordString = String.join(DLMTR, rawRecord);

				// Generate the record's key
				String rawKey = generateKey(rawRecordString);

				// Determine the first date
				Date firstDate = determineFirstDate(rawKey, kickoutDate);

				//LOGGER.info("NASCO process First Date is : "+firstDate);
				// The last date is always the kickout date
				Date lastDate = kickoutDate;

				//LOGGER.info("NASCO process First Date is : "+lastDate);
				
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
				//fullRecordString.append(DLMTR);
				//fullRecordString.append(errorCode);
				fullRecordString.append(NWLN);
				putRecordData(rawKey, errorKey, firstDate, lastDate, fullRecordString.toString());

				//LOGGER.info("NASCO process NASCO_KO Data is : "+fullRecordString.toString());

				// If the error is critical and this is the first time observing
				// this error, then count it as a new critical error
				//if (critical && firstDate.equals(kickoutDate)) {
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
		return nNewCritical;
	}

	protected Date determineKickoutDate(String fileName) throws ParseException {
		DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
		int dateEndIndex=fileName.lastIndexOf(".");
		LOGGER.info("NASCO Date for the File " + fileName + " is "+fileName.substring(dateEndIndex-8, dateEndIndex));
		return dateFormatter.parse(fileName.substring(dateEndIndex-8, dateEndIndex));
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
				} //else if (!fileName.substring(0, 5).equals(ZIP_DELTA)) {
				//	LOGGER.info("NASCO Will not process " + fileName + ": The file is not a delta load");
				//} 
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
		//headerString.append(String.join(DLMTR, systemAliases));
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
					options.setImportMethod(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING);
					importer.runProcessor(options);
				} else {

					// Create new db
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

	private void putTimeseriesData(Date kickoutDate, int nNewCritical) {
		/*String[] nCriticalArray = new String[systems.length];
		int total = 0;
		for (int i = 0; i < systems.length; i++) {
			if (nCriticalBySystem.containsKey(systems[i])) {
				int n = nCriticalBySystem.get(systems[i]);
				total += n;
				nCriticalArray[i] = Integer.toString(n);
			} else {
				nCriticalArray[i] = Integer.toString(0);
			}
		}*/
		StringBuilder timeseriesRowString = new StringBuilder();
		timeseriesRowString.append(QT);
		timeseriesRowString.append(dateFormatter.format(kickoutDate));
		timeseriesRowString.append(DLMTR);
		timeseriesRowString.append(nNewCritical);
		//timeseriesRowString.append(String.join(DLMTR, nCriticalArray));
		//timeseriesRowString.append(DLMTR);
		//timeseriesRowString.append(Integer.toString(total));
		timeseriesRowString.append(NWLN);
		allTimeseriesMap.put(kickoutDate, timeseriesRowString.toString());
		LOGGER.info("NASCO TIME_SERIES count for "+kickoutDate+" is "+ timeseriesRowString.toString());
		addToTimeseriesMap.put(kickoutDate, timeseriesRowString.toString());
	}

	public static void main(String[] args) {

        // TODO Auto-generated method stub

        int nNewCritical = 0;

        String header = "Enterprise_ID;NPI;Tax_Suffix;License_Suffix;First_Name;Last_Name;Middle_Name;Check_Name;Primary_Specialty_Code_1;Primary_Specialty_Code_2;Primary_Specialty_Code_3;Primary_Specialty_Code_4;Primary_Address_Type_Code;Practice;Practice_Address_Line_1;Practice_Address_Line_2;Practice_City;Practice_State;Practice_Postal_Code;Practice_Extended_Postal_Code;Practice_Effective_Date;Practice_Termination_Date;Remit_Address_Line_1;Remit_Address_Line_2;Remit_City;Remit_State;Remit_Postal_Code;Remit_Extended_Postal_Code;Network_ID;Reimbursement;Error_Code;Error_Description";
        String[] headerAlias = header.split(";");
        int nCol = headerAlias.length;
        
        // Read in the header first
//        String line = "26141939,1487720967,582420343004,52291989019,Bruce,Bosse,E,MRI AND IMAGING OF N FULTON LL,,,,,178,13D04AF6DAB322696D4B1BDFFDD04AD9,1400 HEMBREE RD STE 150,,ROSWELL,GA,30076,5711,20110101000000,20150718000000,PO BOX 932391,,ATLANTA,GA,31193,2391,HMOF,SPEC2093,H,Provider does not have a specialty (primary or secondary) tied to a Practice address";
        String line = "26894163,1265639249,811347662001,52595552002,JUAN,GELDRES,,\"MAK ANESTHESIA TCH, LLC\",P0N612VX0X,,,,178,FD07F475EE36DCB49060FF7DAEB7CFB8,201 HOSPITAL RD,,CANTON,GA,30114,2408,20110630000000,99991231000000,1635 OLD 41 HWY N STE,112-328,KENNESAW,GA,30152,4481,BVSP,SPEC7103,H,Network is active but contract is termed";
        //        int nCol = 34;

        // Read the file and publish

//        String[] splitLine = line.split(",");
        String[] splitLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        int rawLength = splitLine.length;
        // LOGGER.info("NASCO NASCO_KO data input length is :
        // "+splitLine.length);
        String[] rawRecord = new String[nCol];
        for (int i = 0; i < nCol; i++) {
              if (i < rawLength) {
                    rawRecord[i] = splitLine[i].trim();
              } else {

                    // Avoids a null pointer when calculating the error code
                    rawRecord[i] = "";
              }
        }

        for (String string : rawRecord) {
              System.out.println(string);      
        }
	}
	
}
