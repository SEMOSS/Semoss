package prerna.util.specific.anthem;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.com.google.common.io.ByteStreams;
import org.h2.mvstore.MVMap;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.Utility;

public class WGSPKickoutWebWatcher extends AbstractKickoutWebWatcher {

	private String keyColName;
	private String firstDateColName;
	private String lastDateColName;
	private String errorCodeColName;

	private String timeseriesPropFilePath;
	private String timeseriesDbName;

	private String[] systems;

	private MVMap<Date, String> allTimeseriesMap;
	private MVMap<Date, String> addToTimeseriesMap;
	private MVMap<Date, String> addedToTimeseriesMap;

	private static final String ALL_TIMESERIES_MAP_NAME = "allTimeseries";
	private static final String ADD_TO_TIMESERIES_MAP_NAME = "addToTimeseries";
	private static final String ADDED_TO_TIMESERIES_MAP_NAME = "addedToTimeseries";

	private static final String PROP_KEY_IN_RDF_MAP = "WGSP_KO_Map";

	// Only load delta files
	private static final String ZIP_DELTA = "DELTA";
	private static final String REPORT_DELTA = "DTL";

	// For convenience
	// Just in case surround everything with quotes in case there is a
	// comma somewhere
	private static final String QT = "\"";
	private static final String DLMTR = "\",\"";
	private static final String NWLN = "\"\r\n";

	public WGSPKickoutWebWatcher() {
		super(PROP_KEY_IN_RDF_MAP);
		keyColName = props.getProperty("key.column.name");
		firstDateColName = props.getProperty("first.date.column.name");
		lastDateColName = props.getProperty("last.date.column.name");
		errorCodeColName = props.getProperty("error.code.column.name");

		timeseriesPropFilePath = props.getProperty("time.series.prop.file.path");
		timeseriesDbName = props.getProperty("time.series.database.name");

		systems = props.getProperty("time.series.systems").split(";");

		allTimeseriesMap = mvStore.openMap(ALL_TIMESERIES_MAP_NAME);
		addToTimeseriesMap = mvStore.openMap(ADD_TO_TIMESERIES_MAP_NAME);
		addedToTimeseriesMap = mvStore.openMap(ADDED_TO_TIMESERIES_MAP_NAME);
	}

	@Override
	public void process(String fileName) {
		try {
			Date kickoutDate = determineKickoutDate(fileName);

			// Store the number of critical errors per system
			Map<String, Integer> nCriticalBySystem = new HashMap<String, Integer>();

			// Loop through the spreadsheets and stream the records out
			ZipFile zipFile = new ZipFile(folderToWatch + "/" + fileName);
			@SuppressWarnings("unchecked")
			List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			for (FileHeader fileHeader : fileHeaders) {

				// Only process delta reports
				String reportFileName = fileHeader.getFileName();
				int extensionIndex = reportFileName.lastIndexOf(".");
				String system = reportFileName.substring(extensionIndex - 9, extensionIndex - 7);
				if (reportFileName.substring(extensionIndex - 3, extensionIndex).equals(REPORT_DELTA)) {

					// Get a reader for the file
					InputStream stream = zipFile.getInputStream(fileHeader);
					byte[] data = ByteStreams.toByteArray(stream);
					InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(data));
					BufferedReader bufferedReader = new BufferedReader(reader);
					int nCritical = saveToStore(bufferedReader, kickoutDate);
					nCriticalBySystem.put(system, nCritical);
				} else {
					LOGGER.info("Will not process " + reportFileName + ": The file is not a delta report");
				}
			}
			putArchivableData(kickoutDate);
			putTimeseriesData(kickoutDate, nCriticalBySystem);
			processedMap.put(fileName, kickoutDate);

			// Commit the store now that everything has been processed
			mvStore.commit();
		} catch (ParseException e) {
			LOGGER.error("Failed to process " + fileName + ": Could not parse kickout date");
			e.printStackTrace();
		} catch (ZipException e) {
			LOGGER.error("Failed to process " + fileName + ": Could not unzip");
			e.printStackTrace();
		} catch (IOException e) {
			LOGGER.error("Failed to process " + fileName + ": Could not read data to the store");
			e.printStackTrace();
		}
	}

	protected int saveToStore(BufferedReader reader, Date kickoutDate) throws IOException {

		// Keep track of the number of critical errors as we loop through
		int nCritical = 0;
		try {

			// Start by writing the header to the csv
			String line = reader.readLine();
			String[] header = line.split("\t");
			int nCol = header.length;
			for (int i = 0; i < nCol; i++) {
				header[i] = header[i].trim();
			}

			// Write the header
			StringBuilder headerString = new StringBuilder();
			headerString.append(QT);
			headerString.append(keyColName);
			headerString.append(DLMTR);
			headerString.append(firstDateColName);
			headerString.append(DLMTR);
			headerString.append(lastDateColName);
			headerString.append(DLMTR);
			headerString.append(String.join(DLMTR, header));
			headerString.append(DLMTR);
			headerString.append(errorCodeColName);
			headerString.append(NWLN);
			metaMap.put(HEADER_KEY, headerString.toString());

			// Read the file and publish
			while ((line = reader.readLine()) != null) {

				// Create the record and clean the entries
				// Sometimes when there is missing data at the end of the
				// record,
				// the raw length is less than the number of columns
				String[] splitLine = line.split("\t");
				int rawLength = splitLine.length;
				String[] rawRecord = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					if (i < rawLength) {
						rawRecord[i] = splitLine[i].trim();
					} else {

						// Avoids a null pointer when calculating the error code
						rawRecord[i] = "";
					}
				}

				// Determine the error code
				String errorCode;
				if (!rawRecord[nCol - 4].trim().isEmpty()) {

					// Critical
					errorCode = rawRecord[nCol - 4];
					nCritical += 1;
				} else if (!rawRecord[nCol - 3].trim().isEmpty()) {

					// Review
					errorCode = rawRecord[nCol - 3];
				} else {

					// Informational
					errorCode = rawRecord[nCol - 2];
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
				fullRecordString.append(DLMTR);
				fullRecordString.append(errorCode);
				fullRecordString.append(NWLN);
				putRecordData(rawKey, errorKey, firstDate, lastDate, fullRecordString.toString());
			}
		} finally {
			reader.close();
		}

		// Commit the store
		mvStore.commit();

		// Return the number of critical errors for this file
		return nCritical;
	}

	protected Date determineKickoutDate(String fileName) throws ParseException {
		DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormatter.parse(fileName.substring(12, 22));
	}

	@Override
	protected boolean needToProcess(String fileName) {
		try {
			Date kickoutDate = determineKickoutDate(fileName);
			boolean needToProcess = false;
			if (processedMap.containsKey(fileName)) {
				LOGGER.info("Will not process " + fileName + ": The file has already been processed");
			} else if (kickoutDate.before(ignoreBeforeDate)) {
				LOGGER.info("Will not process " + fileName + ": The file was kicked out before "
						+ dateFormatter.format(ignoreBeforeDate));
			} else if (!fileName.endsWith(extension)) {
				LOGGER.info("Will not process " + fileName + ": The file is not a zip file");
			} else if (!fileName.substring(0, 5).equals(ZIP_DELTA)) {
				LOGGER.info("Will not process " + fileName + ": The file is not a delta load");
			} else {
				LOGGER.info("Processing " + fileName);
				needToProcess = true;
			}
			return needToProcess;
		} catch (ParseException e) {
			LOGGER.info("Will not process " + fileName + ": Could not determine the kickout date");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected void addOther() {
		addToTimeseries();
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
		headerString.append(String.join(DLMTR, systems));
		headerString.append(DLMTR);
		headerString.append("Total");
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
				addedToTimeseriesMap.putAll(addToTimeseriesMap);

				// Clear the add to archive map so that records are not re-added
				addToTimeseriesMap.clear();
				mvStore.commit();
			} catch (Exception e) {
				LOGGER.error("Failed to import data into " + timeseriesDbName);
				e.printStackTrace();
			}

			// Delete the temporary csv unless running in debug mode
			if (!debugMode) {
				tempCsv.delete();
			}
		} catch (IOException e) {
			LOGGER.error("Failed to write timeseries data to " + tempCsvFilePath);
			e.printStackTrace();
		}
	}

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
		timeseriesRowString.append(String.join(DLMTR, nCriticalArray));
		timeseriesRowString.append(DLMTR);
		timeseriesRowString.append(Integer.toString(total));
		timeseriesRowString.append(NWLN);
		allTimeseriesMap.put(kickoutDate, timeseriesRowString.toString());
		addToTimeseriesMap.put(kickoutDate, timeseriesRowString.toString());
	}

}
