package prerna.poi.main;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSFlatCSVUploader extends AbstractFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSFlatCSVUploader.class.getName());
	
	/**
	 * Load a new flat table rdbms database
	 * @param smssLocation						The location of the smss file
	 * @param fileLocations						The location of the files
	 * @param customBaseURI						customBaseURI to set... need to remove this stupid thing
	 * @param owlPath							The path to the owl file
	 * @param dbName							The name of the database driver
	 * @param dbDriverType						The database type (h2, mysql, etc.)
	 * @param allowDuplicates					Boolean to determine if we should delete duplicate rows
	 * @return
	 * @throws IOException 
	 */
	public IEngine importFileWithOutConnection(String smssLocation, String fileLocations, String customBaseURI, String owlPath, String engineName, SQLQueryUtil.DB_TYPE dbDriverType, boolean allowDuplicates) throws IOException {
		/*
		 * Right now, we will assume we can only load a single csv file
		 */
		
		boolean error = false;
		queryUtil = SQLQueryUtil.initialize(dbDriverType);
		String[] files = prepareReader(fileLocations, customBaseURI, owlPath, smssLocation);
		LOGGER.setLevel(Level.WARN);
		try {
			openRdbmsEngineWithoutConnection(engineName);
			for(int i = 0; i < files.length;i++)
			{
				String fileName = files[i];
				// cause of stupid split adding empty values
				if(fileName.isEmpty()) {
					continue;
				}
				processFirstTable(fileName);
			}
			createBaseRelations();
			RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engine);
		} finally {
			if(error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
			}
		}

		return engine;
	}
	
	/**
	 * This method is used to create the csv table as a rdbms table
	 * This logic only works currently for the first csv table
	 * Also assumes that the data types are defined based on the csv helper
	 * Also assumes that no hidden columns
	 * @param fileLocation
	 */
	public void processFirstTable(String fileLocation) {
		LOGGER.info("Processing csv file: " + fileLocation);

		// use the csv file helper to load the data
		CSVFileHelper helper = new CSVFileHelper();
		// assume csv
		helper.setDelimiter(',');
		helper.parse(fileLocation);
		
		// currently not taking in the dataTypes
		// just use the values that we know will always work
		String[] headers = helper.getHeaders();
		LOGGER.info("Found headers: " + Arrays.toString(headers));
		String[] dataTypes = helper.predictTypes();
		LOGGER.info("Found data types: " + Arrays.toString(dataTypes));

		final String TABLE_NAME = "TESTING_TABLE";
		
		// generate create table statement
		StringBuilder queryBuilder = new StringBuilder("CREATE TABLE ");
		queryBuilder.append(TABLE_NAME); // TODO: will replace this with the fileName?
		queryBuilder.append(" (");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			queryBuilder.append(headers[headerIndex].toUpperCase());
			queryBuilder.append(" ");
			queryBuilder.append(dataTypes[headerIndex].toUpperCase());
			
			// add a comma if not the last index
			if(headerIndex != headers.length-1) {
				queryBuilder.append(", ");
			}
		}
		queryBuilder.append(") AS SELECT * FROM CSVREAD('");
		queryBuilder.append(fileLocation);
		queryBuilder.append("')");
		
		// load the csv as a table
		System.out.println(queryBuilder.toString());
		this.engine.insertData(queryBuilder.toString());
		
		// now we need to append an identity column for the table
		queryBuilder.setLength(0); // clear the query builder
		queryBuilder.append("ALTER TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" ADD UNIQUE_ROW_ID IDENTITY");
		
		// add the unique id for the table
		System.out.println(queryBuilder.toString());
		this.engine.insertData(queryBuilder.toString());
		
		// need to add metadata
		owler.addConcept(TABLE_NAME, "UNIQUE_ROW_ID", "LONG");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			owler.addProp(TABLE_NAME, "UNIQUE_ROW_ID", headers[headerIndex], dataTypes[headerIndex]);
		}
		
	}

	///////////////////////////////// test methods /////////////////////////////////
	
	public static void main(String[] args) throws IOException {
		TestUtilityMethods.loadDIHelper();

		// set the file
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Movie Results.csv";
		
		// set a bunch of db stuff
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		// DO NOT USE engine_name="test" since the \t on load gets interpreted as a tab :/ not sure why
		String engineName = "test";
		String customBase = "http://semoss.org/ontologies";
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", "", "" ,ImportDataProcessor.DB_TYPE.RDBMS);
		
		// do the actual db loading
		RDBMSFlatCSVUploader reader = new RDBMSFlatCSVUploader();
		String owlFile = baseFolder + "/" + propWriter.owlFile;
		reader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBase, owlFile, engineName, dbType, false);
		
		// create the smss file and drop temp file
		File propFile = new File(propWriter.propFileName);
		File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
		FileUtils.copyFile(propFile, newProp);
		newProp.setReadable(true);
		FileUtils.forceDelete(propFile);
	}
	
}
