package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.poi.main.helper.ImportOptions;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class InsightFilesToDatabaseReader {

	// keep track of base dir for engine persisting
	private String baseDirectory;
	
	// keep track of the list of new tables created
	private Set<String> newTables;
	
	public InsightFilesToDatabaseReader() {

	}
	
	public IEngine processInsightFiles(Insight in, String engineName) throws IOException {
		/*
		 * General flow
		 * 1) create a .temp file which will become the .smss
		 * 2) create a persisted engine on disk
		 * 3) create a table in that engine
		 * 4) add everything from the insight dataframe into that table
		 * 5) add single table with properties into the owl
		 * 6) make .temp into a .smss
		 */
		
		IDataMaker dm = in.getDataMaker();
		// can only convert to database if it is a ITableDataFrame
		if(!(dm instanceof ITableDataFrame)) {
			return null;
		}

		// get the base folder to create the engine db in
		this.baseDirectory = DIHelper.getInstance().getProperty("BaseFolder");
		
		// get the data frame object
		List<FileMeta> filesMeta = in.getFilesUsedInInsight();
		
		// we need to collect 2 groups of information
		// one is for csv files
		// the other is for excel files
		IEngine engine = processCsvFiles(filesMeta, engineName);
		// look for excel files 
		// if there are none, it will return the same engine being passed
		engine = processExcelFiles(filesMeta, engineName, engine);

		// return the engine
		return engine;
	}
	
	private IEngine processExcelFiles(
			List<FileMeta> filesMeta, 
			String engineName, 
			IEngine engine) 
			throws IOException
	{
		String fileLocation = "";
		List<Map<String, Map<String, String[]>>> dataTypeMapList = new Vector<Map<String, Map<String, String[]>>>();
		List<Map<String, Map<String, String>>> userDefinedHeadersList = new Vector<Map<String, Map<String, String>>>();
		for(FileMeta meta : filesMeta) {
			// this processing will only work for excel files
			if(meta.getType() != FileMeta.FILE_TYPE.EXCEL) {
				continue;
			}
			fileLocation += meta.getFileLoc() + ";";
			
			// excel loading will only store one sheet
			String sheetName = meta.getSheetName();
			
			// format the types
			Map<String, String> metaDataTypes = meta.getDataMap();
			if(metaDataTypes != null) {
				int numCols = metaDataTypes.size();
				
				// if no data types defined, skip
				if(numCols == 0) {
					Map<String, Map<String, String[]>> sheetMap = new Hashtable<String, Map<String, String[]>>();
					sheetMap.put(sheetName, new Hashtable<String, String[]>());
					dataTypeMapList.add(sheetMap);
					continue;
				}
				
				String[] headers = new String[numCols];
				String[] types = new String[numCols];
				int counter = 0;
				for(String header : metaDataTypes.keySet()) {
					headers[counter] = header;
					types[counter] = metaDataTypes.get(header);
					counter++;
				}
				Map<String, Map<String, String[]>> sheetMap = new Hashtable<String, Map<String, String[]>>();
				Map<String, String[]> dataTypes = new Hashtable<String, String[]>();
				dataTypes.put(RDBMSFlatExcelUploader.XL_HEADERS, headers);
				dataTypes.put(RDBMSFlatExcelUploader.XL_DATA_TYPES, types);
				sheetMap.put(sheetName, dataTypes);
				dataTypeMapList.add(sheetMap);
			} else {
				dataTypeMapList.add(null);
			}
			
			Map<String, String> userDefinedHeaders = meta.getNewHeaders();
			if(userDefinedHeaders != null && !userDefinedHeaders.isEmpty()) {
				Map<String, Map<String, String>> sheetUserDefinedErrorsMap = new Hashtable<String, Map<String, String>>();
				sheetUserDefinedErrorsMap.put(sheetName, userDefinedHeaders);
				userDefinedHeadersList.add(sheetUserDefinedErrorsMap);
			}
		}
		
		if(fileLocation.isEmpty()) {
			return engine;
		}
		
		boolean error = false;
		File tempPropFile = null;
		File newSmssProp = null;
		try {
			// define the uploader
			RDBMSFlatExcelUploader uploader = new RDBMSFlatExcelUploader();
			uploader.setAutoLoad(false);
			uploader.setDataTypeMapList(dataTypeMapList);
			uploader.setNewExcelHeaders(userDefinedHeadersList);
			
			// define the options for the uploader
			ImportOptions options = new ImportOptions();
			options.setRDBMSDriverType(RdbmsTypeEnum.H2_DB);
			options.setAllowDuplicates(true);	
			options.setDbName(engineName);
			options.setFileLocation(fileLocation);
			options.setBaseUrl("");
			options.setCleanString(true);
			
			String smssLocation = null;
			// if engine doesn't exist
			// create necessary files
			// and add file without connection
			if(engine == null) {
				// first write the prop file for the new engine
				PropFileWriter propWriter = new PropFileWriter();
				propWriter.setBaseDir(baseDirectory);
				propWriter.setRDBMSType(RdbmsTypeEnum.H2_DB);
				propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS);
	
				// need to go back and clean the prop writer
				smssLocation = propWriter.propFileName;
				String owlPath = baseDirectory + "/" + propWriter.owlFile;
				Map<String, String> paramHash = new Hashtable<String, String>();
				paramHash.put("engine", engineName);
				owlPath = Utility.fillParam2(owlPath, paramHash);
	
				// need to create the .temp file object before we upload so we can delete the file if an error occurs
				tempPropFile = new File(smssLocation);
				
				// set the necessary options
				options.setSMSSLocation(smssLocation);
				options.setOwlFileLocation(owlPath);
				engine = uploader.importFileWithOutConnection(options);
				// after upload, set the owl
				// make the insights database
				engine.setOWL(owlPath);
				((AbstractEngine) engine).setPropFile(propWriter.propFileName);
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.STORE, smssLocation);
			} 
			// if the engine exists
			// add file with connection
			else {
				options.setOwlFileLocation(engine.getOWL());
				uploader.importFileWithConnection(options);
			}

			// need to store the new tables so we know which columns came from where
			// when we update the data.import cvs file pkql to be data.import from the new
			// engine that was created
			this.newTables = uploader.getNewTables();
			
			// update the engine metadata within the local master and solr
			Utility.synchronizeEngineMetadata(engineName); 
			SecurityUpdateUtils.addApp(engineName);
			
			// smss location is not null when we are making a new engine
			// if it is null, there is no temp file or anything that we need to deal with
			if(smssLocation != null) {
				// only after all of this is good, should we add it to DIHelper
				DIHelper.getInstance().setLocalProperty(engineName, engine);
				String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
				engineNames = engineNames + ";" + engineName;
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
	
				// convert the .temp to .smss file
				newSmssProp = new File(smssLocation.replace("temp", "smss"));
				// we just copy over the the .temp file contents into the .smss
				FileUtils.copyFile(tempPropFile, newSmssProp);
				newSmssProp.setReadable(true);
				tempPropFile.delete();
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.STORE, smssLocation.replace("temp", "smss"));
			}
		} catch (IllegalArgumentException e) {
			error = true;
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			error = true;
			e.printStackTrace();
		} catch (IOException e) {
			error = true;
			e.printStackTrace();
		} finally {
			// something messed up
			if(error) {
				cleanUpErrors(engine, engineName, tempPropFile, newSmssProp);
			}
		}
		
		return engine;
	}

	private IEngine processCsvFiles(List<FileMeta> filesMeta, String engineName) throws IOException {
		String fileLocation = "";
		List<Map<String, String[]>> dataTypeMapList = new Vector<Map<String, String[]>>();
		Map<String, Map<String, String>> userDefinedHeadersMap = new Hashtable<String, Map<String, String>>();
		for(FileMeta meta : filesMeta) {
			// this processing will only work for csv files
			if(meta.getType() != FileMeta.FILE_TYPE.CSV) {
				continue;
			}
			fileLocation += meta.getFileLoc() + ";";
			
			// format the types
			Map<String, String> metaDataTypes = meta.getDataMap();
			if(metaDataTypes != null) {
				int numCols = metaDataTypes.size();
				
				// if no data types defined, skip
				if(numCols == 0) {
					dataTypeMapList.add(new Hashtable<String, String[]>());
					continue;
				}
				
				String[] headers = new String[numCols];
				String[] types = new String[numCols];
				int counter = 0;
				for(String header : metaDataTypes.keySet()) {
					headers[counter] = header;
					types[counter] = metaDataTypes.get(header);
					counter++;
				}
				Map<String, String[]> dataTypes = new Hashtable<String, String[]>();
				dataTypes.put(RDBMSFlatCSVUploader.CSV_HEADERS, headers);
				dataTypes.put(RDBMSFlatCSVUploader.CSV_DATA_TYPES, types);
				dataTypeMapList.add(dataTypes);
			} else {
				dataTypeMapList.add(null);
			}
			
			Map<String, String> userDefinedHeaders = meta.getNewHeaders();
			if(userDefinedHeadersMap != null && !userDefinedHeadersMap.isEmpty()) {
				userDefinedHeadersMap.put(meta.getFileLoc(), userDefinedHeaders);
			}
		}
		
		if(fileLocation.isEmpty()) {
			return null;
		}
		
		boolean error = false;
		IEngine engine = null;
		File tempPropFile = null;
		File newSmssProp = null;
		try {
			// first write the prop file for the new engine
			PropFileWriter propWriter = new PropFileWriter();
			propWriter.setBaseDir(baseDirectory);
			propWriter.setRDBMSType(RdbmsTypeEnum.H2_DB);
			propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS);

			if(fileLocation.isEmpty()) // check to see if they passed the file
				propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS);
			else // if the file location is not empty at this point use it to point to the RDBMS file - I also want some other check to say, if the user asked me to create a h2 do it
				propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS, fileLocation);
			// need to go back and clean the prop writer

			// need to go back and clean the prop writer
			String smssLocation = propWriter.propFileName;
			String owlPath = baseDirectory + "/" + propWriter.owlFile;
			Map<String, String> paramHash = new Hashtable<String, String>();
			paramHash.put("engine", engineName);
			owlPath = Utility.fillParam2(owlPath, paramHash);
			
			// need to create the .temp file object before we upload so we can delete the file if an error occurs
			tempPropFile = new File(smssLocation);
			
			ImportOptions options = new ImportOptions();
			options.setSMSSLocation(smssLocation);
			options.setDbName(engineName);
			options.setFileLocation(fileLocation);
			options.setBaseUrl("");
			options.setOwlFileLocation(owlPath);
			options.setRDBMSDriverType(RdbmsTypeEnum.H2_DB);
			options.setAllowDuplicates(true);			
			options.setCleanString(true);
			
			RDBMSFlatCSVUploader uploader = new RDBMSFlatCSVUploader();
			uploader.setAutoLoad(false);
			uploader.setDataTypeMapList(dataTypeMapList);
			uploader.setNewCsvHeaders(userDefinedHeadersMap);
			engine = uploader.importFileWithOutConnection(options);
			
			// need to store the new tables so we know which columns came from where
			// when we update the data.import cvs file pkql to be data.import from the new
			// engine that was created
			this.newTables = uploader.getNewTables();
			
			engine.setOWL(owlPath);
			((AbstractEngine) engine).setPropFile(propWriter.propFileName);
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.STORE, smssLocation);
			Utility.synchronizeEngineMetadata(engineName); // replacing this for engine
			SecurityUpdateUtils.addApp(engineName);

			// only after all of this is good, should we add it to DIHelper
			DIHelper.getInstance().setLocalProperty(engineName, engine);
			String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames + ";" + engineName;
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);

			// convert the .temp to .smss file
			newSmssProp = new File(smssLocation.replace("temp", "smss"));
			// we just copy over the the .temp file contents into the .smss
			FileUtils.copyFile(tempPropFile, newSmssProp);
			newSmssProp.setReadable(true);
			tempPropFile.delete();
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.STORE, smssLocation.replace("temp", "smss"));

		} catch (IllegalArgumentException e) {
			error = true;
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			error = true;
			e.printStackTrace();
		} catch (IOException e) {
			error = true;
			e.printStackTrace();
		} finally {
			// something messed up
			if(error) {
				cleanUpErrors(engine, engineName, tempPropFile, newSmssProp);
			}
		}
		
		return engine;
	}
	
	/**
	 * Delete the engine and any other files that were created during the process
	 * @param engine
	 * @param engineName
	 * @param tempPropFile
	 * @param newSmssProp
	 * @throws IOException
	 */
	public void cleanUpErrors(IEngine engine, String engineName, File tempPropFile, File newSmssProp) throws IOException {
		if(engine != null) {
			engine.closeDB();
		}
		// delete the engine folder and all its contents
		String engineFolderPath = baseDirectory + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName;
		File engineFolderDir = new File(engineFolderPath);
		if(engineFolderDir.exists()) {
			File[] engineFiles = engineFolderDir.listFiles();
			if(engineFiles != null) { //some JVMs return null for empty dirs
				for(File f: engineFiles) {
					try {
						FileUtils.forceDelete(f);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			try {
				FileUtils.forceDelete(engineFolderDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// delete the .temp file if it is still there
		if(tempPropFile != null) {
			tempPropFile.delete();
		}
		// delete the .smss file if it is there
		if(newSmssProp != null) {
			newSmssProp.delete();
		}
		// remove from engine from solr in case it was added
		SecurityUpdateUtils.deleteApp(engineName);
		
		throw new IOException("Error loading files from insight into database");
	}

	public Set<String> getNewTables() {
		return this.newTables;
	}
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper();

		// load in the engine
		//TODO: put in correct path and engine name for your database
		String engineName = "Movie_RDBMS";
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + engineName + ".smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(engineName);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(engineName, coreEngine);
		
		// create the pkql to add data into the insight
		String pkqlExp = "data.frame('grid'); data.import(api:Movie_RDBMS.query([c:Title , c:Genre_Updated , c:Title__Movie_Budget],([c:Title, inner.join, c:Genre_Updated])));";
		// create the insight and load the data
		Insight in = new Insight();
		in.runPkql(pkqlExp);
		
		// TODO: make sure this is a new name
		String newEngineName = "createNewEngineName";

		InsightFilesToDatabaseReader creator = new InsightFilesToDatabaseReader();
		creator.processInsightFiles(in, newEngineName);
	}
}
