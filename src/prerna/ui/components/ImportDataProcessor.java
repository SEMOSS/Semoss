/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.poi.main.helper.ImportOptions;
import prerna.util.Utility;

public class ImportDataProcessor {
	private static final Logger logger = LogManager.getLogger(ImportDataProcessor.class.getName());
	private String baseDirectory;

	/**
	 * Centralized point for performing a database upload
	 * 
	 * @param options ImportOptions object containing all the necessary data for the
	 *                upload
	 * @throws Exception
	 */
	public void runProcessor(ImportOptions options) throws Exception {
		ImportOptions.IMPORT_METHOD importMethod = options.getImportMethod();
		String baseDir = Utility.normalizePath( options.getBaseFolder());

		// check for basic info that is required.. if not present, throw an error right
		// away
		String errorMessage = null;
		if (importMethod == null) {
			errorMessage = "Import method is not specified";
			throw new IOException(errorMessage);
		}
		if (baseDir == null || baseDir.isEmpty() || !(new File(baseDir).isDirectory())) {
			errorMessage = "Defined base directory does not exist";
			throw new IOException(errorMessage);
		}
		// set the base directory as it is used in other methods when generating files
		this.baseDirectory = baseDir;

//		// separation of methods based on import type
//		switch (importMethod) {
//		case CREATE_NEW:
//			createNewDb(options);
//			break;
//		case ADD_TO_EXISTING:
//			addToExistingDb(options);
//			break;
////		case OVERRIDE : overrideExistingDb(options); break;
//		case CONNECT_TO_EXISTING_RDBMS:
//			createNewDb(options);
//			break; // TODO: see if this should be refactored into create_new
//		default:
//			errorMessage = "Import method, " + importMethod + ", is not supported.";
//			throw new IOException(errorMessage);
//
//		}
	}

	/**
	 * Creating a new database
	 * 
	 * @param options ImportOptions object containing all the necessary data for the
	 *                upload
	 * @throws IOException
	 */
//	private void createNewDb(ImportOptions options) throws IOException {
//		IEngine engine = null;
//		ImportOptions.DB_TYPE dbType = options.getDbType();
//		String engineName = options.getDbName();
//		String engineID = options.getEngineID();
//		options.getBaseUrl();
//		// autoLoad = false ---> means we will close the engine and then start it up
//		// again using the smss file...
//		Boolean autoLoad = (options.isAutoLoad() != null) ? options.isAutoLoad() : true;
//
//		checkImportOptions(options);// perform checks on data being passed, if missing info, throws Exception
//
//		File propFile = null;// file object for .temp file
//		File newProp = null;// file object for .smss file
//
//		// keep track of any errors that might occur
//		boolean error = false;
//
//		try {
//			// first write the prop file for the new engine
//			PropFileWriter propWriter = runPropWriter(options);
//			// TODO: grabbing these values from the prop writer is pretty crap...
//			// need to go back and clean the prop writer
//			String smssLocation = propWriter.propFileName;
//			options.setSMSSLocation(smssLocation);
//
//			String owlPath = baseDirectory + "/" + propWriter.owlFile;
//			Hashtable<String, String> paramHash = new Hashtable<>();
//			paramHash.put("ENGINE", SmssUtilities.getUniqueName(engineName, engineID));
//			owlPath = Utility.fillParam2(owlPath, paramHash);
//			options.setOwlFileLocation(owlPath);
//
//			// need to create the .temp file object before we upload so we can delete the
//			// file if an error occurs
//			propFile = new File(Utility.normalizePath(smssLocation));
//
//			// then process based on what type of database - RDF or RDBMS
//			switch (dbType) {
//			case RDF:
//				engine = createNewRDF(options);
//				break;
//			case TINKER:
//				engine = createNewTinker(options);
//				break;
//			case RDBMS:
//				engine = createNewRDBMS(options);
//				break;
//			default:
//				String errorMessage = "Database type, " + dbType + ", is not recognized as a valid format";
//				throw new IOException(errorMessage);
//			}
//
//			engine.setEngineId(engineID);
//			// if not auto load... i.e. manually load here into DIHelper
//			if (!autoLoad) {
//				engine.setOWL(owlPath);
//				if (dbType == ImportOptions.DB_TYPE.RDBMS) {
//					((AbstractEngine) engine).setPropFile(propWriter.propFileName);
//				}
//				DIHelper.getInstance().getCoreProp().setProperty(engineID + "_" + Constants.STORE, smssLocation);
//				Utility.synchronizeEngineMetadata(engineID); // replacing this for engine
//				SecurityUpdateUtils.addApp(engineID, !AbstractSecurityUtils.securityEnabled());
//
//				// only after all of this is good, should we add it to DIHelper
//				DIHelper.getInstance().setLocalProperty(engineID, engine);
//				String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
//				engineNames = engineNames + ";" + engineID;
//				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
//			}
//
//			// convert the .temp to .smss file
//			smssLocation = smssLocation.replace("temp", "smss");
//			newProp = new File(Utility.normalizePath(smssLocation));
//			// replace the .temp on the DI Helper with .smss
//			DIHelper.getInstance().getCoreProp().setProperty(engineID + "_" + Constants.STORE, smssLocation);
//			try {
//				// we just copy over the the .temp file contents into the .smss
//				FileUtils.copyFile(propFile, newProp);
//				newProp.setReadable(true);
//			} catch (IOException ioe) {
//				logger.error(Arrays.toString(ioe.getStackTrace()));
//				throw new IOException("Could not create .smss file for new database");
//			}
//
//			// set the prop file to be the smss
//			engine.setPropFile(smssLocation);
//		} catch (IOException ioe) {
//			error = true;
//			logger.error(Arrays.toString(ioe.getStackTrace()));
//			throw new IOException(ioe.getMessage());
//		} catch (Exception e) {
//			error = true;
//			logger.error(Arrays.toString(e.getStackTrace()));
//			String errorMessage = e.getMessage();
//			if (e.getMessage() == null) {
//				errorMessage = "Unknown error occurred during data loading. Please check computer configuration.";
//			}
//			throw new IOException(errorMessage);
//		} finally {
//			// here, the propFile is the .temp file
//			// we converted it into a .smss already
//			// so delete the .temp
//			if (propFile != null) {
//				try {
//					// this should work now
//					FileUtils.forceDelete(propFile);
//				} catch (Exception e) {
//					logger.error(Arrays.toString(e.getStackTrace()));
//					throw new IOException("Could not delete .temp file for new database");
//				}
//			}
//
//			// we got an error... so undo any files/paths we just created
//			if (error) {
//				try {
//					// close the DB so we can delete it
//					if (engine != null)
//						engine.close();
//
//					// delete the .temp file
//					if (propFile != null && propFile.exists())
//						FileUtils.forceDelete(propFile);
//
//					// delete the .smss file
//					if (newProp != null && newProp.exists())
//						FileUtils.forceDelete(newProp);
//
//					// delete the engine folder and all its contents
//					String engineFolderPath = baseDirectory + System.getProperty("file.separator") + "db"
//							+ System.getProperty("file.separator") + SmssUtilities.getUniqueName(engineName, engineID);
//					File engineFolderDir = new File(Utility.normalizePath(engineFolderPath));
//					if (engineFolderDir.exists()) {
//						File[] files = engineFolderDir.listFiles();
//						if (files != null) { // some JVMs return null for empty dirs
//							for (File f : files) {
//								FileUtils.forceDelete(f);
//							}
//						}
//						FileUtils.forceDelete(engineFolderDir);
//					}
//				} catch (Exception e) {
//					logger.error(Arrays.toString(e.getStackTrace()));
//				}
//			}
//		}
//	}
//
//	private IEngine createNewTinker(ImportOptions options) throws Exception {
//		// autoLoad = false ---> means we will close the engine and then start it up
//		// again using the smss file...
//		Boolean autoLoad = (options.isAutoLoad() != null) ? options.isAutoLoad() : true;
//		options.setAutoLoad(autoLoad);
//		ImportOptions.IMPORT_TYPE importType = options.getImportType();
//		Hashtable<String, String>[] metamodelInfo;
//		AbstractFileReader reader = null;
//		IEngine engine;
//
//		switch (importType) {
//		// if it is POI excel using the loader sheet format
////		case EXCEL_POI : reader = new POIReader();
////						 reader.setAutoLoad(autoLoad);
////						 engine = reader.importFileWithOutConnection(options);break;
////						 
////		// if it is a flat table excel to be loaded as a RDF
////		// this is similar to csv file but loops through each sheet
////		case EXCEL : reader = new RdfExcelTableReader();
////		reader.setAutoLoad(autoLoad);
//// 		 metamodelInfo = options.getMetamodelArray();
//// 		// if a metamodel has been defined set it for the reader to use; if not, it assumes a hand written prop file location is specified in the last column of each sheet
//// 		 if(metamodelInfo != null) {			 			 
//// 			reader.setRdfMapArr(metamodelInfo);
//// 		 }
//// 		 engine = reader.importFileWithOutConnection(options);break;
//
//		// if it is a flat csv table upload
//		case CSV:
//			reader = new TinkerCsvReader();
//			reader.setAutoLoad(autoLoad);
//			metamodelInfo = options.getMetamodelArray();
//			if (metamodelInfo != null) {
//				reader.setRdfMapArr(metamodelInfo);
//			}
//
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
////		case NLP :reader = new NLPReader();
////		reader.setAutoLoad(autoLoad);
////		   engine = reader.importFileWithOutConnection(options);break; 
//
//		// no other options exist, throw an error
//		default:
//			String errorMessage = "Import type, " + importType + ", in a RDF database format is not supported";
//			throw new IOException(errorMessage);
//		}
//
//		// write type map to tinker prop file
//		if (engine.getEngineType() == IEngine.ENGINE_TYPE.TINKER) {
//			Map<String, String> typeMap = ((TinkerEngine) engine).getTypeMap();
//			Gson gson = new GsonBuilder().create();
//			String json = gson.toJson(typeMap);
//			String mapProp = "TYPE_MAP" + "\t" + json + "\n";
//			Map<String, String> nameMap = ((TinkerEngine) engine).getNameMap();
//			json = gson.toJson(nameMap);
//			mapProp += "NAME_MAP" + "\t" + json + "\n";
//			try {
//				Files.write(Paths.get(options.getSMSSLocation()), mapProp.getBytes(), StandardOpenOption.APPEND);
//			} catch (IOException ioe) {
//				logger.error(Arrays.toString(ioe.getStackTrace()));
//				throw new IOException("Unable to add type map to smms.");
//			}
//		}
//
//		return engine;
//	}
//
//	private IEngine createNewRDF(ImportOptions options) throws Exception {
//		// autoLoad = false ---> means we will close the engine and then start it up
//		// again using the smss file...
//		Boolean autoLoad = (options.isAutoLoad() != null) ? options.isAutoLoad() : true;
//		options.setAutoLoad(autoLoad);
//		ImportOptions.IMPORT_TYPE importType = options.getImportType();
//		Hashtable<String, String>[] metamodelInfo;
//		AbstractFileReader reader = null;
//		IEngine engine;
//
//		switch (importType) {
//		// if it is POI excel using the loader sheet format
//		case EXCEL_POI:
//			reader = new POIReader();
//			reader.setAutoLoad(autoLoad);
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		// if it is a flat table excel to be loaded as a RDF
//		// this is similar to csv file but loops through each sheet
//		case EXCEL:
//			reader = new RdfExcelTableReader();
//			reader.setAutoLoad(autoLoad);
//			metamodelInfo = options.getMetamodelArray();
//			// if a metamodel has been defined set it for the reader to use; if not, it
//			// assumes a hand written prop file location is specified in the last column of
//			// each sheet
//			if (metamodelInfo != null) {
//				reader.setRdfMapArr(metamodelInfo);
//			}
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		// if it is a flat csv table upload
//		case CSV:
//			reader = new CSVReader();
//			reader.setAutoLoad(autoLoad);
//			metamodelInfo = options.getMetamodelArray();
//			if (metamodelInfo != null) {
//				reader.setRdfMapArr(metamodelInfo);
//			}
//
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		case NLP:
//			reader = new NLPReader();
//			reader.setAutoLoad(autoLoad);
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		// no other options exist, throw an error
//		default:
//			String errorMessage = "Import type, " + importType + ", in a RDF database format is not supported";
//			throw new IOException(errorMessage);
//		}
//		return engine;
//
//	}
//
//	private IEngine createNewRDBMS(ImportOptions options) throws Exception {
//
//		Hashtable<String, String>[] metamodelInfo;
//		// default is to just upload and keep duplications in data, i.e. what you put in
//		// is what you get
//		Boolean allowDups = (options.isAllowDuplicates() == null) ? true : options.isAllowDuplicates();
//		options.setAllowDuplicates(allowDups);
//		// autoLoad = false ---> means we will close the engine and then start it up
//		// again using the smss file...
//		Boolean autoLoad = (options.isAutoLoad() != null) ? options.isAutoLoad() : true;
//		options.setAutoLoad(autoLoad);
//		ImportOptions.IMPORT_TYPE importType = options.getImportType();
//		AbstractFileReader reader = null;
//		IEngine engine;
//
//		switch (importType) {
//		// excel upload from a loader sheet format as a RDBMS
//		case EXCEL_POI:
//			POIReader poiReader = new POIReader();
//			poiReader.setAutoLoad(autoLoad);
//			engine = poiReader.importFileWithOutConnectionRDBMS(options);
//			break;
//
//		// csv upload into rdbms
//		case CSV:
//			reader = new RDBMSReader();
//			reader.setAutoLoad(autoLoad);
//			reader.setObjectValueMap(options.getObjectValueMap());
//			reader.setObjectTypeMap(options.getObjectTypeMap());
//			reader.setRowKey(options.getRowKey());
//			reader.setCreateIndexes(options.getCreateIndexes());
//			// if a metamodel has been defined set it for the reader to use
//			// if one is not defined, it assumes a hand written prop file location is
//			// specified in the last column of each sheet
//			metamodelInfo = options.getMetamodelArray();
//			if (metamodelInfo != null) {
//				reader.setRdfMapArr(metamodelInfo);
//			}
//
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		// csv upload via flat
//		case CSV_FLAT_LOAD:
//			reader = new RDBMSFlatCSVUploader();
//
//			// get new headers if user defined
//			Map<String, Map<String, String>> newCsvHeaders = options.getCsvNewHeaders();
//			if (newCsvHeaders != null) {
//				((AbstractCSVFileReader) reader).setNewCsvHeaders(newCsvHeaders);
//			}
//
//			// if the data type map has been created from the FE
//			List<Map<String, String[]>> csvDataTypeMap = options.getCsvDataTypeMap();
//			if (csvDataTypeMap != null) {
//				((RDBMSFlatCSVUploader) reader).setDataTypeMapList(csvDataTypeMap);
//			}
//
//			reader.setAutoLoad(autoLoad);
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		// excel upload via flat
//		case EXCEL_FLAT_UPLOAD:
//			reader = new RDBMSFlatExcelUploader();
//
//			// get new headers if user defined
//			List<Map<String, Map<String, String>>> newExcelHeaders = options.getExcelNewHeaders();
//			if (newExcelHeaders != null) {
//				((RDBMSFlatExcelUploader) reader).setNewExcelHeaders(newExcelHeaders);
//			}
//
//			// if the data type map has been created from the FE
//			List<Map<String, Map<String, String[]>>> excelDataTypeMap = options.getExcelDataTypeMap();
//			if (excelDataTypeMap != null) {
//				((RDBMSFlatExcelUploader) reader).setDataTypeMapList(excelDataTypeMap);
//			}
//
//			reader.setAutoLoad(autoLoad);
//			engine = reader.importFileWithOutConnection(options);
//			break;
//
//		case EXTERNAL_RDBMS:
//			ImportRDBMSProcessor importRDBMSReader = new ImportRDBMSProcessor();
//			engine = importRDBMSReader.addNewRDBMS(options);
//			break;
//
//		// no other options exist, throw an error
//		default:
//			String errorMessage = "Import type, " + importType + ", in a relational database format is not supported";
//			throw new IOException(errorMessage);
//		}
//		return engine;
//
//	}

	/**
	 * Update an existing database
	 * 
	 * @param options ImportOptions object containing all the necessary data for the
	 *                upload
	 * @throws Exception
	 */
//	private void addToExistingDb(ImportOptions options) throws Exception {
//		ImportOptions.IMPORT_TYPE importType = options.getImportType();
//		String engineId = options.getEngineID();
//		AbstractFileReader reader = null;
//
//		checkImportOptions(options);// perform checks on data being passed, if missing info, throws Exception
//		String errorMessage = "";
//
//		// we need the engine because after upload, we need to update solr instances
//		IEngine engine = Utility.getEngine(engineId);
//		if (engine == null) {
//			errorMessage = "Engine name, " + engineId + ", cannot be found to add new data";
//			throw new IOException(errorMessage);
//		}
//		ENGINE_TYPE engineDbType = engine.getEngineType();
//
//		String owlPath = engine.getOWL();
//		Hashtable<String, String> paramHash2 = new Hashtable<>();
//		paramHash2.put("engine", engineId);
//		owlPath = Utility.fillParam2(owlPath, paramHash2);
//
//		options.setOwlFileLocation(owlPath);
//
//		Hashtable<String, String>[] metamodelInfo;
//
//		switch (engineDbType) {
//		case SESAME:
//			switch (importType) {
//			case EXCEL_POI:
//				reader = new POIReader();
//				reader.importFileWithConnection(options);
//				break;
//
//			case EXCEL:
//				reader = new RdfExcelTableReader();
//
//				// if a metamodel has been defined set it for the reader to use
//				// if one is not defined, it assumes a hand written prop file location is
//				// specified in the last column of each sheet
//				if (options.getMetamodelArray() != null) {
//					metamodelInfo = options.getMetamodelArray();
//					reader.setRdfMapArr(metamodelInfo);
//				}
//				reader.importFileWithConnection(options);
//				break;
//
//			case CSV:
//				reader = new CSVReader();
//
//				// if a metamodel has been defined set it for the reader to use
//				// if one is not defined, it assumes a hand written prop file location is
//				// specified in the last column of each sheet
//				if (options.getMetamodelArray() != null) {
//					metamodelInfo = options.getMetamodelArray();
//					reader.setRdfMapArr(metamodelInfo);
//				}
//				reader.importFileWithConnection(options);
//				break;
//
//			case NLP:
//				reader = new NLPReader();
//				reader.importFileWithConnection(options);
//				break;
//
//			default:
//				errorMessage = "Import type, " + importType + ", in a RDF database format is not supported";
//				throw new IOException(errorMessage);
//			}
//			break;
//
//		case TINKER:
//			switch (importType) {
//			// TODO: build out adding to an existing tinker engine
//
//			}
//		case RDBMS:
//			switch (importType) {
//			case CSV:
//				reader = new RDBMSReader();
//				reader.setObjectValueMap(options.getObjectValueMap());
//				reader.setObjectTypeMap(options.getObjectTypeMap());
//				reader.setRowKey(options.getRowKey());
//				reader.setCreateIndexes(options.getCreateIndexes());
//
//				// if a metamodel has been defined set it for the reader to use
//				// if one is not defined, it assumes a hand written prop file location is
//				// specified in the last column of each sheet
//				if (options.getMetamodelArray() != null) {
//					metamodelInfo = options.getMetamodelArray();
//					reader.setRdfMapArr(metamodelInfo);
//				}
//				reader.importFileWithConnection(options);
//				break;
//
//			case CSV_FLAT_LOAD:
//				reader = new RDBMSFlatCSVUploader();
//
//				// get new headers if user defined
//				if (options.getCsvNewHeaders() != null) {
//					Map<String, Map<String, String>> newCsvHeaders = options.getCsvNewHeaders();
//					((AbstractCSVFileReader) reader).setNewCsvHeaders(newCsvHeaders);
//				}
//
//				// if the data type map has been created from the FE
//				if (options.getCsvDataTypeMap() != null) {
//					List<Map<String, String[]>> csvDataTypeMap = options.getCsvDataTypeMap();
//					((RDBMSFlatCSVUploader) reader).setDataTypeMapList(csvDataTypeMap);
//				}
//				reader.importFileWithConnection(options);
//				break;
//
//			case EXCEL_FLAT_UPLOAD:
//				reader = new RDBMSFlatExcelUploader();
//
//				// get new headers if user defined
//				if (options.getExcelNewHeaders() != null) {
//					List<Map<String, Map<String, String>>> newExcelHeaders = options.getExcelNewHeaders();
//					((RDBMSFlatExcelUploader) reader).setNewExcelHeaders(newExcelHeaders);
//				}
//
//				// if the data type map has been created from the FE
//				if (options.getExcelDataTypeMap() != null) {
//					List<Map<String, Map<String, String[]>>> excelDataTypeMap = options.getExcelDataTypeMap();
//					((RDBMSFlatExcelUploader) reader).setDataTypeMapList(excelDataTypeMap);
//				}
//				reader.importFileWithConnection(options);
//				break;
//
//			default:
//				errorMessage = "Import type, " + importType + ", in a relational database format is not supported";
//				throw new IOException(errorMessage);
//			}
//			break;
//
//		default:
//			errorMessage = "Unable to add data into database " + engineId + ", because it has database type, "
//					+ engineDbType + ", which " + "is currently unsupported for adding data.";
//			throw new IOException(errorMessage);
//		}
//
////		try {
//		// need to synchronize the metadata again since it might be out dated
//		Utility.synchronizeEngineMetadata(engineId);
//		// Do we need this?
////			Utility.addToSolrInstanceCore(engine);
////		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | ParseException e) {
////			classLogger.error(Constants.STACKTRACE, e);
////			errorMessage = "Loaded data successfully, but error occurred updating instances into solr";
////			throw new IOException(errorMessage);
////		}
//	}
//
//	// TODO: who really uses this?
//	// I CAN SEE THIS BEING USEFUL... BUT THIS NEEDS A LOT MORE THINKING WITH HOW IT SHOULD WORK
//	// AND NEEDS TO BE EXPANDED TO ALLOW FOR RDBMS
//	private boolean overrideExistingDb(ImportOptions options) {
//		ImportOptions.IMPORT_TYPE importType = options.getImportType();
//		String engineName = options.getDbName();
//		options.getBaseUrl();
//		String fileNames = options.getFileLocations();
//		
//		if(importType == IMPORT_TYPE.EXCEL_POI){
//			boolean success = true;
//			POIReader reader = new POIReader();
//			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
//			String[] files = fileNames.split(";");
//			for (String file : files) {
//				FileInputStream fileIn = null;
//				try {
//					fileIn = new FileInputStream(file);
//					XSSFWorkbook book = new XSSFWorkbook(fileIn);
//					XSSFSheet lSheet = book.getSheet("Loader");
//					int lastRow = lSheet.getLastRowNum();
//
//					ArrayList<String> nodes = new ArrayList<String>();
//					ArrayList<String[]> relationships = new ArrayList<String[]>();
//					for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
//						XSSFRow sheetNameRow = lSheet.getRow(rIndex);
//						XSSFCell cell = sheetNameRow.getCell(0);
//						XSSFSheet sheet = book.getSheet(cell.getStringCellValue());
//
//						XSSFRow row = sheet.getRow(0);
//						String sheetType = "";
//						if (row.getCell(0) != null) {
//							sheetType = row.getCell(0).getStringCellValue();
//						}
//						if ("Node".equalsIgnoreCase(sheetType)) {
//							if (row.getCell(1) != null) {
//								nodes.add(Utility.cleanString(row.getCell(1).getStringCellValue(),true));
//							}
//						}
//						if ("Relation".equalsIgnoreCase(sheetType)) {
//							String subject = "";
//							String object = "";
//							String relationship = "";
//							if (row.getCell(1) != null && row.getCell(2) != null) {
//								subject = row.getCell(1).getStringCellValue();
//								object = row.getCell(2).getStringCellValue();
//
//								row = sheet.getRow(1);
//								if (row.getCell(0) != null) {
//									relationship = row.getCell(0)
//											.getStringCellValue();
//								}
//
//								relationships.add(new String[] { Utility.cleanString(subject, true),
//										Utility.cleanString(relationship, true), Utility.cleanString(object, true) });
//							}
//						}
//					}
//					String deleteQuery = "";
//					UpdateProcessor proc = new UpdateProcessor();
//					proc.setEngine(engine);
//
//					int numberNodes = nodes.size();
//					if (numberNodes > 0) {
//						for (String node : nodes) {
//							deleteQuery = "DELETE {?s ?p ?prop. ?s ?x ?y} WHERE { {";
//							deleteQuery += "SELECT ?s ?p ?prop ?x ?y WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
//							deleteQuery += node;
//							deleteQuery += "> ;} {?s ?x ?y} MINUS {?x <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation> ;} ";
//							deleteQuery += "OPTIONAL{ {?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?s ?p ?prop ;} } } } ";
//							deleteQuery += "}";
//
//							proc.setQuery(deleteQuery);
//							logger.info(deleteQuery);
//							proc.setEngine(engine);
//							proc.processQuery();
//						}
//					}
//
//					int numberRelationships = relationships.size();
//					if (numberRelationships > 0) {
//						for (String[] rel : relationships) {
//							deleteQuery = "DELETE {?in ?relationship ?out. ?relationship ?contains ?prop} WHERE { {";
//							deleteQuery += "SELECT ?in ?relationship ?out ?contains ?prop WHERE { "
//									+ "{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
//							deleteQuery += rel[0];
//							deleteQuery += "> ;} ";
//
//							deleteQuery += "{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
//							deleteQuery += rel[2];
//							deleteQuery += "> ;} ";
//
//							deleteQuery += "{?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/";
//							deleteQuery += rel[1];
//							deleteQuery += "> ;} {?in ?relationship ?out ;} ";
//							deleteQuery += "OPTIONAL { {?relationship ?contains ?prop ;} } } } ";
//							deleteQuery += "}";
//
//							proc.setQuery(deleteQuery);
//							logger.info(deleteQuery);
//							proc.processQuery();
//						}
//					}
//
//					String owlFile = baseDirectory + "/" + DIHelper.getInstance().getProperty(engineName+"_"+Constants.OWL);
//					options.setOwlFileLocation(owlFile);
//					//reader.importFileWithConnection(engineName, file, customBaseUri, owlFile);
//					reader.importFileWithConnection(options);//seems tricky, might break as only one file is passed at a time during the loop, however, this file is not used anywhere as such..need to find the real use in the workflow.
//				} catch (Exception ex) {
//					success = false;
//					classLogger.error(Constants.STACKTRACE, ex);
//				}finally{
//					try{
//						if(fileIn!=null)
//							fileIn.close();
//					}catch(IOException e) {
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//			return success;
//		}
//		else{
//			// currently only processing override for excel
//			return false;
//		}
//	}

//	// TOOD: need to go through and make prop file writer cleaner
//	private PropFileWriter runPropWriter(ImportOptions options) throws IOException {
//		PropFileWriter propWriter = new PropFileWriter();
//		String dbName = options.getDbName(), dbPropFile = options.getSMSSLocation();
//		ImportOptions.DB_TYPE dbType = options.getDbType();
//		propWriter.setEngineID(options.getEngineID());
//		// need to make provision for dbType
//		if (dbType == ImportOptions.DB_TYPE.RDBMS) {
//			RdbmsTypeEnum dbDriverType = options.getRDBMSDriverType();
//			if (options.getImportMethod().equals(ImportOptions.IMPORT_METHOD.CONNECT_TO_EXISTING_RDBMS)) { // TODO:
//																											// check if
//																											// this if
//																											// check
//																											// needs to
//																											// be here
//				String host = options.getHost();
//				String port = options.getPort();
//				String schema = options.getSchema();
//				String username = options.getUsername();
//				String password = options.getPassword();
//
//				AbstractSqlQueryUtil sqlQueryUtil = SqlQueryUtilFactory.initialize(dbDriverType, host, port, schema,
//						username, password);
//				propWriter.setSQLQueryUtil(sqlQueryUtil);
//			}
//			propWriter.setRDBMSType(dbDriverType);
//		}
//
//		if (dbType == ImportOptions.DB_TYPE.TINKER) {
//			ImportOptions.TINKER_DRIVER driver = options.getTinkerDriverType();
//			propWriter.setTinkerType(driver);
//		}
//
//		propWriter.setBaseDir(baseDirectory);
//		propWriter.runWriter(dbName, dbPropFile, dbType);
//		return propWriter;
//	}
//
//	// perform checks on import options, if error throw exception right away
//	private void checkImportOptions(ImportOptions options) throws IOException {
//		String errorMessage = null;
//		String engineId = options.getEngineID();
//		String engineName = options.getDbName();
//		String filePath = options.getFileLocations();
//		ImportOptions.IMPORT_TYPE importType = options.getImportType();
//		ImportOptions.DB_TYPE dbType = options.getDbType();
//		ImportOptions.IMPORT_METHOD importMethod = options.getImportMethod();
//		String engineID = options.getEngineID();
//		if (DIHelper.getInstance().getLocalProp(engineId) != null
//				&& ((importType.equals(ImportOptions.IMPORT_TYPE.EXTERNAL_RDBMS)
//						|| importMethod.equals(ImportOptions.IMPORT_METHOD.CREATE_NEW)))) {
//			errorMessage = "Database name already exists. \nPlease make the database name unique \nor consider import method to \"Add To Existing\".";
//			throw new IOException(errorMessage);
//		}
//		if (engineId == null || engineId.isEmpty()) {
//			errorMessage = "Engine name is empty.  Require a valid engine name.";
//		}
//		if (importType == null) {
//			errorMessage = "Import type is not specified.";
//			throw new IOException(errorMessage);
//		}
//		if ((filePath == null || filePath.isEmpty()) && ((!importType.equals(ImportOptions.IMPORT_TYPE.EXTERNAL_RDBMS)
//				&& importMethod.equals(ImportOptions.IMPORT_METHOD.CREATE_NEW))
//				|| importMethod.equals(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING))) {
//			errorMessage = "No files have been identified to upload.  Please select valid files to upload.";
//			throw new IOException(errorMessage);
//		}
//		if (importMethod.equals(ImportOptions.IMPORT_METHOD.CREATE_NEW) && dbType == null) {
//			errorMessage = "Database type is not specified.";
//			throw new IOException(errorMessage);
//		}
//		if (importMethod.equals(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING) && Utility.getEngine(engineId) == null) {
//			errorMessage = "Database to add to cannot be found. \nPlease select an existing database or considering creating a new database.";
//			throw new IOException(errorMessage);
//		}
//		if (engineID == null) {
//			errorMessage = "Need to add engineID";
//			throw new IOException(errorMessage);
//		}
//
//	}

	// TODO: this needs to be where we do connect to existing RDBMS
	public boolean processNewRDBMS(String customBaseURI, String fileNames, String repoName, String type, String url,
			String username, char[] password) throws IOException {
		boolean success = false;

//		ImportRDBMSProcessor proc = new ImportRDBMSProcessor();
//		if(proc.checkConnection(type, url, username, password)) {
//			success = proc.setUpRDBMS();
//		} else {
//			return false;
//		}
//
//		File propFile = new File(proc.propWriter.propFileName);
//		File newProp = new File(proc.propWriter.propFileName.replace("temp", "smss"));
//		
//		try {
//			FileUtils.copyFile(propFile, newProp);
//			success = true;
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		propFile.delete();

		return success;
	}
}
