package prerna.sablecc2.reactor.database.upload.rdbms.excel;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.excel.ExcelBlock;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.poi.main.helper.excel.ExcelWorkbookFilePreProcessor;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.database.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class RdbmsUploadExcelDataReactor extends AbstractUploadFileReactor {

	/*
	 * There are quite a few things that we need
	 * 1) database -> name of the database to create
	 * 1) filePath -> string contianing the path of the file
	 * 2) dataTypes -> map of the sheet to another map of the header to the type, this will contain the original headers we send to FE
	 * 3) newHeaders -> map of the sheet to another map containing old header to new headers for the csv file
	 * 4) additionalTypes -> map of the sheet to another map containing header to an additional type specification
	 * 						additional inputs would be {header : currency, header : date_format, ... }
	 * 5) clean -> boolean if we should clean up the strings before insertion, default is true
	 * TODO: 6) deduplicate -> boolean if we should remove duplicate rows in the relational database
	 * 7) existing -> boolean if we should add to an existing database, defualt is false
	 */

	private ExcelWorkbookFileHelper helper;
	
	public RdbmsUploadExcelDataReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.DATABASE, 
				UploadInputUtility.FILE_PATH, 
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.DATA_TYPE_MAP,
				UploadInputUtility.NEW_HEADERS, 
				UploadInputUtility.ADDITIONAL_DATA_TYPES, 
				UploadInputUtility.CLEAN_STRING_VALUES,
				UploadInputUtility.REMOVE_DUPLICATE_ROWS,
				UploadInputUtility.REPLACE_EXISTING
			};
	}

	@Override
	public void generateNewDatabase(User user, final String newDatabaseName, final String filePath) throws Exception {
		/*
		 * Things we need to do
		 * 1) make directory
		 * 2) make owl
		 * 3) make temporary smss
		 * 4) make database class
		 * 5) load actual data
		 * 6) load owl metadata
		 * 7) add to localmaster and solr
		 */
		if(!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}

		Map<String, Map<String, Map<String, String>>> dataTypesMap = getDataTypeMap();
		Map<String, Map<String, Map<String, String>>> newHeaders = getNewHeaders();
		Map<String, Map<String, Map<String, String>>> additionalDataTypeMap = getAdditionalTypes();
		Map<String, Map<String, Map<String, String>>> metaDescriptions = getMetaDescriptions();
		Map<String, Map<String, Map<String, List<String>>>> metaLogicalNames = getMetaLogicalNames();
		Map<String,Map<String,String>> tableNames = getTableNameMap();
		Map<String,Map<String,String>> uniqueColumnNames = getUniqueColumnNameMap();
		final boolean clean = UploadInputUtility.getClean(this.store);
		final boolean replace = UploadInputUtility.getReplace(this.store);

		// now that I have everything, let us go through and insert

		// start by validation
		int stepCounter = 1;
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.databaseId, newDatabaseName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for database...");
		this.tempSmss = UploadUtilities.createTemporaryRdbmsSmss(this.databaseId, newDatabaseName, owlFile, RdbmsTypeEnum.H2_DB, null);
		DIHelper.getInstance().setEngineProperty(this.databaseId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create database store...");
		this.database = new RDBMSNativeEngine();
		this.database.setEngineId(this.databaseId);
		this.database.setEngineName(newDatabaseName);
		Properties smssProps = Utility.loadProperties(this.tempSmss.getAbsolutePath());
		smssProps.put("TEMP", true);
		this.database.open(smssProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		logger.info("Load excel file...");
		this.helper = new ExcelWorkbookFileHelper();
		this.helper.parse(filePath);
		logger.info("Done loading excel file");

		Owler owler = new Owler(this.databaseId, owlFile.getAbsolutePath(), DATABASE_TYPE.RDBMS);
		// here is where we actually insert the data
		processExcelSheets(this.database, owler, this.helper, dataTypesMap, 
				additionalDataTypeMap, newHeaders, 
				metaDescriptions, metaLogicalNames, 
				tableNames, uniqueColumnNames, 
				clean, replace);
		this.helper.clear();
		owler.export();
		this.database.setOwlFilePath(owlFile.getPath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		
		// TODO
		// TODO special insights for excel
//		Map<String, Map<String, SemossDataType>> existingMetamodel = UploadUtilities.getExistingMetamodel(owler);
		// create form insights
		// user hasn't defined the data types
		// that means i am going to assume that i should
		// load everything
//		if (dataTypesMap == null || dataTypesMap.isEmpty()) {
//			// need to calculate all the ranges
//			ExcelWorkbookFilePreProcessor wProcessor = new ExcelWorkbookFilePreProcessor();
//			wProcessor.parse(this.helper.getFilePath());
//			wProcessor.determineTableRanges();
//			Map<String, ExcelSheetPreProcessor> sProcessor = wProcessor.getSheetProcessors();
//			for (String sheetName : sProcessor.keySet()) {
//				ExcelSheetPreProcessor sheetProcessor = sProcessor.get(sheetName);
//				List<ExcelBlock> blocks = sheetProcessor.getAllBlocks();
//				for (ExcelBlock eBlock : blocks) {
//					List<ExcelRange> ranges = eBlock.getRanges();
//					for (ExcelRange eRange : ranges) {
//						String range = eRange.getRangeSyntax();
//						boolean singleRange = (blocks.size() == 1 && ranges.size() == 1);
//						ExcelQueryStruct qs = new ExcelQueryStruct();
//						qs.setSheetName(sheetName);
//						qs.setSheetRange(range);
//						if (newHeaders.containsKey(sheetName)) {
//							Map<String, Map<String, String>> aNewHeadersMap = newHeaders.get(sheetName);
//							if (aNewHeadersMap.containsKey(range)) {
//								qs.setNewHeaderNames(aNewHeadersMap.get(range));
//							}
//						}
//						// sheetIterator will calculate all the types if
//						// necessary
//						ExcelSheetFileIterator sheetIterator = this.helper.getSheetIterator(qs);
//						Sheet sheet = sheetIterator.getSheet();
//						int[] headerIndicies = sheetIterator.getHeaderIndicies();
//						Map<String, String> newRangeHeaders = qs.getNewHeaderNames();
//						int startRow = eRange.getStartRow();
//						SemossDataType[] types = sheetIterator.getTypes();
//						String[] headers = sheetIterator.getHeaders();
//						Map<String, Object> dataValidationMap = ExcelDataValidationHelper.getDataValidation(sheet, newRangeHeaders, Arrays.copyOf(headers, headers.length), types, headerIndicies, startRow);
//						sheetName = RDBMSEngineCreationHelper.cleanTableName(sheetName).toUpperCase();
//						if (dataValidationMap != null && !dataValidationMap.isEmpty()) {
//							Map<String, Object> widgetJson = ExcelDataValidationHelper.createInsertForm(newAppName, sheetName, dataValidationMap,  Arrays.copyOf(headers, headers.length));
//							UploadUtilities.addInsertFormInsight(insightDatabase, this.appId, newAppName, sheetName, widgetJson);
//						} else {
//							// get header descriptions
//							dataValidationMap = ExcelDataValidationHelper.getHeaderComments(sheet, newRangeHeaders, Arrays.copyOf(headers, headers.length), types, headerIndicies, startRow);
//							Map<String, Object> widgetJson = ExcelDataValidationHelper.createInsertForm(newAppName, sheetName, dataValidationMap, Arrays.copyOf(headers, headers.length));
//							UploadUtilities.addInsertFormInsight(insightDatabase, this.appId, newAppName, sheetName, widgetJson);							
//						}
//					}
//				}
//			}
//		} else {
//			// only load the things that are defined
//			for (String sheetName : dataTypesMap.keySet()) {
//				Map<String, Map<String, String>> rangeMaps = dataTypesMap.get(sheetName);
//				boolean singleRange = (rangeMaps.keySet().size() == 1);
//				for (String range : rangeMaps.keySet()) {
//					ExcelQueryStruct qs = new ExcelQueryStruct();
//					qs.setSheetName(sheetName);
//					qs.setSheetRange(range);
//					qs.setColumnTypes(rangeMaps.get(range));
//					if (additionalDataTypeMap.containsKey(sheetName)) {
//						Map<String, Map<String, String>> aRangeMap = additionalDataTypeMap.get(sheetName);
//						if (aRangeMap.containsKey(range)) {
//							qs.setAdditionalTypes(aRangeMap.get(range));
//						}
//					}
//					if (newHeaders.containsKey(sheetName)) {
//						Map<String, Map<String, String>> aNewHeadersMap = newHeaders.get(sheetName);
//						if (aNewHeadersMap.containsKey(range)) {
//							qs.setNewHeaderNames(aNewHeadersMap.get(range));
//						}
//					}
//					Map<String, String> newRangeHeaders = qs.getNewHeaderNames();
//					ExcelSheetFileIterator sheetIterator = this.helper.getSheetIterator(qs);
//					Sheet sheet = sheetIterator.getSheet();
//					int[] headerIndicies = sheetIterator.getHeaderIndicies();
//					ExcelRange eRange = new ExcelRange(range);
//					int startRow = eRange.getStartRow();
//					SemossDataType[] types = sheetIterator.getTypes();
//					String[] headers = sheetIterator.getHeaders();
//					Map<String, Object> dataValidationMap = ExcelDataValidationHelper.getDataValidation(sheet, newRangeHeaders, Arrays.copyOf(headers, headers.length), types, headerIndicies, startRow);
//					sheetName = RDBMSEngineCreationHelper.cleanTableName(sheetName).toUpperCase();
//					if (dataValidationMap != null && !dataValidationMap.isEmpty()) {
//						Map<String, Object> widgetJson = ExcelDataValidationHelper.createInsertForm(newAppName, sheetName, dataValidationMap, Arrays.copyOf(headers, headers.length));
//						UploadUtilities.addInsertFormInsight(insightDatabase, this.appId, newAppName, sheetName, widgetJson);
//					} else {
//						// get header descriptions
//						dataValidationMap = ExcelDataValidationHelper.getHeaderComments(sheet, newRangeHeaders, Arrays.copyOf(headers, headers.length), types, headerIndicies, startRow);
//						Map<String, Object> widgetJson = ExcelDataValidationHelper.createInsertForm(newAppName, sheetName, dataValidationMap, Arrays.copyOf(headers, headers.length));
//						UploadUtilities.addInsertFormInsight(insightDatabase, this.appId, newAppName, sheetName, widgetJson);
//					}
//				}
//			}
//		}
	}

	@Override
	public void addToExistingDatabase(String filePath) throws Exception {
		if(!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}
		if (!(this.database instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("Database must be using a relational database");
		}

		Map<String, Map<String, Map<String, String>>> dataTypesMap = getDataTypeMap();
		Map<String, Map<String, Map<String, String>>> newHeaders = getNewHeaders();
		Map<String, Map<String, Map<String, String>>> additionalDataTypeMap = getAdditionalTypes();
		Map<String, Map<String, Map<String, String>>> metaDescriptions = getMetaDescriptions();
		Map<String, Map<String, Map<String, List<String>>>> metaLogicalNames = getMetaLogicalNames();
		final boolean clean = UploadInputUtility.getClean(this.store);
		final boolean replace = UploadInputUtility.getReplace(this.store);

		// logger.info("Get existing database schema...");
		// Map<String, Map<String, String>> existingRDBMSStructure =
		// RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
		// logger.info("Done getting existing database schema");

		int stepCounter = 1;
		logger.info(stepCounter + ". Start loading data..");
		logger.info("Load excel file...");
		this.helper = new ExcelWorkbookFileHelper();
		this.helper.parse(filePath);
		logger.info("Done loading excel file");

		/*
		 * Since we want to determine if we should add to an existing table or
		 * make new tables We need to go to the sheet level and determine it
		 */

		Owler owler = new Owler(this.database);
		processExcelSheets(this.database, owler, this.helper, dataTypesMap, 
				additionalDataTypeMap, newHeaders, 
				metaDescriptions, metaLogicalNames, 
				null, null, 
				clean, replace);
		owler.export();
		this.database.setOwlFilePath(owler.getOwlPath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

	}

	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
		}
	}

	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Processing actually happens here
	 * 
	 */
	
	/**
	 * Process all the excel sheets using the data type map
	 * @param database
	 * @param owler
	 * @param helper
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @param newHeaders
	 * @param metaDescriptions
	 * @param metaLogicalNames
	 * @param clean
	 * @param replace
	 * @throws Exception
	 */
	private void processExcelSheets(
			IDatabaseEngine database, 
			Owler owler, 
			ExcelWorkbookFileHelper helper, 
			Map<String, Map<String, Map<String, String>>> dataTypesMap, 
			Map<String, Map<String, Map<String, String>>> additionalDataTypeMap,
			Map<String, Map<String, Map<String, String>>> newHeaders,
			Map<String, Map<String, Map<String, String>>> metaDescriptions,
			Map<String, Map<String, Map<String, List<String>>>> metaLogicalNames,
			Map<String,Map<String,String>> tableNames,
			Map<String,Map<String,String>> uniqueColumnsMap,
			boolean clean,
			boolean replace) throws Exception {
		
		// 
		Map<String, String> rangeAndNameMap = null;
		Map<String, String> rangeAndUniqueColumnMap = null;
		// user hasn't defined the data types
		// that means i am going to assume that i should 
		// load everything
		if (dataTypesMap == null || dataTypesMap.isEmpty()) {
			// need to calculate all the ranges
			ExcelWorkbookFilePreProcessor wProcessor = new ExcelWorkbookFilePreProcessor();
			wProcessor.parse(helper.getFilePath());
			wProcessor.determineTableRanges();
			Map<String, ExcelSheetPreProcessor> sProcessor = wProcessor.getSheetProcessors();

			for (String sheet : sProcessor.keySet()) {
				ExcelSheetPreProcessor sheetProcessor = sProcessor.get(sheet);
				List<ExcelBlock> blocks = sheetProcessor.getAllBlocks();
				int counterSheetName = 0;
				
				if (tableNames != null) {
					rangeAndNameMap = tableNames.get(sheet);
				}
				
				if (uniqueColumnsMap != null) {
					rangeAndUniqueColumnMap = uniqueColumnsMap.get(sheet);
				}

				for (ExcelBlock eBlock : blocks) {
					List<ExcelRange> ranges = eBlock.getRanges();
					for (ExcelRange eRange : ranges) {
						String range = eRange.getRangeSyntax();
						boolean singleRange = (blocks.size() == 1 && ranges.size() == 1);
						ExcelQueryStruct qs = new ExcelQueryStruct();
						String tableName = null;
						String uniqueColumnName = null;
						
						if (rangeAndNameMap != null) {
							tableName = rangeAndNameMap.get(range);
						}

						if (tableName == null) {
							if (ranges.size() > 1) {
								tableName = sheet + "_" + counterSheetName;
								counterSheetName++;
							} else {
								tableName = sheet;
							}
						}
						
						if (rangeAndUniqueColumnMap != null) {
							uniqueColumnName = rangeAndUniqueColumnMap.get(range);
						}

						qs.setSheetName(sheet);
						qs.setSheetRange(range);
						// sheetIterator will calculate the types if necessary
						ExcelSheetFileIterator sheetIterator = helper.getSheetIterator(qs);

						processSheet(database, owler, sheetIterator, singleRange, null, null, tableName, uniqueColumnName, clean, replace);
					}
				}
			}
		} else {
			// only load the things that are defined
			for (String sheet : dataTypesMap.keySet()) {
				Map<String, Map<String, String>> rangeMaps = dataTypesMap.get(sheet);
				Map<String, Map<String, String>> rangeDescription = metaDescriptions == null ? null : metaDescriptions.get(sheet);
				Map<String, Map<String, List<String>>> rangeLogicalNames = metaLogicalNames == null ? null : metaLogicalNames.get(sheet);
				boolean singleRange = (rangeMaps.keySet().size() == 1);
				int counterSheetName = 0;
				
				if (tableNames != null) {
					rangeAndNameMap = tableNames.get(sheet);
				}
				
				if (uniqueColumnsMap != null) {
					rangeAndUniqueColumnMap = uniqueColumnsMap.get(sheet);
				}

				for (String range : rangeMaps.keySet()) {
					ExcelQueryStruct qs = new ExcelQueryStruct();
					String tableName = null;
					String uniqueColumnName = null;
					
					if (rangeAndNameMap != null) {
						tableName = rangeAndNameMap.get(range);
					}

					if (tableName == null) {
						if (rangeMaps.size() > 1) {
							tableName = sheet + "_" + counterSheetName;
							counterSheetName++;
						} else {
							tableName = sheet;
						}
					}
					
					if (rangeAndUniqueColumnMap != null) {
						uniqueColumnName = rangeAndUniqueColumnMap.get(range);
					}

					qs.setSheetName(sheet);
					qs.setSheetRange(range);
					qs.setColumnTypes(rangeMaps.get(range));

					if (additionalDataTypeMap.containsKey(sheet)) {
						Map<String, Map<String, String>> aRangeMap = additionalDataTypeMap.get(sheet);
						if (aRangeMap.containsKey(range)) {
							qs.setAdditionalTypes(aRangeMap.get(range));
						}
					}

					if (newHeaders.containsKey(sheet)) {
						Map<String, Map<String, String>> aNewHeadersMap = newHeaders.get(sheet);
						if (aNewHeadersMap.containsKey(range)) {
							qs.setNewHeaderNames(aNewHeadersMap.get(range));
						}
					}

					Map<String, String> descriptions = rangeDescription == null ? null : rangeDescription.get(range);
					Map<String, List<String>> logicalNames = rangeLogicalNames == null ? null : rangeLogicalNames.get(range);
					ExcelSheetFileIterator sheetIterator = helper.getSheetIterator(qs);

					processSheet(database, owler, sheetIterator, singleRange, descriptions, logicalNames, tableName, uniqueColumnName, clean, replace);
				}
			}
		}
	}
	
	/**
	 * Process a single sheet
	 * @param database
	 * @param owler
	 * @param helper
	 * @param sheetname
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @param clean
	 * @param classLogger
	 * @throws Exception 
	 */
	private void processSheet(IDatabaseEngine database, Owler owler, ExcelSheetFileIterator helper, boolean singleRange, 
			Map<String, String> descriptions, Map<String, List<String>> logicalNames, 
			String sheet, String uniqueColumnName, 
			boolean clean, boolean replace) throws Exception {
		logger.info("Start parsing sheet metadata");
		// even if types are not defined
		// the qs will end up calculating everything for unknown types
		ExcelQueryStruct qs = helper.getQs();
		String sheetName = qs.getSheetName();
		String inputtedTableName = sheet;

		Map<String, String> sheetDataTypesMap = qs.getColumnTypes();
		Map<String, String> sheetAdditionalDataTypesMap = qs.getAdditionalTypes();

		Object[] headerTypesArr = getHeadersAndTypes(helper, sheetDataTypesMap, sheetAdditionalDataTypesMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info("Done parsing sheet metadata");
 
		logger.info("Create table...");
		String tableName = RDBMSEngineCreationHelper.cleanTableName(inputtedTableName).toUpperCase();

		// if user defines unique column name set that if not generate one
		// TODO: add change for false values once we want to enable that
		String uniqueRowId = uniqueColumnName == null ? tableName + RdbmsUploadReactorUtility.UNIQUE_ROW_ID: uniqueColumnName;

		// NOTE ::: SQL_TYPES will have the added unique row id at index 0
		String[] sqlTypes = null;
		try {
			sqlTypes = RdbmsUploadReactorUtility.createNewTable(database, tableName, uniqueRowId, headers, types, replace);
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new SemossPixelException(new NounMetadata("Error occurred during upload", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		logger.info("Done create table");

		bulkInsertSheet(database, helper, sheetName, tableName, headers, types, additionalTypes, clean, logger);
		RdbmsUploadReactorUtility.addIndex(database, tableName, uniqueRowId);

		RdbmsUploadReactorUtility.generateTableMetadata(owler, tableName, uniqueRowId, headers, sqlTypes, additionalTypes);
		UploadUtilities.insertFlatOwlMetadata(owler, tableName, headers, descriptions, logicalNames);
	}

	private void bulkInsertSheet(IDatabaseEngine database, ExcelSheetFileIterator helper, final String SHEET_NAME, final String TABLE_NAME, String[] headers,
			SemossDataType[] types, String[] additionalTypes, boolean clean, Logger logger) throws IOException {

		// now we need to loop through the excel sheet and cast to the appropriate type and insert
		// let us be smart about this and use a PreparedStatement for bulk insert
		// get the bulk statement

		// the prepared statement requires the table name and then the list of columns
		Object[] getPreparedStatementArgs = new Object[headers.length + 1];
		getPreparedStatementArgs[0] = TABLE_NAME;
		for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			getPreparedStatementArgs[headerIndex + 1] = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
		}
		PreparedStatement ps = (PreparedStatement) database.doAction(ACTION_TYPE.BULK_INSERT, getPreparedStatementArgs);

		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;

		logger.info("Start inserting data into table");
		// we loop through every row of the sheet
		Object[] nextRow = null;
		try {
			while (helper.hasNext()) {
				nextRow = helper.next().getValues();
				// we need to loop through every value and cast appropriately
				for (int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					Object value = nextRow[colIndex];
					// nulls get added as null
					// not interesting...
					if (value == null) {
						ps.setObject(colIndex + 1, null);
						continue;
					}

					// yay, actual data
					SemossDataType type = types[colIndex];
					// strings
					if (type == SemossDataType.STRING) {
						// expand different variable types to string
						String strValue = value + "";
						if (clean) {
							strValue = Utility.cleanString(strValue, false);
						}
						if (strValue.length() > 2000) {
							strValue = strValue.substring(0, 1997) + "...";
						}
						ps.setString(colIndex + 1, strValue);
					}
					// int
					else if(type == SemossDataType.INT) {
						if(value instanceof Number) {
							ps.setInt(colIndex+1, ((Number) value).intValue());
						} else {
							Integer intValue = null;
							String strValue = nextRow[colIndex].toString().trim();
							try {
								//added to remove $ and , in data and then try parsing as Double
								int mult = 1;
								if(strValue.startsWith("(") || strValue.startsWith("-")) {
									mult = -1;
								}
								strValue = strValue.replaceAll("[^0-9\\.E]", "");
								intValue = mult * ((Number) Double.parseDouble(strValue.trim())).intValue();
							} catch(NumberFormatException ex) {
								//do nothing
							}
							if(intValue != null) {
								ps.setInt(colIndex+1, intValue);
							} else {
								// set default as null
								ps.setObject(colIndex+1, null);
							}
						}
					}
					// doubles
					else if(type == SemossDataType.DOUBLE) {
						if(value instanceof Number) {
							ps.setDouble(colIndex+1, ((Number) value).doubleValue());
						} else {
							Double doubleValue = null;
							String strValue = nextRow[colIndex].toString().trim();
							try {
								// added to remove $ and , in data and then try
								// parsing as Double
								int mult = 1;
								if (strValue.startsWith("(") || strValue.startsWith("-")) { 
									mult = -1;
								}
								strValue = strValue.replaceAll("[^0-9\\.E]", "");
								doubleValue = mult * Double.parseDouble(strValue.trim());
							} catch (NumberFormatException ex) {
								// do nothing
							}
							if (doubleValue != null) {
								ps.setDouble(colIndex + 1, doubleValue);
							} else {
								// set default as null
								ps.setObject(colIndex + 1, null);
							}
						}
					}
					// dates
					else if (type == SemossDataType.DATE) {
						Long dTime = null;
						if (value instanceof SemossDate) {
							dTime = SemossDate.getTimeForDate((SemossDate) value);
						} else {
							dTime = SemossDate.getTimeForDate(value.toString(), additionalTypes[colIndex]);
						}
						if (dTime != null) {
							ps.setDate(colIndex + 1, new java.sql.Date(dTime));
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						}
					}
					// timestamps
					else if (type == SemossDataType.TIMESTAMP) {
						Long dTime = null;
						if (value instanceof SemossDate) {
							dTime = SemossDate.getTimeForTimestamp((SemossDate) value);
						} else {
							dTime = SemossDate.getTimeForTimestamp(value.toString(), additionalTypes[colIndex]);
						}
						if (dTime != null) {
							ps.setTimestamp(colIndex + 1, new java.sql.Timestamp(dTime));
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
						}
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					logger.info("Done inserting " + count + " number of rows");
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			logger.info("Finished");
			logger.info("Completed " + count + " number of rows");
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
			String errorMessage = "";
			if (nextRow == null) {
				errorMessage = "Error occurred while performing insert on excel row number = " + count;
			} else {
				errorMessage = "Error occurred while performing insert on excel data row:" + "\n" + Arrays.toString(nextRow);
			}
			throw new IOException(errorMessage);
		}

	}

	/**
	 * Figure out the types and how to use them
	 * Will return an object[]
	 * Index 0 of the return is an array of the headers
	 * Index 1 of the return is an array of the types
	 * Index 2 of the return is an array of the additional type information
	 * The 3 arrays all match based on index
	 * @param helper
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @return
	 */
	private Object[] getHeadersAndTypes(ExcelSheetFileIterator helper, Map<String, String> dataTypesMap, Map<String, String> additionalDataTypeMap) {
		String[] headers = helper.getHeaders();
		int numHeaders = headers.length;
		// we want types
		// and we want additional types
		SemossDataType[] types = new SemossDataType[numHeaders];
		String[] additionalTypes = new String[numHeaders];

		for (int i = 0; i < numHeaders; i++) {
			types[i] = SemossDataType.convertStringToDataType(dataTypesMap.get(headers[i]));
		}

		// get additional type information
		if (additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
			for (int i = 0; i < numHeaders; i++) {
				additionalTypes[i] = additionalDataTypeMap.get(headers[i]);
			}
		}

		return new Object[] { headers, types, additionalTypes };
	}
	
	///////////////////////////////////////////////////////

	/*
	 * Getters from noun store
	 */

	private Map<String, Map<String, Map<String, String>>> getDataTypeMap() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.DATA_TYPE_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, Map<String, String>>>) grs.get(0);
	}

	private Map<String, Map<String, Map<String, String>>> getNewHeaders() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.NEW_HEADERS);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, Map<String, String>>>) grs.get(0);
	}

	private Map<String, Map<String, Map<String, String>>> getAdditionalTypes() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.ADDITIONAL_DATA_TYPES);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		Gson gson = null;
		Map<String, Map<String, Map<String, Object>>> values = (Map<String, Map<String, Map<String, Object>>>) grs.get(0);
		Map<String, Map<String, Map<String, String>>> strValues = new HashMap<>();
		// stringify since the FE sends custom types as a map
		for(String k1 : values.keySet()) {
			Map<String, Map<String, Object>> inner = values.get(k1);
			Map<String, Map<String, String>> strInner = new HashMap<>();

			for(String k2 : inner.keySet()) {
				Map<String, Object> inner2 = inner.get(k2);
				Map<String, String> strInner2 = new HashMap<>();
				
				for(String k3 : inner2.keySet()) {
					
					if(inner2.get(k3) instanceof String) {
						strInner2.put(k3, inner2.get(k3) + "");
					} else {
						if(gson == null) {
							gson = new Gson();
						}
						strInner2.put(k3, gson.toJson(inner2.get(k3)));
					}
				}
			
				// put in parent map
				strInner.put(k2, strInner2);
			}
			
			// put in parent map
			strValues.put(k1, strInner);
		}
		return strValues;
	}
	
	public Map<String, Map<String, Map<String, String>>> getMetaDescriptions() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.DESCRIPTION_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, Map<String, String>>>) grs.get(0);
	}
	
	public Map<String, Map<String, Map<String, List<String>>>> getMetaLogicalNames() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.LOGICAL_NAMES_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, Map<String, List<String>>>>) grs.get(0);
	}

	public Map<String, Map<String, String>> getTableNameMap() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.TABLE_NAMES);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, String>>) grs.get(0);
	}
	
	public Map<String,Map<String,String>> getUniqueColumnNameMap() {
		GenRowStruct grs = this.store.getNoun(UploadInputUtility.UNIQUE_COLUMN);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Map<String, String>>) grs.get(0);
	}

	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//		
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
//		IDatabase coreEngine = new H2EmbeddedServerEngine();
//		coreEngine.open(engineProp);
//		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);
//		
//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\security.smss";
//		coreEngine = new H2EmbeddedServerEngine();
//		coreEngine.open(engineProp);
//		DIHelper.getInstance().setLocalProperty("security", coreEngine);
//		AbstractSecurityUtils.loadSecurityDatabase();
//
//		String filePath = "C:/Users/SEMOSS/Desktop/shifted.xlsx";
//
//		Insight in = new Insight();
//		PixelPlanner planner = new PixelPlanner();
//		planner.setVarStore(in.getVarStore());
//		in.getVarStore().put("$JOB_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
//		in.getVarStore().put("$INSIGHT_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
//
//		RdbmsUploadExcelDataReactor reactor = new RdbmsUploadExcelDataReactor();
//		reactor.setInsight(in);
//		reactor.setPixelPlanner(planner);
//		NounStore nStore = reactor.getNounStore();
//		// database name struct
//		{
//			GenRowStruct struct = new GenRowStruct();
//			struct.add(new NounMetadata("a" + Utility.getRandomString(6), PixelDataType.CONST_STRING));
//			nStore.addNoun(ReactorKeysEnum.APP.getKey(), struct);
//		}
//		// file path
//		{
//			GenRowStruct struct = new GenRowStruct();
//			struct.add(new NounMetadata(filePath, PixelDataType.CONST_STRING));
//			nStore.addNoun(ReactorKeysEnum.FILE_PATH.getKey(), struct);
//		}
//
//		reactor.In();
//		reactor.execute();
//	}

}
