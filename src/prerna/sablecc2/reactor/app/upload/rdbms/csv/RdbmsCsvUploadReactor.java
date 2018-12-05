package prerna.sablecc2.reactor.app.upload.rdbms.csv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.app.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.app.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class RdbmsCsvUploadReactor extends AbstractUploadFileReactor {

	// Need to create a unique id that includes the row number
	private int rowCounter;
	private Map<String, Map<String, String>> allConcepts = new LinkedHashMap<String, Map<String, String>>();
	private Map<String, Map<String, String>> concepts = new LinkedHashMap<String, Map<String, String>>();
	private Map<String, List<String>> relations = new LinkedHashMap<String, List<String>>();
	private Map<String, Map<String, String>> existingRDBMSStructure = new Hashtable<String, Map<String, String>>();
	private Set<String> existingTableWithAllColsAccounted = new HashSet<String>();
	private Set<String> addedTables = new HashSet<String>();
	private Map<String, String> sqlHash = new Hashtable<String, String>();
	private int indexUniqueId = 1;
	private List<String> recreateIndexList = new Vector<String>();
	private List<String> tempIndexAddedList = new Vector<String>();
	private List<String> tempIndexDropList = new Vector<String>();
	private final String FK = "_FK";
	protected Map<String, String> objectValueMap = new HashMap<String, String>();
	protected Map<String, String> objectTypeMap = new HashMap<String, String>();
	protected SQLQueryUtil queryUtil;
	protected boolean createIndexes = true; 
	// What to put in a prop file to grab the current row number
	protected String rowKey;
	
	private CSVFileHelper helper;

	public RdbmsCsvUploadReactor() {
		this.keysToGet = new String[] {
				UploadInputUtility.APP, 
				UploadInputUtility.FILE_PATH, 
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.DELIMITER, 
				UploadInputUtility.DATA_TYPE_MAP, 
				UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.METAMODEL, 
				UploadInputUtility.PROP_FILE,
				UploadInputUtility.START_ROW, 
				UploadInputUtility.END_ROW, 
				UploadInputUtility.ADDITIONAL_DATA_TYPES,
				UploadInputUtility.CREATE_INDEX };
	}

	@Override
	public void generateNewApp(User user, final String newAppId, final String newAppName, final String filePath) throws Exception {
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		boolean allowDuplicates = false;

		int stepCounter = 1;
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for database...");
		this.tempSmss = UploadUtilities.createTemporaryRdbmsSmss(newAppId, newAppName, owlFile, "H2_DB", null);
		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// get metamodel
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store);
		Map<String, String> dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);;

		/*
		 * Load data into rdbms engine
		 */
		logger.info(stepCounter + ". Create database store...");
		this.engine = new RDBMSNativeEngine();
		this.engine.setEngineId(newAppId);
		this.engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		this.engine.setProp(props);
		this.engine.openDB(null);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Parsing file metadata...");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.NEW_HEADERS));
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), this.engine.getEngineType());
		try {
			// open the csv file
			// and get the headers
			Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(helper, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.ADDITIONAL_DATA_TYPES));
			String[] headers = (String[]) headerTypesArr[0];
			SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
			queryUtil = SQLQueryUtil.initialize(((RDBMSNativeEngine) this.engine).getDbType());
			logger.info(stepCounter + ". Complete");
			stepCounter++;

			logger.info(stepCounter + ". Start loading data..");
			String[] sqlDataTypes = parseMetamodel(metamodelProps, owler, Arrays.asList(headers), types);

			// if(i ==0 ) {
			// scriptFile.println("-- ********* begin load process ********* ");
			// }
			// scriptFile.println("-- ********* begin load " + fileName + "
			// ********* ");
			// LOGGER.info("-- ********* begin load " + fileName + " *********
			// ");

			// processDisplayNames();
			processData(true, this.engine, helper, sqlDataTypes, Arrays.asList(headers), metamodelProps);
			logger.info(stepCounter + ". Complete");
			stepCounter++;
			// scriptFile.println("-- ********* completed processing file " +
			// fileName + " ********* ");
			// LOGGER.info("-- ********* completed processing file " + fileName

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (helper != null) {
				helper.clear();
			}
			clearTables();
		}
		cleanUpDBTables(this.engine, allowDuplicates);

		/*
		 * Back to normal app flow
		 */
		logger.info(stepCounter + ". Commit app metadata...");
		owler.commit();
		owler.export();
		this.engine.setOWL(owler.getOwlPath());
		// if(scriptFile != null) {
		// scriptFile.println("-- ********* completed load process ********* ");
		// scriptFile.close();
		// }
		// and rename .temp to .smss
		logger.info(stepCounter + ". Complete...");
		stepCounter++;

		logger.info(stepCounter + ". Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		this.engine.setInsightDatabase(insightDatabase);
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(this.engine, owler);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Save csv metamodel prop file");
		UploadUtilities.createPropFile(newAppId, newAppName, filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

	}

	public void addToExistingApp(final String appId, final String filePath) throws Exception {
		if (!(this.engine instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be using a relational database");
		}
		queryUtil = SQLQueryUtil.initialize(((RDBMSNativeEngine) this.engine).getDbType());
		
		int stepCounter = 1;
		logger.info(stepCounter + ". Get app upload input...");
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		boolean allowDuplicates = false;
		final boolean clean = UploadInputUtility.getClean(this.store);
		// get metamodel
		Map<String, Object> metamodelProps = UploadInputUtility.getMetamodelProps(this.store);
		Map<String, String> dataTypesMap = (Map<String, String>) metamodelProps.get(Constants.DATA_TYPES);;
		logger.info(stepCounter + ". Done...");
		stepCounter++;

		logger.info(stepCounter + ". Parsing file metadata...");
		OWLER owler = new OWLER(this.engine, this.engine.getOWL());
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.NEW_HEADERS));
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(helper, dataTypesMap, (Map<String, String>) metamodelProps.get(UploadInputUtility.ADDITIONAL_DATA_TYPES) );
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] sqlDataTypes = parseMetamodel(metamodelProps, owler, Arrays.asList(headers), types);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Start loading existing data to rdbms app
		 */
		logger.info(stepCounter + ". Start loading data..");
		try {
			// openScriptFile(appId);
			findIndexes(this.engine);
			// if(i ==0 ) {
			// scriptFile.println("-- ********* begin load process *********
			// ");
			// }
			// scriptFile.println("-- ********* begin load " + fileName + "
			// ********* ");
			// LOGGER.info("-- ********* begin load " + fileName + "
			// ********* ");

			// processDisplayNames();

			processData(false, this.engine, this.helper, sqlDataTypes, Arrays.asList(headers), metamodelProps);

			// scriptFile.println("-- ********* completed processing file "
			// + fileName + " ********* ");
			// LOGGER.info("-- ********* completed processing file " +
			// fileName + " ********* ");
		}
		// catch (IOException e) {
		// e.printStackTrace();
		// }
		finally {
			if (this.helper != null) {
				this.helper.clear();
			}
			clearTables();
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Back to normal app flow
		 */

		logger.warn(stepCounter + ". Committing app metadata....");
		owler.commit();
		owler.export();
		logger.info(stepCounter + ". Complete...");
		stepCounter++;

		addOriginalIndices(this.engine);
		cleanUpDBTables(this.engine, allowDuplicates);

		logger.info(stepCounter + ". Start generating default app insights");
		RDBMSEngineCreationHelper.insertNewTablesAsInsights(this.engine, owler, addedTables);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Save csv metamodel prop file");
		UploadUtilities.createPropFile(appId, this.engine.getEngineName(), filePath, metamodelProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;
	}

	private void addOriginalIndices(IEngine engine) throws Exception {
		// add back original
		for (String query : recreateIndexList) {
			insertData(engine, query);
		}
	}

	private String retrieveSqlType(String string) {
		String sqlType = this.sqlHash.get(string);
		if (sqlType == null) {
			// this should never happen...
			System.err.println("Need to add ui data type option, " + string + ", into sql hash");
			sqlType = "VARCHAR(800)";
		}
		return sqlType;
	}

	private String[] parseMetamodel(Map<String, Object> metamodel, OWLER owler, List<String> headers, SemossDataType[] types) {
		RdbmsUploadReactorUtility.createSQLTypes(this.sqlHash);
		// create the data types list
		String[] sqlDataTypes = new String[headers.size()];
		for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
			// fill in the column to index
			SemossDataType uiType = types[colIndex];
			String sqlType = retrieveSqlType(uiType.toString());
			sqlDataTypes[colIndex] = sqlType;
		}

		if (metamodel.get(Constants.NODE_PROP) != null) {
			Map<String, Object> nodeProps = (Map<String, Object>) metamodel.get(Constants.NODE_PROP);
			for (String concept : nodeProps.keySet()) {
				String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(concept);
				Map<String, String> propMap = null;
				if (concepts.containsKey(concept)) {
					propMap = concepts.get(concept);
				} else {
					propMap = new LinkedHashMap<String, String>();
					// it could be a concat
					// in which case, it is a string
					if (concept.contains("+")) {
						propMap.put(concept, "VARCHAR(2000)");
					} else if (headers.contains(concept)) {
						propMap.put(concept, sqlDataTypes[headers.indexOf(concept)]);
					} else if (objectTypeMap.containsKey(concept)) {
						// If not contained in the csv, go with type from the
						// objectTypeMap
						propMap.put(concept, retrieveSqlType(objectTypeMap.get(concept)));
					} else {
						// Default to string
						propMap.put(concept, "VARCHAR(800)");
					}
					// need to add the actual concept as a column
					owler.addConcept(cleanConceptTableName, propMap.get(concept).trim());
				}
				// properties
				List<String> conceptProps = (List<String>) nodeProps.get(concept);
				for (String prop : conceptProps) {
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					// it could be a concat
					// in which case, it is a string
					if (prop.contains("+")) {
						propMap.put(prop, "VARCHAR(2000)");
					} else if (headers.contains(prop)) {
						propMap.put(prop, sqlDataTypes[headers.indexOf(prop)]);
					} else if (objectTypeMap.containsKey(prop)) {
						// If not contained in the csv, go with type from the
						// objectTypeMap
						propMap.put(prop, retrieveSqlType(objectTypeMap.get(prop)));
					} else {
						// Default to string
						propMap.put(prop, "VARCHAR(800)");
					}
					owler.addProp(cleanConceptTableName, cleanProp, propMap.get(prop).trim());
				}

				concepts.put(concept, propMap);

			}
		}

		if (metamodel.get(Constants.RELATION) != null) {
			List<Map<String, Object>> edgeList = (List<Map<String, Object>>) metamodel.get(Constants.RELATION);
			// process each relationship
			for (Map relMap : edgeList) {
				String fromConcept = (String) relMap.get(Constants.FROM_TABLE);
				String toConcept = (String) relMap.get(Constants.TO_TABLE);
				String rel = (String) relMap.get(Constants.REL_NAME);
				String cleanFromConceptTableName = RDBMSEngineCreationHelper.cleanTableName(fromConcept);
				String cleanToConceptTableName = RDBMSEngineCreationHelper.cleanTableName(toConcept);
				// need to make sure all concepts are there
				if (!concepts.containsKey(fromConcept)) {
					Map<String, String> propMap = new LinkedHashMap<String, String>();
					// don't forget to add the actual column as well
					if (fromConcept.contains("+")) {
						propMap.put(fromConcept, "VARCHAR(2000)");
					} else if (headers.contains(fromConcept)) {
						propMap.put(fromConcept, sqlDataTypes[headers.indexOf(fromConcept)]);
					} else if (objectTypeMap.containsKey(fromConcept)) {
						// If not contained in the csv, go with type from the
						// objectTypeMap
						propMap.put(fromConcept, retrieveSqlType(objectTypeMap.get(fromConcept)));
					} else {
						// Default to string
						propMap.put(fromConcept, "VARCHAR(800)");
					}
					owler.addConcept(cleanFromConceptTableName, propMap.get(fromConcept));
					concepts.put(fromConcept, propMap);
				}
				if (!concepts.containsKey(toConcept)) {
					Map<String, String> propMap = new LinkedHashMap<String, String>();
					// don't forget to add the actual column as well
					if (toConcept.contains("+")) {
						propMap.put(toConcept, "VARCHAR(2000)");
					} else if (headers.contains(toConcept)) {
						propMap.put(toConcept, sqlDataTypes[headers.indexOf(toConcept)]);
					} else if (objectTypeMap.containsKey(toConcept)) {
						// If not contained in the csv, go with type from the
						// objectTypeMap
						propMap.put(toConcept, retrieveSqlType(objectTypeMap.get(toConcept)));
					} else {
						// Default to string
						propMap.put(toConcept, "VARCHAR(800)");
					}
					owler.addConcept(cleanToConceptTableName, propMap.get(toConcept));
					concepts.put(toConcept, propMap);
				}

				// add relationships
				List<String> relList = null;
				String predicate = cleanToConceptTableName + "." + cleanFromConceptTableName + FK + "."
						+ cleanFromConceptTableName + "." + cleanFromConceptTableName;
				// determine order based on *
				// if it is fromConcept *has toConcept
				// then toConcept has the FK for fromConcept
				if (rel.startsWith("*")) {
					if (relations.containsKey(toConcept)) {
						relList = relations.get(toConcept);
					} else {
						relList = new Vector<String>();
					}
					relList.add(fromConcept);
					relations.put(toConcept, relList);
					// add FK as property
					// String dataType = null;
					// if (csvColumnToIndex.containsKey(toConcept)) {
					// dataType = dataTypes[csvColumnToIndex.get(toConcept)];
					// } else if (objectTypeMap.containsKey(toConcept)) {
					// // If not contained in the csv, go with type from the
					// objectTypeMap dataType =
					// retrieveSqlType(objectTypeMap.get(toConcept));
					// } else {
					// // Default to string
					// dataType = "VARCHAR(800)";
					// }
					// owler.addProp(cleanToConceptTableName,
					// cleanToConceptTableName, cleanFromConceptTableName + FK,
					// dataType, null);

				}
				// if it is fromConcept has* toConcept
				// then fromConcept has the FK for toConcept
				else if (rel.endsWith("*")) {
					if (relations.containsKey(fromConcept)) {
						relList = relations.get(fromConcept);
					} else {
						relList = new Vector<String>();
					}
					relList.add(toConcept);
					relations.put(fromConcept, relList);

					// the predicate string is different from the default defined
					predicate = cleanToConceptTableName + "." + cleanToConceptTableName + "."
							+ cleanFromConceptTableName + "." + cleanToConceptTableName + FK;

					// add FK as property
					// String dataType = null;
					// if (csvColumnToIndex.containsKey(toConcept)) {
					// dataType = dataTypes[csvColumnToIndex.get(toConcept)];
					// } else if (objectTypeMap.containsKey(toConcept)) {
					// // If not contained in the csv, go with type from the
					// objectTypeMap dataType =
					// retrieveSqlType(objectTypeMap.get(toConcept));
					// } else {
					// // Default to string
					// dataType = "VARCHAR(800)";
					// }
					// owler.addProp(cleanFromConceptTableName,
					// cleanFromConceptTableName, cleanToConceptTableName + FK,
					// dataType, null);

				} else {
					// do it based on relationship format
					// TODO: build it out to do it based on
					// number of unique instances in each row
					if (relations.containsKey(toConcept)) {
						relList = relations.get(toConcept);
					} else {
						relList = new Vector<String>();
					}
					relList.add(fromConcept);
					relations.put(toConcept, relList);

					// add FK as property
					// String dataType = null;
					// if (csvColumnToIndex.containsKey(toConcept)) {
					// dataType = dataTypes[csvColumnToIndex.get(toConcept)];
					// } else if (objectTypeMap.containsKey(toConcept)) {
					// // If not contained in the csv, go with type from the
					// objectTypeMap dataType =
					// retrieveSqlType(objectTypeMap.get(toConcept));
					// } else {
					// // Default to string
					// dataType = "VARCHAR(800)";
					// }
					// owler.addProp(cleanToConceptTableName,
					// cleanToConceptTableName, cleanFromConceptTableName + FK,
					// dataType, null);
				}

				owler.addRelation(cleanFromConceptTableName, cleanToConceptTableName, predicate);
			}
		}
		return sqlDataTypes;
	}

	private void processData(boolean noExistingData, IEngine engine, CSVFileHelper helper, String[] sqlDataTypes, List<String> headers, Map<String, Object> metamodel) throws Exception {
		// get existing data is present
		if (!noExistingData) {
			existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
		} else {
			existingRDBMSStructure = new Hashtable<String, Map<String, String>>();
		}

		Set<String> conceptKeys = concepts.keySet();
		for (String concept : conceptKeys) {
			String cleanConcept = RDBMSEngineCreationHelper.cleanTableName(concept);
			// based on existing structure if present, see if you need to alter
			// the table
			if (existingRDBMSStructure.containsKey(cleanConcept.toUpperCase())) {
				alterTable(engine, concept);
			}
			// table does not exist, easy, just create new one with columns
			else {
				createTable(engine, concept);
			}
		}
		processCSVTable(noExistingData, engine, helper, sqlDataTypes, headers, metamodel);
	}

	private void processCSVTable(boolean noExistingData, IEngine engine, CSVFileHelper csvHelper, String[] sqlDataTypes, List<String> headers, Map<String, Object> metamodel) throws Exception {
		logger.setLevel(Level.INFO);
//		long start = System.currentTimeMillis();

		// Reset rowCounter to 0
		rowCounter = 0;
//		long lastTimeCheck = start;
		Map<String, String> defaultInsertStatements = getDefaultInsertStatements();
		String[] values = null;
		// skip rows
		int startRow = (int) metamodel.get(Constants.START_ROW);
		// start count at 1 just row 1 is the header
		int count = 1;
		while (count < startRow - 1 && csvHelper.getNextRow() != null) {
			count++;
		}
		Integer endRow = (Integer) metamodel.get(Constants.END_ROW);
		if (endRow == null) {
			endRow = UploadInputUtility.END_ROW_INT;
		}
		while ((values = csvHelper.getNextRow()) != null && count < endRow) {
			// Increment the rowCounter by 1
			rowCounter += 1;
			if (rowCounter % 10000 == 0) {
//				logger.info(">>>>>Processing row " + rowCounter + ", elapsed time: " + (System.currentTimeMillis() - lastTimeCheck) / 1000 + " sec");
//				lastTimeCheck = System.currentTimeMillis();
				logger.info("Done inserting " + count + " number of rows");
			}

			// loop through all the concepts
			for (String concept : concepts.keySet()) {
				String defaultInsert = defaultInsertStatements.get(concept);
				String sqlQuery = null;
				if (noExistingData) {
					// this is the easiest case, just add all the information
					sqlQuery = createInsertStatement(concept, defaultInsert, values, sqlDataTypes, headers);
				} else {
					// if the concept is a brand new table, easy, just add
					if (!existingRDBMSStructure.containsKey(concept.toUpperCase())) {
						sqlQuery = createInsertStatement(concept, defaultInsert, values, sqlDataTypes, headers);
					}
					// other case is if the table is there,
					// but it hasn't been altered, also just add
					else if (existingTableWithAllColsAccounted.contains(concept) || objectValueMap.containsKey(concept)) {
						sqlQuery = createInsertStatement(concept, defaultInsert, values, sqlDataTypes, headers);
					} else {
						// here we add the logic if we should perform an update
						// query vs. an insert query
						sqlQuery = getAlterQuery(engine, concept, values, defaultInsert, sqlDataTypes, headers);
						if (!sqlQuery.isEmpty()) {
							insertData(engine, sqlQuery);
						}
					}
				}
				if (sqlQuery != null && !sqlQuery.isEmpty()) {
					insertData(engine, sqlQuery);
				}
			}
			count++;
		}
		logger.info("Completed " + count + " number of rows");
		metamodel.put(Constants.END_ROW, count);
		// delete the indexes created and clear the arrays
		dropTempIndicies(engine);
		tempIndexDropList.clear();// clear the drop index sql text
		tempIndexAddedList.clear();// clear the index array text
	}

	private void dropTempIndicies(IEngine engine) throws Exception {
		// drop all added indices
		for (String query : tempIndexDropList) {
			insertData(engine, query);
		}
	}

	private void insertData(IEngine engine, String sqlQuery) throws Exception {
		if (!sqlQuery.endsWith(";")) {
			sqlQuery += ";";
		}
		// scriptFile.println(sql);
		engine.insertData(sqlQuery);

	}

	private Object getProperlyFormatedForQuery(String colName, Object value, String type) {
		if (value == null || value.toString().isEmpty()) {
			return "null";
		}
		type = type.toUpperCase();
		if (type.contains("VARCHAR")) {
			return "'" + RdbmsQueryBuilder.escapeForSQLStatement(Utility.cleanString(value.toString(), true)) + "'";
		} else if (Utility.isNumericType(type)) {
			return value;
		} else if (type.contains("DATE")) {
			return "'" + value + "'";
		}
		return "'" + RdbmsQueryBuilder.escapeForSQLStatement(value.toString()) + "'";
	}

	private String createInsertStatement(String concept, String defaultInsertQuery, String[] values,
			String[] sqlDataTypes, List<String> headers) {
		StringBuilder insertQuery = new StringBuilder(defaultInsertQuery);
		Map<String, String> propMap = concepts.get(concept);
		String type = propMap.get(concept);
		Object propertyValue = createObject(concept, values, sqlDataTypes, headers);
		insertQuery.append(getProperlyFormatedForQuery(concept, propertyValue, type));

		for (String prop : propMap.keySet()) {
			if (prop.equals(concept)) {
				continue;
			}
			insertQuery.append(",");

			type = propMap.get(prop);
			propertyValue = createObject(prop, values, sqlDataTypes, headers);
			insertQuery.append(getProperlyFormatedForQuery(prop, propertyValue, type));
		}

		if (relations.containsKey(concept)) {
			List<String> rels = relations.get(concept);
			for (String rel : rels) {
				insertQuery.append(",");

				String relColType = concepts.get(rel).get(rel);
				propertyValue = createObject(rel, values, sqlDataTypes, headers);
				insertQuery.append(getProperlyFormatedForQuery(rel, propertyValue, relColType));
			}
		}

		insertQuery.append(");");
		return insertQuery.toString();
	}

	private String getAlterQuery(IEngine engine, String concept, String[] rowValues, String defaultInsert,
			String[] sqlDataTypes, List<String> headers) throws Exception {
		String cleanConcept = RDBMSEngineCreationHelper.cleanTableName(concept);

		// get all the columns to be added for the table
		Map<String, String> columns = concepts.get(concept);
		// get all the rels to be added for the table
		List<String> rels = relations.get(concept);
		String conceptType = columns.get(concept);

		// here we loop through the values to get portions of strings that would
		// be used in query generation
		// assume we have in jcrMap {key1 -> value1, key2 -> value2, key3 ->
		// value3
		// where key1-key3 are columns and value1-value3 are values
		// for simplicity, assume the keys are already cleaned
		// at the end of this routine the following would be in each buffer:
		// valuesBuffer : "key1 = value1, key2 = value2, key3 = value3"
		// selectClauseWhereBuffer : "key1 IS NULL AND key2 IS NULL AND key3 IS
		// NULL"
		// insertValsClauseBuffer : "value1 AS key1, value2 as key2, value3 as
		// key3"
		// the list insertValsAliasClause contains the clean values for the
		// keys, but all upper cased

		StringBuffer valuesBuffer = new StringBuffer();
		StringBuffer selectClauseWhereBuffer = new StringBuffer();
		StringBuffer insertValsClauseBuffer = new StringBuffer();
		List<String> insertValsAliasClause = new ArrayList<String>();

		// we need to do the above for all the columns
		for (String col : columns.keySet()) {
			if (col.equals(concept)) {
				continue;
			}
			String cleanCol = RDBMSEngineCreationHelper.cleanTableName(col);
			String type = columns.get(col);
			Object value = getProperlyFormatedForQuery(col, createObject(col, rowValues, sqlDataTypes, headers), type);

			if (valuesBuffer.toString().length() == 0) {
				valuesBuffer.append(cleanCol + " = " + value);
				selectClauseWhereBuffer.append(cleanCol + " IS NULL ");
				insertValsClauseBuffer.append(value + " AS " + cleanCol);
				insertValsAliasClause.add(cleanCol.toUpperCase());
			} else {
				valuesBuffer.append(" , " + cleanCol + " = " + value);
				selectClauseWhereBuffer.append(" AND " + cleanCol + " IS NULL ");
				insertValsClauseBuffer.append(" , " + value + " AS " + cleanCol);
				insertValsAliasClause.add(cleanCol.toUpperCase());
			}
		}
		// and we also need to do it for all the relationships on the table
		if (rels != null) {
			for (String rel : rels) {
				String cleanRel = RDBMSEngineCreationHelper.cleanTableName(rel) + FK;
				String type = concepts.get(rel).get(rel);
				Object value = getProperlyFormatedForQuery(rel, createObject(rel, rowValues, sqlDataTypes, headers),
						type);
				if (valuesBuffer.toString().length() == 0) {
					valuesBuffer.append(cleanRel + " = " + value);
					selectClauseWhereBuffer.append(cleanRel + " IS NULL ");
					insertValsClauseBuffer.append(value + " AS " + cleanRel);
					insertValsAliasClause.add(cleanRel.toUpperCase());
				} else {
					valuesBuffer.append(" , " + cleanRel + " = " + value);
					selectClauseWhereBuffer.append(" AND " + cleanRel + " IS NULL ");
					insertValsClauseBuffer.append(" , " + value + " AS " + cleanRel);
					insertValsAliasClause.add(cleanRel.toUpperCase());
				}
			}
		}

		// this whereBuffer contains the string "concept = instance_value"
		StringBuffer whereBuffer = new StringBuffer();
		Object value = getProperlyFormatedForQuery(concept, createObject(concept, rowValues, sqlDataTypes, headers),
				conceptType);
		whereBuffer.append(cleanConcept).append(" = ").append(value);
		HashMap<String, String> whereValues = new HashMap<String, String>();
		whereValues.put(cleanConcept, value + "");

		// create indices to speed up querying
		createIndices(engine, cleanConcept, cleanConcept);

		// here, we execute a query to see if we need to perform an update query
		// or an insert query
		boolean isInsert = false;
		// "SELECT COUNT(1) AS ROWCOUNT FROM " + tableKey + " WHERE " +
		// whereclause;
		String getRowCountQuery = "";
		String selectClauseWhere = selectClauseWhereBuffer.toString();
		if (selectClauseWhere.isEmpty()) {
			getRowCountQuery = queryUtil.getDialectSelectRowCountFrom(cleanConcept, whereBuffer.toString());
		} else {
			getRowCountQuery = queryUtil.getDialectSelectRowCountFrom(cleanConcept,
					whereBuffer.toString() + " AND " + selectClauseWhereBuffer.toString());
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getRowCountQuery);
		if (wrapper.hasNext()) {
			String rowcount = wrapper.next().getValues()[0].toString();
			if (rowcount.equals("0")) {
				isInsert = true;
			}
		}

		String values = valuesBuffer.toString();
		if (isInsert) {
			if (values.length() == 0) {
				// this is the odd case when no columns are being added, but we
				// need to insert the instance by itself
				return createInsertStatement(concept, defaultInsert, rowValues, sqlDataTypes, headers);
			} else {
				// doing a distinct here, alternatively we can just let it
				// create all the dups later
				String allColumnsString = "";
				ArrayList<String> columnList = new ArrayList<String>();
				String insertIntoClause = "";

				// list of columns that we were going to update but are going to
				// be the varying value when it comes to the insert statement
				for (String singleClause : insertValsAliasClause) {
					if (insertIntoClause.length() > 0) {
						insertIntoClause += " , ";
					}
					insertIntoClause += singleClause;
				}

				// get all the columns you'll need for the insert statement
				Set<String> cols = new HashSet<String>();
				for (String s : columns.keySet()) {
					cols.add(RDBMSEngineCreationHelper.cleanTableName(s).toUpperCase());
				}
				if (rels != null) {
					for (String s : rels) {
						cols.add((RDBMSEngineCreationHelper.cleanTableName(s) + FK).toUpperCase());
					}
				}
				Map<String, String> previousCols = existingRDBMSStructure.get(cleanConcept.toUpperCase());
				cols.addAll(previousCols.keySet());

				// we want to pull the value columns out of the insert clause
				// columns so that you only have the columns that are being
				// copied
				// and not the ones we are setting the individual values for (so
				// pulling out the the [xyz AS columnName] columns)
				for (String colToAdd : cols) {
					if (!insertValsAliasClause.contains(colToAdd)) {
						if (!colToAdd.equalsIgnoreCase(cleanConcept)) {
							allColumnsString += colToAdd + ", ";
							columnList.add(colToAdd);
						}
					}
				}
				allColumnsString += cleanConcept;
				// now add the columns that you pulled out of the allColumns
				// string back in
				// (doing it this way because we can control the order of the
				// insert and select clause) since the ORDER IS VERY IMPORTANT
				// HERE
				String insertIntoClauseValues = "";
				if (allColumnsString.length() > 0) {
					insertIntoClause = " , " + insertIntoClause;
				}
				if (allColumnsString.length() > 0) {
					insertIntoClauseValues = " , " + insertValsClauseBuffer.toString();
				}
				insertIntoClause = allColumnsString + insertIntoClause;
				insertIntoClauseValues = allColumnsString + insertIntoClauseValues;
				return queryUtil.getDialectMergeStatement(cleanConcept, insertIntoClause, columnList, whereValues,
						insertValsClauseBuffer.toString(), whereBuffer.toString());
			}
		}
		// this is the easy case when it is an update query
		// construct it based on the values present and with the provided
		// instance
		else {
			if (values.length() == 0) {
				return "";
			} else {
				StringBuilder sqlQuery = new StringBuilder();
				sqlQuery.append("UPDATE ").append(cleanConcept).append(" SET ").append(values).append(" WHERE ")
						.append(" ( ").append(whereBuffer.toString()).append(")");
				return sqlQuery.toString();
			}
		}
	}

	private void alterTable(IEngine engine, String concept) throws Exception {
		String cleanConcept = RDBMSEngineCreationHelper.cleanTableName(concept);
		// need to determine if the existing structure needs to be altered
		// get the current table structure
		Map<String, String> existingConceptTable = existingRDBMSStructure.get(cleanConcept.toUpperCase());

		// NOTE: THESE VALUES ARE NOT CLEANED
		// NOTE: RELS DO NOT HAVE _FK ATTACHED
		// get the information for the table that we are adding
		Map<String, String> newColsForConcept = concepts.get(concept);
		// also need to get the relationships we plan to add
		List<String> newRelsForConcept = relations.get(concept);

		// this map will store the column names and types for the alter table
		// logic
		Map<String, String> colsToAdd = new Hashtable<String, String>();

		for (String newCol : newColsForConcept.keySet()) {
			String cleanNewCol = RDBMSEngineCreationHelper.cleanTableName(newCol);
			// if this column doesn't exist, need to add it to the set to alter
			// for some reason, all existing metadata comes back upper case
			if (!existingConceptTable.containsKey(cleanNewCol.toUpperCase())) {
				colsToAdd.put(cleanNewCol, newColsForConcept.get(newCol));
			}
		}

		if (newRelsForConcept != null) {
			for (String newRel : newRelsForConcept) {
				String cleanNewRel = RDBMSEngineCreationHelper.cleanTableName(newRel) + FK;
				// if this column doesn't exist, need to add it to the set to
				// alter
				if (!existingConceptTable.containsKey(cleanNewRel.toUpperCase())) {
					String relColType = concepts.get(newRel).get(newRel);
					colsToAdd.put(cleanNewRel, relColType);
				}
			}
		}

		// given there is new information to process, create the alter and run
		// it
		if (!colsToAdd.isEmpty()) {
			StringBuilder alterSql = new StringBuilder("ALTER TABLE ");
			alterSql.append(cleanConcept).append(" ADD (");

			Set<String> cols = colsToAdd.keySet();
			int counter = 0;
			int size = cols.size();
			for (String newCol : cols) {
				alterSql.append(newCol).append(" ").append(colsToAdd.get(newCol));
				if (counter + 1 != size) {
					alterSql.append(", ");
				}
				counter++;
			}
			alterSql.append(")");

			logger.info("ALTER TABLE SQL: " + alterSql.toString());
			insertData(engine, alterSql.toString());
		} else {
			// see if all the columns are accoutned for in the table
			if (newRelsForConcept == null) {
				newRelsForConcept = new ArrayList<String>();
			}
			if (existingConceptTable.keySet().size() == (newColsForConcept.keySet().size() + newRelsForConcept.size())) {
				existingTableWithAllColsAccounted.add(concept);
			} else {
				logger.warn("Number of existing concepts not equal to new cols plus new rels");
			}
		}
	}

	private void createTable(IEngine engine, String concept) throws Exception {
		Map<String, String> propMap = concepts.get(concept);
		String cleanConceptName = RDBMSEngineCreationHelper.cleanTableName(concept);
		String conceptType = propMap.get(concept);

		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TABLE ").append(cleanConceptName).append(" ( ").append(cleanConceptName).append(" ")
				.append(conceptType);

		// want the first column to be the concept, so now remove it from
		// propMap... other prop order doesn't matter as much
		propMap.remove(concept);

		for (String prop : propMap.keySet()) {
			String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
			String propType = propMap.get(prop);
			sqlBuilder.append(", ").append(cleanProp).append(" ").append(propType);
		}

		if (relations.containsKey(concept)) {
			List<String> rels = relations.get(concept);
			for (String rel : rels) {
				String cleanRel = RDBMSEngineCreationHelper.cleanTableName(rel) + FK;
				String relColType = concepts.get(rel).get(rel);
				sqlBuilder.append(", ").append(cleanRel).append(" ").append(relColType);
			}
		}

		// add back the concept as a column to propMap
		propMap.put(concept, conceptType);

		sqlBuilder.append(")");
		logger.info("CREATE TABLE SQL: " + sqlBuilder.toString());
		insertData(engine, sqlBuilder.toString());

		addedTables.add(cleanConceptName.toUpperCase());
	}

	/**
	 * Gets the properly formatted object from the string[] values object. Since
	 * this just goes into a sql insert query, the object is always in string
	 * format Also handles if the column is a concatenation
	 * 
	 * @param object
	 *            The column to get the correct data type for - can be a
	 *            concatenation
	 * @param values
	 *            The string[] containing the values for the row
	 * @param sqlDataType
	 *            The string[] containing the data type for each column in the
	 *            values array
	 * @param headers
	 *            The headers
	 * @return The object in the correct format
	 */
	private Object createObject(String object, String[] values, String[] sqlDataType, List<String> headers) {
		if (object.contains("+")) {
			return parseConcatenatedObject(object, values, headers);
		}

		// If the object doesn't exist as a column name, try to get it from the
		// objectMap
		if (!headers.contains(object)) {
			// Will throw a null pointer if the objectValueMap doesn't contain
			// the object
			String valueFromMap = objectValueMap.get(object);

			// Allow a value in the map to contain "+" (not recursive)
			if (valueFromMap.contains("+")) {
				return parseConcatenatedObject(valueFromMap, values, headers);
			} else if (objectTypeMap.containsKey(object)) {
				// If a type is specified in the objectTypeMap, then cast value
				// by type
				return castValueByType(valueFromMap, retrieveSqlType(objectTypeMap.get(object)));
			} else {
				// Default to String
				return Utility.cleanString(valueFromMap, true);
			}
		}

		int colIndex = headers.indexOf(object);
		return castValueByType(values[colIndex], sqlDataType[colIndex]);
	}

	private Object castValueByType(String strVal, String type) {
		// here we need to grab the value and cast it based on the type
		Object retObj = null;

		if (type.equals("INT")) {
			try {
				// added to remove $ and , in data and then try parsing as
				// Double
				int mult = 1;
				// this is a negativenumber
				if (strVal.startsWith("(") || strVal.startsWith("-"))
					mult = -1;
				strVal = strVal.replaceAll("[^0-9\\.]", "");
				return mult * Integer.parseInt(strVal.trim());
			} catch (NumberFormatException ex) {
				// do nothing
				return null;
			}
		} else if (type.equals("FLOAT")) {
			try {
				// added to remove $ and , in data and then try parsing as
				// Double
				int mult = 1;
				// this is a negativenumber
				if (strVal.startsWith("(") || strVal.startsWith("-"))
					mult = -1;
				strVal = strVal.replaceAll("[^0-9\\.]", "");
				return mult * Double.parseDouble(strVal.trim());
			} catch (NumberFormatException ex) {
				// do nothing
				return null;
			}
		} else if (type.equals("DATE")) {
			Long dTime = SemossDate.getTimeForDate(strVal);
			if (dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd");
			}
		} else if (type.equals("TIMESTAMP")) {
			Long dTime = SemossDate.getTimeForTimestamp(strVal);
			if (dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd HH:mm:ss");
			}
		} else {
			retObj = strVal;
		}

		return retObj;
	}

	// Method to parse objects with "+"
	private String parseConcatenatedObject(String object, String[] values, List<String> headers) {
		StringBuilder strBuilder = new StringBuilder();
		String[] objList = object.split("\\+");
		for (int i = 0; i < objList.length; i++) {
			// Allow for concatenated unique id
			if (headers.contains(objList[i])) {
				// Before just this line of code
				strBuilder.append(values[headers.indexOf(objList[i])]);
			} else if (objList[i].equals(rowKey)) {
				// Create a unique id by appending the zip file name, system and
				// row
				strBuilder.append(Integer.toString(rowCounter));
			} else {
				// Will throw a null pointer if the objectValueMap doesn't
				// contain the object
				strBuilder.append(objectValueMap.get(objList[i]));
			}
		}
		return Utility.cleanString(strBuilder.toString(), true);
	}

	private Map<String, String> getDefaultInsertStatements() {
		Map<String, String> retMap = new Hashtable<String, String>();
		for (String concept : concepts.keySet()) {
			StringBuilder sqlBuilder = new StringBuilder();
			Map<String, String> propMap = concepts.get(concept);
			String cleanConceptName = RDBMSEngineCreationHelper.cleanTableName(concept);
			sqlBuilder.append("INSERT INTO ").append(cleanConceptName).append(" ( ").append(cleanConceptName)
					.append(" ");
			for (String prop : propMap.keySet()) {
				if (prop.equals(concept)) {
					continue;
				}
				String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
				sqlBuilder.append(", ").append(cleanProp);
			}
			if (relations.containsKey(concept)) {
				List<String> rels = relations.get(concept);
				for (String rel : rels) {
					String cleanRel = RDBMSEngineCreationHelper.cleanTableName(rel) + FK;
					sqlBuilder.append(", ").append(cleanRel);
				}
			}
			sqlBuilder.append(") VALUES (");
			retMap.put(concept, sqlBuilder.toString());
		}
		return retMap;
	}

	private void cleanUpDBTables(IEngine engine, boolean allowDuplicates) throws Exception {
		logger.setLevel(Level.INFO);
		// fill up the availableTables and availableTablesInfo maps
		Map<String, Map<String, String>> existingStructure = RDBMSEngineCreationHelper
				.getExistingRDBMSStructure(engine);
		Set<String> alteredTables = allConcepts.keySet();
		Set<String> allTables = existingStructure.keySet();
		for (String tableName : allTables) {
			List<String> createIndex = new ArrayList<String>();
			Map<String, String> tableStruc = existingStructure.get(tableName);
			Set<String> columns = tableStruc.keySet();
			StringBuilder colsBuilder = new StringBuilder();
			colsBuilder.append(tableName);
			int indexCount = 0;
			for (String col : columns) {
				if (!col.equals(tableName)) {
					colsBuilder.append(", ").append(col);
				}
				// index should be created on each individual primary and
				// foreign key column
				if (col.equals(tableName) || col.endsWith(FK)) {
					createIndex.add(queryUtil.getDialectCreateIndex(tableName + "_INDX_" + indexCount, tableName, col));
					indexCount++;
				}
			}

			boolean tableAltered = false;
			for (String altTable : alteredTables) {
				if (tableName.equalsIgnoreCase(altTable)) {
					tableAltered = true;
					break;
				}
			}

			// do this duplicates removal for only the tables that were modified
			if (tableAltered) {
				if (!allowDuplicates) {
					// create new temporary table that has ONLY distinct values,
					// also make sure you are removing those null values from
					// the PK column
					String createTable = queryUtil.getDialectRemoveDuplicates(tableName, colsBuilder.toString());
					insertData(engine, createTable);
				}

				// check that the temp table was created before dropping the
				// table.
				String verifyTable = queryUtil.dialectVerifyTableExists(tableName + "_TEMP");
				// if temp table wasnt successfully created, go to the next
				// table.
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, verifyTable);
				if (wrapper.hasNext()) {
					String rowcount = wrapper.next().getValues()[0].toString();
					if (rowcount.equals("0")) {
						// This REALLY shouldnt happen, but its here just in case...
						logger.error("**** Error***** occurred during database clean up on table " + tableName);
						continue;
					}
				}

				if (!allowDuplicates) {
					// drop existing table
					// dropTable = "DROP TABLE " + tableName;
					String dropTable = queryUtil.getDialectDropTable(tableName);//
					insertData(engine, dropTable);

					// rename our temporary table to the new table name
					// alterTableName = "ALTER TABLE " + tableName + "_TEMP
					// RENAME TO " + tableName;
					String alterTableName = queryUtil.getDialectAlterTableName(tableName + "_TEMP", tableName);
					insertData(engine, alterTableName);
				}
			}

			// create indexes for ALL tables since we deleted all indexes before
			Long lastTimeCheck = System.currentTimeMillis();
			if (createIndexes) {
				logger.info("Creating indexes for " + tableName);
				for (String singleIndex : createIndex) {
					insertData(engine, singleIndex);
					logger.info(">>>>>" + singleIndex + ", elapsed time: " + (System.currentTimeMillis() - lastTimeCheck) / 1000 + " sec");
				}
			} else {
				logger.info("Will not create indexes for " + tableName);
			}
		}
	}

	private void clearTables() {
		// keep track of all the concepts that were modified during the loading
		// process
		allConcepts.putAll(concepts);
		concepts.clear();
		relations.clear();
		existingTableWithAllColsAccounted.clear();
		existingRDBMSStructure.clear();
	}

	private void createIndices(IEngine engine, String cleanTableKey, String indexStr) throws Exception {
		String indexOnTable = cleanTableKey + " ( " + indexStr + " ) ";
		String indexName = "INDX_" + cleanTableKey + indexUniqueId;
		String createIndex = "CREATE INDEX " + indexName + " ON " + indexOnTable;
		// "DROP INDEX " + indexName;
		String dropIndex = queryUtil.getDialectDropIndex(indexName, cleanTableKey);
		if (tempIndexAddedList.size() == 0) {
			insertData(engine, createIndex);
			tempIndexAddedList.add(indexOnTable);
			tempIndexDropList.add(dropIndex);
			indexUniqueId++;
		} else {
			boolean indexAlreadyExists = false;
			for (String index : tempIndexAddedList) {
				// TODO check various order of keys since they are comma
				// separated
				if (index.equals(indexOnTable)) {
					indexAlreadyExists = true;
					break;
				}

			}
			if (!indexAlreadyExists) {
				insertData(engine, createIndex);
				tempIndexDropList.add(dropIndex);
				tempIndexAddedList.add(indexOnTable);
				indexUniqueId++;
			}
		}
	}

	private void findIndexes(IEngine engine) throws Exception {
		// this gets all the existing tables
		String query = queryUtil.getDialectAllIndexesInDB(engine.getEngineId());
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while (wrapper.hasNext()) {
			String tablename = "";
			String dropCurrentIndexText = "";
			IHeadersDataRow rawArr = wrapper.next();
			Object[] values = rawArr.getValues();
			String indexName = values[0].toString();
			// only storing off custom indexes, recreating the non custom ones
			// on the fly on the cleanUpDBTables method

			String indexInfoQry = queryUtil.getDialectIndexInfo(indexName, engine.getEngineId());
			IRawSelectWrapper indexInfo = WrapperManager.getInstance().getRawWrapper(engine, indexInfoQry);
			List<String> columnsInIndex = new Vector<String>();
			String columnName = "";
			while (indexInfo.hasNext()) {
				IHeadersDataRow rawIndx = indexInfo.next();
				Object[] indxValues = rawIndx.getValues();
				tablename = indxValues[0].toString();
				columnName = indxValues[1].toString();
				columnsInIndex.add(columnName);
			}
			if (indexName.startsWith("CUST_")) {
				recreateIndexList.add(queryUtil.getDialectCreateIndex(indexName, tablename, columnsInIndex));
			}
			// drop all indexes, recreate the custom ones, the non custom ones
			// will be systematically recreated.
			dropCurrentIndexText = queryUtil.getDialectDropIndex(indexName, tablename);
			insertData(engine, dropCurrentIndexText);
		}
	}

	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
		}
	}


	/**
	 * Open script file that contains all the sql statements run during the
	 * upload process
	 * 
	 * @param engineName
	 *            name of the engine/db
	 */
	private void openScriptFile(String engineName) {
		// try {
		// String dbBaseFolder =
		// DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\",
		// System.getProperty("file.separator"));
		// scriptFileName = dbBaseFolder + "/db/" + engineName + "/" +
		// scriptFileName;
		// if (scriptFile == null) {
		// scriptFile = new PrintWriter(
		// new OutputStreamWriter(new FileOutputStream(new File(scriptFileName),
		// true)));// set
		// // append
		// // to
		// // true
		// }
		// } catch (FileNotFoundException e) {
		// e.printStackTrace();
		// }
	}

}
