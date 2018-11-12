package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.poi.main.helper.ImportOptions;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSReader extends AbstractCSVFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSReader.class.getName());

	// Need to create a unique id that includes the row number
	private int rowCounter;
	
	private Map<String, Map<String, String>> allConcepts = new LinkedHashMap<String, Map<String, String>>();
	private Map<String, Map<String, String>> concepts = new LinkedHashMap<String, Map<String, String>>();
	private Map<String, List<String>> relations = new LinkedHashMap <String, List<String>>();
	private Map<String, Map<String, String>> existingRDBMSStructure = new Hashtable<String, Map<String, String>>();
	private Set<String> existingTableWithAllColsAccounted = new HashSet<String>();
	private Set<String> addedTables = new HashSet<String>();

	private int indexUniqueId = 1;
	private List<String> recreateIndexList = new Vector<String>(); 
	private List<String> tempIndexAddedList = new Vector<String>();
	private List<String> tempIndexDropList = new Vector<String>();

	private final String FK = "_FK";

//	protected String scriptFileName = "DBScript.sql";
//	protected PrintWriter scriptFile = null;

	private void clearTables() {
		// keep track of all the concepts that were modified during the loading process
		allConcepts.putAll(concepts);
		concepts.clear();
		relations.clear();
		existingTableWithAllColsAccounted.clear();
		existingRDBMSStructure.clear();
	}

	/**
	 * Loading data into SEMOSS to create a new database
	 * @param smssLocation					The location of the smss file for the engine
	 * @param engineId					The name of the engine
	 * @param fileNames						String containing the file names semicolon delimited
	 * @param customBase					The custom base uri for the database
	 * @param owlFile						The location of the OWL file for the database
	 * @param dbType						The RDBMS data type
	 * @param allowDuplicates				Boolean is we should allow duplicated in the tables
	 * @return								The newly created engine
	 * @throws RepositoryException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SailException
	 * @throws Exception
	 */
	public IEngine importFileWithOutConnection(ImportOptions options) 
			throws RepositoryException, FileNotFoundException, IOException, SailException, Exception {
		LOGGER.setLevel(Level.WARN);

		String smssLocation = options.getSMSSLocation();
		String engineName = options.getDbName();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		String propertyFiles = options.getPropertyFiles();
		RdbmsTypeEnum dbType = options.getRDBMSDriverType();
		boolean allowDuplicates = options.isAllowDuplicates();
		String engineID = options.getEngineID();
		
		long start = System.currentTimeMillis();

		boolean error = false;
		queryUtil = SQLQueryUtil.initialize(dbType);
		String[] files = prepareCsvReader(fileNames, customBase, owlFile, smssLocation, propertyFiles);

		try {
			openRdbmsEngineWithoutConnection(engineName, engineID);
			openScriptFile(engineName);
			for(int i = 0; i<files.length;i++)
			{
				try {
					String fileName = files[i];
					// open the csv file
					// and get the headers
					openCSVFile(fileName);
					// load the prop file for the CSV file 
					if(propFileExist){
						// if we have multiple files, load the correct one
						if(propFiles != null) {
							propFile = propFiles[i];
						}
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					// use the rdf map to get the data types
					// that match the headers
					preParseRdbmsCSVMetaData(rdfMap);
					parseMetadata();

//					if(i ==0 ) {
//						scriptFile.println("-- ********* begin load process ********* ");
//					}
//					scriptFile.println("-- ********* begin load " + fileName + " ********* ");
//					LOGGER.info("-- ********* begin load " + fileName + " ********* ");

//					processDisplayNames();
					skipRows();
					processData(i == 0);

//					scriptFile.println("-- ********* completed processing file " + fileName + " ********* ");
//					LOGGER.info("-- ********* completed processing file " + fileName + " ********* ");

				} finally {
					closeCSVFile();
					clearTables();
				}
			}
			cleanUpDBTables(engineName, allowDuplicates);
			createBaseRelations();
			RDBMSEngineCreationHelper.insertAllTablesAsInsights(this.engine, this.owler);
		} catch(FileNotFoundException e) {
			error = true;
			throw new FileNotFoundException(e.getMessage());
		} catch(IOException e) {
			error = true;
			throw new IOException(e.getMessage());
		} finally {
			if(error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
			}
//			if(scriptFile != null) {
//				scriptFile.println("-- ********* completed load process ********* ");
//				scriptFile.close();
//			}
		}

		long end = System.currentTimeMillis();
		LOGGER.info((end - start)/1000 + " seconds to load...");

		return engine;
	}

	/**
	 * Load data into an existing relational database
	 * @param engineId				The name of the engine to add data into
	 * @param fileNames					The list of files to upload, semicolon delimited
	 * @param customBase				The base URI for the database
	 * @param owlFile					The path to the existing owl file... TODO: can grab this from the engine directly
	 * @param dbType					The type of relational database....		TODO: can grab this from the engine directly
	 * @param allowDuplicates			Boolean to allow duplicate rows in the tables
	 * @throws IOException
	 */
	public void importFileWithConnection(ImportOptions options) 
			throws IOException {
		
		long start = System.currentTimeMillis();

		String engineName = options.getDbName();
		String fileNames = options.getFileLocations();
		String customBase = options.getBaseUrl();
		String owlFile = options.getOwlFileLocation();
		RdbmsTypeEnum dbType = options.getRDBMSDriverType();
		String propertyFiles = options.getPropertyFiles();
		boolean allowDuplicates = options.isAllowDuplicates();

		queryUtil = SQLQueryUtil.initialize(dbType);
		String[] files = prepareCsvReader(fileNames, customBase, owlFile, engineName, propertyFiles);

		LOGGER.setLevel(Level.WARN);
		try {
			openEngineWithConnection(engineName);
			openScriptFile(engineName);
			findIndexes(engine.getEngineId());
			for(int i = 0; i<files.length;i++)
			{
				try {
					String fileName = files[i];

					// open the csv file
					openCSVFile(fileName);
					// load the prop file for the CSV file 
					if(propFileExist){
						// if we have multiple files, load the correct one
						if(propFiles != null) {
							propFile = propFiles[i];
						}
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					preParseRdbmsCSVMetaData(rdfMap);
					parseMetadata();

//					if(i ==0 ) {
//						scriptFile.println("-- ********* begin load process ********* ");
//					}
//					scriptFile.println("-- ********* begin load " + fileName + " ********* ");
//					LOGGER.info("-- ********* begin load " + fileName + " ********* ");
					
//					processDisplayNames();
					
					skipRows();
					processData(false);
					
//					scriptFile.println("-- ********* completed processing file " + fileName + " ********* ");
//					LOGGER.info("-- ********* completed processing file " + fileName + " ********* ");
				} finally {
					closeCSVFile();
					clearTables();
				}
			}
			createBaseRelations();
			addOriginalIndices();
			cleanUpDBTables(engineName, allowDuplicates);
			RDBMSEngineCreationHelper.insertNewTablesAsInsights(this.engine, this.owler, this.addedTables);
		} finally {
//			if(scriptFile != null) {
//				scriptFile.println("-- ********* completed load process ********* ");
//				scriptFile.close();
//			}
		}

		long end = System.currentTimeMillis();
		LOGGER.info((end - start)/1000 + " seconds to load...");
	}

	private void processData(boolean noExistingData) throws IOException {
		
		// get existing data is present
		if(!noExistingData) {
			existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
		} else {
			existingRDBMSStructure = new Hashtable<String, Map<String, String>>();
		}

		Set<String> conceptKeys = concepts.keySet();
		for(String concept : conceptKeys) {
			String cleanConcept = RDBMSEngineCreationHelper.cleanTableName(concept);
			// based on existing structure if present, see if you need to alter the table
			// for some reason, all existing metadata comes back upper case
			if(existingRDBMSStructure.containsKey(cleanConcept.toUpperCase())) {
				alterTable(concept);
			} 
			// table does not exist, easy, just create new one with columns
			else {
				createTable(concept);
			}
		}
		processCSVTable(noExistingData);
	}

	private void createTable(String concept) {
		Map<String, String> propMap = concepts.get(concept);

		String cleanConceptName = RDBMSEngineCreationHelper.cleanTableName(concept);
		String conceptType = propMap.get(concept);

		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TABLE ").append(cleanConceptName).append(" ( ").append(cleanConceptName).append(" ").append(conceptType);

		// want the first column to be the concept, so now remove it from propMap... other prop order doesn't matter as much
		propMap.remove(concept);

		for(String prop : propMap.keySet()) {
			String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
			String propType = propMap.get(prop);
			sqlBuilder.append(", ").append(cleanProp).append(" ").append(propType);
		}

		if(relations.containsKey(concept)) {
			List<String> rels = relations.get(concept);
			for(String rel : rels) {
				String cleanRel = RDBMSEngineCreationHelper.cleanTableName(rel) + FK;
				String relColType = concepts.get(rel).get(rel);
				sqlBuilder.append(", ").append(cleanRel).append(" ").append(relColType);
			}
		}

		// add back the concept as a column to propMap
		propMap.put(concept, conceptType);

		sqlBuilder.append(")");
		LOGGER.info("CREATE TABLE SQL: " + sqlBuilder.toString());
		insertData(sqlBuilder.toString());

		addedTables.add(cleanConceptName);
	}

	private void alterTable(String concept) {
		String cleanConcept = RDBMSEngineCreationHelper.cleanTableName(concept);
		// need to determine if the existing structure needs to be altered
		// get the current table structure

		// for some reason, all existing metadata comes back upper case
		Map<String, String> existingConceptTable = existingRDBMSStructure.get(cleanConcept.toUpperCase());

		// NOTE: THESE VALUES ARE NOT CLEANED
		// NOTE: RELS DO NOT HAVE _FK ATTACHED
		// get the information for the table that we are adding
		Map<String, String> newColsForConcept = concepts.get(concept);
		// also need to get the relationships we plan to add
		List<String> newRelsForConcept = relations.get(concept);

		// this map will store the column names and types for the alter table logic
		Map<String, String> colsToAdd = new Hashtable<String, String>();

		for(String newCol : newColsForConcept.keySet()) {
			String cleanNewCol = RDBMSEngineCreationHelper.cleanTableName(newCol);
			// if this column doesn't exist, need to add it to the set to alter
			// for some reason, all existing metadata comes back upper case
			if(!existingConceptTable.containsKey(cleanNewCol.toUpperCase())) {
				colsToAdd.put(cleanNewCol, newColsForConcept.get(newCol));
			}
		}

		if(newRelsForConcept != null) {
			for(String newRel : newRelsForConcept) {
				String cleanNewRel = RDBMSEngineCreationHelper.cleanTableName(newRel) + FK;
				// if this column doesn't exist, need to add it to the set to alter
				// for some reason, all existing metadata comes back upper case
				if(!existingConceptTable.containsKey(cleanNewRel.toUpperCase())) {
					String relColType = concepts.get(newRel).get(newRel);
					colsToAdd.put(cleanNewRel, relColType);
				}
			}
		}

		// given there is new information to process, create the alter and run it
		if(!colsToAdd.isEmpty()) {
			StringBuilder alterSql = new StringBuilder("ALTER TABLE ");
			alterSql.append(cleanConcept).append(" ADD (");

			Set<String> cols = colsToAdd.keySet();
			int counter = 0;
			int size = cols.size();
			for(String newCol : cols) {
				alterSql.append(newCol).append(" ").append(colsToAdd.get(newCol));
				if(counter + 1 != size) {
					alterSql.append(", ");
				}
				counter++;
			}
			alterSql.append(")");

			LOGGER.info("ALTER TABLE SQL: " + alterSql.toString());
			insertData(alterSql.toString());
		} else {
			// see if all the columns are accoutned for in the table
			if(newRelsForConcept == null) {
				newRelsForConcept = new ArrayList<String>();
			}
			if(existingConceptTable.keySet().size() == (newColsForConcept.keySet().size() + newRelsForConcept.size())) {
				existingTableWithAllColsAccounted.add(concept);
			} else {
				LOGGER.warn("Number of existing concepts not equal to new cols plus new rels");
			}
		}
	}

	private void processCSVTable(boolean noExistingData) throws IOException{
		LOGGER.setLevel(Level.INFO);
		long start = System.currentTimeMillis();
		
		// Reset rowCounter to 0
		rowCounter = 0;
		
		long lastTimeCheck = start;
		Map<String, String> defaultInsertStatements = getDefaultInsertStatements();
		String[] values = null;
		while( (values = csvHelper.getNextRow()) != null && count<maxRows )
		{
			// Increment the rowCounter by 1
			rowCounter += 1;
			
			if (rowCounter % 10000 == 0) {
				LOGGER.info(">>>>>Processing row " + rowCounter + ", elapsed time: " + (System.currentTimeMillis() - lastTimeCheck)/1000 + " sec");
				lastTimeCheck = System.currentTimeMillis();
			}
			
			// loop through all the concepts
			for(String concept : concepts.keySet()) {
				String defaultInsert = defaultInsertStatements.get(concept);

				String sqlQuery = null;
				if(noExistingData) {
					// this is the easiest case, just add all the information
					sqlQuery = createInsertStatement(concept, defaultInsert, values);
				} else {
					// if the concept is a brand new table, easy, just add
					// for some reason, all existing metadata comes back upper case
					if(!existingRDBMSStructure.containsKey(concept.toUpperCase())) {
						sqlQuery = createInsertStatement(concept, defaultInsert, values);
					} 
					// other case is if the table is there, but it hasn't been altered, also just add
					else if(existingTableWithAllColsAccounted.contains(concept) || objectValueMap.containsKey(concept)) {
						sqlQuery = createInsertStatement(concept, defaultInsert, values);
					} else {
						// here we add the logic if we should perform an update query vs. an insert query
						sqlQuery = getAlterQuery(concept, values, defaultInsert);
						if(!sqlQuery.isEmpty()) {
							insertData(sqlQuery);
						}
					}
				}
				if(sqlQuery != null && !sqlQuery.isEmpty()) {
					insertData(sqlQuery);
				}
			}
		}

		// delete the indexes created and clear the arrays
		dropTempIndicies();
		tempIndexDropList.clear();//clear the drop index sql text
		tempIndexAddedList.clear();//clear the index array text		
	}

	private String getAlterQuery(String concept, String[] rowValues, String defaultInsert) {
		String cleanConcept = RDBMSEngineCreationHelper.cleanTableName(concept);

		// get all the columns to be added for the table
		Map<String, String> columns = concepts.get(concept);
		// get all the rels to be added for the table
		List<String> rels = relations.get(concept);
		String conceptType = columns.get(concept);

		// here we loop through the values to get portions of strings that would be used in query generation
		// assume we have in jcrMap {key1 -> value1, key2 -> value2, key3 -> value3
		// where key1-key3 are columns and value1-value3 are values
		// for simplicity, assume the keys are already cleaned
		// at the end of this routine the following would be in each buffer:
		// valuesBuffer : "key1 = value1, key2 = value2, key3 = value3"
		// selectClauseWhereBuffer : "key1 IS NULL AND key2 IS NULL AND key3 IS NULL"
		// insertValsClauseBuffer : "value1 AS key1, value2 as key2, value3 as key3"
		// the list insertValsAliasClause contains the clean values for the keys, but all upper cased

		StringBuffer valuesBuffer = new StringBuffer();
		StringBuffer selectClauseWhereBuffer = new StringBuffer();
		StringBuffer insertValsClauseBuffer = new StringBuffer();
		List<String> insertValsAliasClause = new ArrayList<String>();

		// we need to do the above for all the columns
		for(String col : columns.keySet()) {
			if(col.equals(concept)) {
				continue;
			}
			String cleanCol = RDBMSEngineCreationHelper.cleanTableName(col);
			String type = columns.get(col);
			Object value = getProperlyFormatedForQuery(col, createObject(col, rowValues, dataTypes, csvColumnToIndex), type);

			if(valuesBuffer.toString().length() == 0) {
				valuesBuffer.append(cleanCol + " = " +  value); 
				selectClauseWhereBuffer.append( cleanCol + " IS NULL ");
				insertValsClauseBuffer.append(value + " AS " + cleanCol);
				insertValsAliasClause.add(cleanCol.toUpperCase());
			}
			else {
				valuesBuffer.append(" , " + cleanCol + " = " +  value);
				selectClauseWhereBuffer.append( " AND " + cleanCol + " IS NULL ");
				insertValsClauseBuffer.append(" , " + value + " AS " + cleanCol);	
				insertValsAliasClause.add(cleanCol.toUpperCase());
			}
		}
		// and we also need to do it for all the relationships on the table
		if(rels != null) {
			for(String rel : rels) {
				String cleanRel = RDBMSEngineCreationHelper.cleanTableName(rel) + FK;
				String type = concepts.get(rel).get(rel);
				Object value = getProperlyFormatedForQuery(rel, createObject(rel, rowValues, dataTypes, csvColumnToIndex), type);

				if(valuesBuffer.toString().length() == 0) {
					valuesBuffer.append(cleanRel + " = " +  value); 
					selectClauseWhereBuffer.append( cleanRel + " IS NULL ");
					insertValsClauseBuffer.append(value + " AS " + cleanRel);
					insertValsAliasClause.add(cleanRel.toUpperCase());
				}
				else {
					valuesBuffer.append(" , " + cleanRel + " = " +  value);
					selectClauseWhereBuffer.append( " AND " + cleanRel + " IS NULL ");
					insertValsClauseBuffer.append(" , " + value + " AS " + cleanRel);	
					insertValsAliasClause.add(cleanRel.toUpperCase());
				}
			}
		}

		// this whereBuffer contains the string "concept = instance_value"
		StringBuffer whereBuffer = new StringBuffer();
		Object value = getProperlyFormatedForQuery(concept, createObject(concept, rowValues, dataTypes, csvColumnToIndex), conceptType);
		whereBuffer.append(cleanConcept).append(" = ").append(value);
		HashMap<String, String> whereValues = new HashMap<String, String>();
		whereValues.put(cleanConcept, value + "");

		// create indices to speed up querying
		createIndices(cleanConcept, cleanConcept);

		// here, we execute a query to see if we need to perform an update query or an insert query
		boolean isInsert = false;
		//"SELECT COUNT(1) AS ROWCOUNT FROM " + tableKey + " WHERE " + whereclause;
		String getRowCountQuery = "";
		String selectClauseWhere = selectClauseWhereBuffer.toString();
		if(selectClauseWhere.isEmpty()) {
			getRowCountQuery = queryUtil.getDialectSelectRowCountFrom(cleanConcept, whereBuffer.toString());
		} else {
			getRowCountQuery = queryUtil.getDialectSelectRowCountFrom(cleanConcept, whereBuffer.toString() + " AND " + selectClauseWhereBuffer.toString());
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getRowCountQuery);
		if(wrapper.hasNext()){
			String rowcount = wrapper.next().getValues()[0].toString();
			if(rowcount.equals("0")){
				isInsert = true;
			}
		}

		String values = valuesBuffer.toString();
		if(isInsert){
			if(values.length()==0) {
				// this is the odd case when no columns are being added, but we need to insert the instance by itself
				return createInsertStatement(concept, defaultInsert, rowValues);
			} else {
				//doing a distinct here, alternatively we can just let it create all the dups later
				String allColumnsString = ""; 
				ArrayList<String> columnList = new ArrayList<String>();
				String insertIntoClause = "";

				//list of columns that we were going to update but are going to be the varying value when it comes to the insert statement
				for(String singleClause : insertValsAliasClause){
					if(insertIntoClause.length() > 0) insertIntoClause+=" , ";
					insertIntoClause += singleClause;
				}

				//get all the columns you'll need for the insert statement
				Set<String> cols = new HashSet<String>();
				for(String s : columns.keySet()) {
					cols.add(RDBMSEngineCreationHelper.cleanTableName(s).toUpperCase());
				}
				if(rels != null) {
					for(String s : rels) {
						cols.add((RDBMSEngineCreationHelper.cleanTableName(s) + FK).toUpperCase());
					}
				}
				Map<String, String> previousCols = existingRDBMSStructure.get(cleanConcept.toUpperCase());
				cols.addAll(previousCols.keySet());

				// we want to pull the value columns out of the insert clause columns so that you only have the columns that are being copied
				// and not the ones we are setting the individual values for (so pulling out the the [xyz AS columnName]  columns)
				for(String colToAdd : cols) {
					if(!insertValsAliasClause.contains(colToAdd)){
						if(!colToAdd.equalsIgnoreCase(cleanConcept)) {
							allColumnsString += colToAdd + ", ";
							columnList.add(colToAdd);
						}
					} 
				}
				allColumnsString += cleanConcept;

				//now add the columns that you pulled out of the allColumns string back in 
				//(doing it this way because we can control the order of the insert and select clause) since the ORDER IS VERY IMPORTANT HERE
				String insertIntoClauseValues = "";
				if(allColumnsString.length() > 0) {
					insertIntoClause = " , " + insertIntoClause; 
				}
				if(allColumnsString.length() > 0) {
					insertIntoClauseValues = " , " + insertValsClauseBuffer.toString(); 
				}
				insertIntoClause = allColumnsString + insertIntoClause;
				insertIntoClauseValues = allColumnsString + insertIntoClauseValues;

				return queryUtil.getDialectMergeStatement(cleanConcept, insertIntoClause, columnList, whereValues, insertValsClauseBuffer.toString(), whereBuffer.toString());
			}
		} 
		// this is the easy case when it is an update query
		// construct it based on the values present and with the provided instance
		else {
			if(values.length() == 0) {
				return "";
			} else {
				StringBuilder sqlQuery = new StringBuilder();
				sqlQuery.append("UPDATE ").append(cleanConcept).append(" SET ").append(values).append(" WHERE ").append(" ( ").append(whereBuffer.toString()).append(")");
				return sqlQuery.toString();
			}
		}
	}


	private void findIndexes(String engineName){
		// this gets all the existing tables
		String query = queryUtil.getDialectAllIndexesInDB(engineName);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext())
		{
			String tablename = "";
			String dropCurrentIndexText = "";
			IHeadersDataRow rawArr = wrapper.next();
			Object[] values = rawArr.getValues();
			String indexName = values[0].toString();
			//only storing off custom indexes, recreating the non custom ones on the fly on the cleanUpDBTables method

			String indexInfoQry = queryUtil.getDialectIndexInfo(indexName, engineName);
			IRawSelectWrapper indexInfo = WrapperManager.getInstance().getRawWrapper(engine, indexInfoQry);
			List<String> columnsInIndex = new Vector<String>();
			String columnName = "";
			while(indexInfo.hasNext()){
				IHeadersDataRow rawIndx = indexInfo.next();
				Object[] indxValues = rawIndx.getValues();
				tablename = indxValues[0].toString();
				columnName = indxValues[1].toString();
				columnsInIndex.add(columnName);
			}
			if(indexName.startsWith("CUST_")){
				recreateIndexList.add(queryUtil.getDialectCreateIndex(indexName,tablename,columnsInIndex));
			}
			//drop all indexes, recreate the custom ones, the non custom ones will be systematically recreated.
			dropCurrentIndexText = queryUtil.getDialectDropIndex(indexName, tablename);
			insertData(dropCurrentIndexText);
		}
	}

	private void createIndices(String cleanTableKey, String indexStr) {
		String indexOnTable =  cleanTableKey + " ( " +  indexStr + " ) ";
		String indexName = "INDX_" + cleanTableKey + indexUniqueId;
		String createIndex = "CREATE INDEX " + indexName + " ON " + indexOnTable;
		String dropIndex = queryUtil.getDialectDropIndex(indexName, cleanTableKey);//"DROP INDEX " + indexName;
		if(tempIndexAddedList.size() == 0 ){
			insertData(createIndex);
			tempIndexAddedList.add(indexOnTable);
			tempIndexDropList.add(dropIndex);
			indexUniqueId++;
		} else {
			boolean indexAlreadyExists = false; 
			for(String index : tempIndexAddedList){
				if(index.equals(indexOnTable)){ //TODO check various order of keys since they are comma seperated
					indexAlreadyExists = true;
					break;
				}

			}
			if(!indexAlreadyExists){
				insertData(createIndex);
				tempIndexDropList.add(dropIndex);
				tempIndexAddedList.add(indexOnTable);
				indexUniqueId++;
			}
		}
	}

	private void dropTempIndicies() {
		// drop all added indices
		for(String query : tempIndexDropList) {
			insertData(query);
		}
	}

	private void addOriginalIndices() {
		// add back original
		for(String query : recreateIndexList) {
			insertData(query);
		}
	}

	private void cleanUpDBTables(String engineName, boolean allowDuplicates){
		LOGGER.setLevel(Level.INFO);
		//fill up the availableTables and availableTablesInfo maps
		Map<String, Map<String, String>> existingStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
		Set<String> alteredTables = allConcepts.keySet();

		Set<String> allTables = existingStructure.keySet();
		for(String tableName : allTables) {
			List<String> createIndex = new ArrayList<String>();

			Map<String, String> tableStruc = existingStructure.get(tableName);
			Set<String> columns = tableStruc.keySet();

			StringBuilder colsBuilder = new StringBuilder();
			colsBuilder.append(tableName);
			int indexCount = 0;
			for(String col : columns) {
				if(!col.equals(tableName)) {
					colsBuilder.append(", ").append(col);
				}
				//index should be created on each individual primary and foreign key column
				if(col.equals(tableName) ||  col.endsWith(FK)){
					createIndex.add(queryUtil.getDialectCreateIndex(tableName + "_INDX_"+indexCount, tableName, col));
					indexCount++;
				}
			}
			
			boolean tableAltered = false;
			for(String altTable : alteredTables) {
				if(tableName.equalsIgnoreCase(altTable)) {
					tableAltered = true;
					break;
				}
			}

			//do this duplicates removal for only the tables that were modified
			if(tableAltered){
				if(!allowDuplicates){
					//create new temporary table that has ONLY distinct values, also make sure you are removing those null values from the PK column
					String createTable = queryUtil.getDialectRemoveDuplicates(tableName, colsBuilder.toString());
					insertData(createTable);
				}

				//check that the temp table was created before dropping the table.
				String verifyTable = queryUtil.dialectVerifyTableExists(tableName + "_TEMP"); //query here would return a row count 
				//if temp table wasnt successfully created, go to the next table.
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, verifyTable);
				if(wrapper.hasNext()){
					String rowcount = wrapper.next().getValues()[0].toString();
					if(rowcount.equals("0")){
						//This REALLY shouldnt happen, but its here just in case...
						LOGGER.error("**** Error***** occurred during database clean up on table " + tableName);
						continue;
					}
				}

				if(!allowDuplicates){
					//drop existing table
					String dropTable = queryUtil.getDialectDropTable(tableName);//dropTable = "DROP TABLE " + tableName;
					insertData(dropTable);

					//rename our temporary table to the new table name				
					String alterTableName = queryUtil.getDialectAlterTableName(tableName+"_TEMP", tableName); //alterTableName = "ALTER TABLE " + tableName + "_TEMP RENAME TO " + tableName;
					insertData(alterTableName);
				}
			}

			//create indexes for ALL tables since we deleted all indexes before
			Long lastTimeCheck = System.currentTimeMillis();
			if (createIndexes) {
				LOGGER.info("Creating indexes for " + tableName);
				for(String singleIndex: createIndex){
					insertData(singleIndex);
					LOGGER.info(">>>>>" + singleIndex + ", elapsed time: " + (System.currentTimeMillis() - lastTimeCheck)/1000 + " sec");
				}
			} else {
				LOGGER.info("Will not create indexes for " + tableName);
			}
		}
	}

	private String createInsertStatement(String concept, String defaultInsertQuery, String[] values) {

		StringBuilder insertQuery = new StringBuilder(defaultInsertQuery);
		Map<String, String> propMap = concepts.get(concept);
		String type = propMap.get(concept);
		Object propertyValue = createObject(concept, values, dataTypes, csvColumnToIndex);
		insertQuery.append(getProperlyFormatedForQuery(concept, propertyValue, type));

		for(String prop : propMap.keySet()) {
			if(prop.equals(concept)) {
				continue;
			}
			insertQuery.append(",");

			type = propMap.get(prop);
			propertyValue = createObject(prop, values, dataTypes, csvColumnToIndex);
			insertQuery.append(getProperlyFormatedForQuery(prop, propertyValue, type));
		}

		if(relations.containsKey(concept)) {
			List<String> rels = relations.get(concept);
			for(String rel : rels) {
				insertQuery.append(",");

				String relColType = concepts.get(rel).get(rel);
				propertyValue = createObject(rel, values, dataTypes, csvColumnToIndex);
				insertQuery.append(getProperlyFormatedForQuery(rel, propertyValue, relColType));
			}
		}

		insertQuery.append(");");
		return insertQuery.toString();
	}

	private Object getProperlyFormatedForQuery(String colName, Object value, String type) {
		if(value == null || value.toString().isEmpty()) {
			return "null";
		}

		type = type.toUpperCase();
		if(type.contains("VARCHAR")) {
			return "'" + RdbmsQueryBuilder.escapeForSQLStatement(Utility.cleanString(value.toString(), true)) + "'";
		} else if(type.contains("FLOAT") || type.contains("DECIMAL") || type.contains("DOUBLE") || type.contains("INT") || type.contains("LONG") || type.contains("BIGINT")
				|| type.contains("TINYINT") || type.contains("SMALLINT")){
			return value;
		} else  if(type.contains("DATE")) {
			return "'" + value + "'";
		}

		return "'" + RdbmsQueryBuilder.escapeForSQLStatement(value.toString()) + "'";
	}

	private Map<String, String> getDefaultInsertStatements() {
		Map<String, String> retMap = new Hashtable<String, String>();
		for(String concept : concepts.keySet()) {
			StringBuilder sqlBuilder = new StringBuilder();

			Map<String, String> propMap = concepts.get(concept);

			String cleanConceptName = RDBMSEngineCreationHelper.cleanTableName(concept);
			sqlBuilder.append("INSERT INTO ").append(cleanConceptName).append(" ( ").append(cleanConceptName).append(" ");

			for(String prop : propMap.keySet()) {
				if(prop.equals(concept)) {
					continue;
				}
				String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
				sqlBuilder.append(", ").append(cleanProp);
			}

			if(relations.containsKey(concept)) {
				List<String> rels = relations.get(concept);
				for(String rel : rels) {
					String cleanRel = RDBMSEngineCreationHelper.cleanTableName(rel) + FK;
					sqlBuilder.append(", ").append(cleanRel);
				}
			}

			sqlBuilder.append(") VALUES (");

			retMap.put(concept, sqlBuilder.toString());
		}

		return retMap;
	}

	/**
	 * Get the data types and the csvColumnToIndex maps ready for the file load
	 * I call this preParseMetaData since this is needed for parseMetaData as it needs
	 * to know what the types are for each column to properly add into OWL
	 * @param rdfMap
	 */
	private void preParseRdbmsCSVMetaData(Map<String, String> rdfMap) {
		if(sqlHash.isEmpty()) {
			createSQLTypes();
		}
		// create the data types list
		int numCols = header.length;
		this.dataTypes = new String[numCols];
		// create a map from column name to index
		// this will be used to help speed up finding the location of values
		this.csvColumnToIndex = new Hashtable<String, Integer>();

		for(int colIndex = 1; colIndex <= numCols; colIndex++) {
			// fill in the column to index
			csvColumnToIndex.put(header[colIndex-1], colIndex-1);

			// fill in the data type for the column
			if(rdfMap.containsKey(colIndex + "")) {
				String uiType = rdfMap.get(colIndex + "");
				String sqlType = retrieveSqlType(uiType);
				dataTypes[colIndex-1] = sqlType;
			} else {
				//TODO: if it is not passed from the FE... lets go with string for now
				dataTypes[colIndex-1] = "VARCHAR(800)";
			}
		}
	}
	
	private String retrieveSqlType(String uiType) {
		String sqlType = sqlHash.get(uiType);
		if(sqlType == null) {
			// this should never happen...
			System.err.println("Need to add ui data type option, " + uiType + ", into sql hash");
			sqlType = "VARCHAR(800)";
		}
		return sqlType;
	}
	
	private void parseMetadata() {
		if(rdfMap.get("NODE_PROP") != null)
		{
			String nodePropNames = rdfMap.get("NODE_PROP");
			StringTokenizer nodePropTokens = new StringTokenizer(nodePropNames, ";");
			nodePropArrayList = new ArrayList<String>();
			if(basePropURI.equals("")){
				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			while(nodePropTokens.hasMoreElements())
			{
				String relation = nodePropTokens.nextToken();
				// in case the end of the prop string is empty string or spaces
				if(!relation.contains("%"))
					continue;

				nodePropArrayList.add(relation);

				String[] strSplit = relation.split("%");
				String concept = strSplit[0].trim();
				String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(concept);
				Map<String, String> propMap = null;
				if(concepts.containsKey(concept)) {
					propMap = concepts.get(concept);
				} else {					
					propMap = new LinkedHashMap<String, String>();
					// it could be a concat
					// in which case, it is a string
					if(concept.contains("+")) {
						propMap.put(concept, "VARCHAR(2000)");
					} else if(csvColumnToIndex.containsKey(concept)) {
						propMap.put(concept, dataTypes[csvColumnToIndex.get(concept)]);
					} else if(objectTypeMap.containsKey(concept)) {
						// If not contained in the csv, go with type from the objectTypeMap
						propMap.put(concept, retrieveSqlType(objectTypeMap.get(concept)));
					} else {
						// Default to string
						propMap.put(concept, "VARCHAR(800)");
					}
					//need to add the actual concept as a column
					owler.addConcept(cleanConceptTableName, propMap.get(concept).trim());
				}
				for(int i = 1; i < strSplit.length; i++) {
					String prop = strSplit[i];
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					// it could be a concat
					// in which case, it is a string
					if(prop.contains("+")) {
						propMap.put(prop, "VARCHAR(2000)");
					} else if(csvColumnToIndex.containsKey(prop)) {
						propMap.put(prop, dataTypes[csvColumnToIndex.get(prop)]);
					} else if(objectTypeMap.containsKey(prop)) {
						// If not contained in the csv, go with type from the objectTypeMap
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

		if(rdfMap.get("RELATION") != null)
		{
			String relationNames = rdfMap.get("RELATION");
			StringTokenizer relationTokens = new StringTokenizer(relationNames, ";");
			relationArrayList = new ArrayList<String>();
			// process each relationship
			while(relationTokens.hasMoreElements())
			{
				String relation = relationTokens.nextToken();
				// just in case the end of the prop string is empty string or spaces
				if(!relation.contains("@"))
					continue;

				relationArrayList.add(relation);

				String[] strSplit = relation.split("@");
				String fromConcept = strSplit[0].trim();
				String rel = strSplit[1].trim();
				String toConcept = strSplit[2].trim();
				
				String cleanFromConceptTableName = RDBMSEngineCreationHelper.cleanTableName(fromConcept);
				String cleanToConceptTableName = RDBMSEngineCreationHelper.cleanTableName(toConcept);
				
				// need to make sure all concepts are there
				if(!concepts.containsKey(fromConcept)) {
					Map<String, String> propMap = new LinkedHashMap<String, String>();
					// don't forget to add the actual column as well
					if(fromConcept.contains("+")) {
						propMap.put(fromConcept, "VARCHAR(2000)");
					} else if(csvColumnToIndex.containsKey(fromConcept)) {
						propMap.put(fromConcept, dataTypes[csvColumnToIndex.get(fromConcept)]);
					} else if(objectTypeMap.containsKey(fromConcept)) {
						// If not contained in the csv, go with type from the objectTypeMap
						propMap.put(fromConcept, retrieveSqlType(objectTypeMap.get(fromConcept)));
					} else {
						// Default to string
						propMap.put(fromConcept, "VARCHAR(800)");
					}
					owler.addConcept(cleanFromConceptTableName, propMap.get(fromConcept));
					concepts.put(fromConcept, propMap);
				}
				if(!concepts.containsKey(toConcept)) {
					Map<String, String> propMap = new LinkedHashMap<String, String>();
					// don't forget to add the actual column as well
					if(toConcept.contains("+")) {
						propMap.put(toConcept, "VARCHAR(2000)");
					} else if(csvColumnToIndex.containsKey(toConcept)) {
						propMap.put(toConcept, dataTypes[csvColumnToIndex.get(toConcept)]);
					} else if(objectTypeMap.containsKey(toConcept)) {
						// If not contained in the csv, go with type from the objectTypeMap
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
				String predicate = cleanToConceptTableName + "." + cleanFromConceptTableName + FK 
						+ "." + cleanFromConceptTableName + "." + cleanFromConceptTableName;
				// determine order based on * 
				// if it is fromConcept *has toConcept
				// then toConcept has the FK for fromConcept
				if(rel.startsWith("*")) {
					if(relations.containsKey(toConcept)) {
						relList = relations.get(toConcept);
					} else {
						relList = new Vector<String>();
					}
					relList.add(fromConcept);
					relations.put(toConcept, relList);
					
					// add FK as property
//					String dataType = null;
//					if(csvColumnToIndex.containsKey(toConcept)) {
//						dataType = dataTypes[csvColumnToIndex.get(toConcept)];
//					} else if(objectTypeMap.containsKey(toConcept)) {
//						// If not contained in the csv, go with type from the objectTypeMap
//						dataType = retrieveSqlType(objectTypeMap.get(toConcept));
//					} else {
//						// Default to string
//						dataType = "VARCHAR(800)";
//					}
//					owler.addProp(cleanToConceptTableName, cleanToConceptTableName, cleanFromConceptTableName + FK, dataType, null);
					
				}
				// if it is fromConcept has* toConcept
				// then fromConcept has the FK for toConcept
				else if(rel.endsWith("*")) {
					if(relations.containsKey(fromConcept)) {
						relList = relations.get(fromConcept);
					} else {
						relList = new Vector<String>();
					}
					relList.add(toConcept);
					relations.put(fromConcept, relList);

					// the predicate string is different from the default defined
					predicate = cleanToConceptTableName + "." + cleanToConceptTableName 
							+ "." + cleanFromConceptTableName + "." + cleanToConceptTableName + FK;
					
					// add FK as property
//					String dataType = null;
//					if(csvColumnToIndex.containsKey(toConcept)) {
//						dataType = dataTypes[csvColumnToIndex.get(toConcept)];
//					} else if(objectTypeMap.containsKey(toConcept)) {
//						// If not contained in the csv, go with type from the objectTypeMap
//						dataType = retrieveSqlType(objectTypeMap.get(toConcept));
//					} else {
//						// Default to string
//						dataType = "VARCHAR(800)";
//					}
//					owler.addProp(cleanFromConceptTableName, cleanFromConceptTableName, cleanToConceptTableName + FK, dataType, null);
					
				} else {
					// do it based on relationship format
					// TODO: build it out to do it based on
					// number of unique instances in each row
					if(relations.containsKey(toConcept)) {
						relList = relations.get(toConcept);
					} else {
						relList = new Vector<String>();
					}
					relList.add(fromConcept);
					relations.put(toConcept, relList);
					
//					// add FK as property
//					String dataType = null;
//					if(csvColumnToIndex.containsKey(toConcept)) {
//						dataType = dataTypes[csvColumnToIndex.get(toConcept)];
//					} else if(objectTypeMap.containsKey(toConcept)) {
//						// If not contained in the csv, go with type from the objectTypeMap
//						dataType = retrieveSqlType(objectTypeMap.get(toConcept));
//					} else {
//						// Default to string
//						dataType = "VARCHAR(800)";
//					}
//					owler.addProp(cleanToConceptTableName, cleanToConceptTableName, cleanFromConceptTableName + FK, dataType, null);
				}

				owler.addRelation(cleanFromConceptTableName, cleanToConceptTableName, predicate);
			}
		}
	}

	private void insertData(String sql){
		if(!sql.endsWith(";")){
			sql+= ";";
		}
//		scriptFile.println(sql);
		try {
			engine.insertData(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * Gets the properly formatted object from the string[] values object.  Since this just goes into a sql insert
	 * query, the object is always in string format
	 * Also handles if the column is a concatenation
	 * @param object					The column to get the correct data type for - can be a concatenation
	 * @param values					The string[] containing the values for the row
	 * @param dataTypes					The string[] containing the data type for each column in the values array
	 * @param colNameToIndex			Map containing the column name to index in values[] for fast retrieval of data
	 * @return							The object in the correct format
	 */
	private Object createObject(String object, String[] values, String[] dataTypes, Map<String, Integer> colNameToIndex)
	{
		
		if(object.contains("+"))
		{
			return parseConcatenatedObject(object, values, colNameToIndex);
		}
		
		// If the object doesn't exist as a column name, try to get it from the objectMap
		if(!colNameToIndex.containsKey(object))
		{
			// Will throw a null pointer if the objectValueMap doesn't contain the object
			String valueFromMap = objectValueMap.get(object);
			
			// Allow a value in the map to contain "+" (not recursive)
			if (valueFromMap.contains("+")) {
				return parseConcatenatedObject(valueFromMap, values, colNameToIndex);
			} else if (objectTypeMap.containsKey(object)) {
				// If a type is specified in the objectTypeMap, then cast value by type
				return castValueByType(valueFromMap, retrieveSqlType(objectTypeMap.get(object)));
			} else {
				// Default to String
				return Utility.cleanString(valueFromMap, true);
			}
		}

		int colIndex = colNameToIndex.get(object);
		return castValueByType(values[colIndex], dataTypes[colIndex]);
	}
	
	// Method to parse objects with "+"
	private String parseConcatenatedObject(String object, String[] values, Map<String, Integer> colNameToIndex) {
		StringBuilder strBuilder = new StringBuilder();
		String[] objList = object.split("\\+");
		for(int i = 0; i < objList.length; i++){
			// Allow for concatenated unique id
			if (colNameToIndex.containsKey(objList[i])) {
				
				// Before just this line of code
				strBuilder.append(values[colNameToIndex.get(objList[i])]); 
			} else if (objList[i].equals(rowKey)) {
				
				// Create a unique id by appending the zip file name, system and row 
				strBuilder.append(Integer.toString(rowCounter));
			} else {
				
				// Will throw a null pointer if the objectValueMap doesn't contain the object
				strBuilder.append(objectValueMap.get(objList[i]));
			}
		}
		return Utility.cleanString(strBuilder.toString(), true);
	}
	
	private Object castValueByType(String strVal, String type) {
		// here we need to grab the value and cast it based on the type
		Object retObj = null;

		if(type.equals("INT")) {
			try {
				//added to remove $ and , in data and then try parsing as Double
				int mult = 1;
				if(strVal.startsWith("(") || strVal.startsWith("-")) // this is a negativenumber
					mult = -1;
				strVal = strVal.replaceAll("[^0-9\\.]", "");
				return mult * Integer.parseInt(strVal.trim());
			} catch(NumberFormatException ex) {
				//do nothing
				return null;
			}
		} else if(type.equals("FLOAT")) {
			try {
				//added to remove $ and , in data and then try parsing as Double
				int mult = 1;
				if(strVal.startsWith("(") || strVal.startsWith("-")) // this is a negativenumber
					mult = -1;
				strVal = strVal.replaceAll("[^0-9\\.]", "");
				return mult * Double.parseDouble(strVal.trim());
			} catch(NumberFormatException ex) {
				//do nothing
				return null;
			}
		} else if(type.equals("DATE")) {
			Long dTime = SemossDate.getTimeForDate(strVal);
			if(dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd");
			}
		} else if(type.equals("TIMESTAMP")){
			Long dTime = SemossDate.getTimeForTimestamp(strVal);
			if(dTime != null) {
				retObj = new SemossDate(dTime, "yyyy-MM-dd HH:mm:ss");
			}
		} else {
			retObj = strVal;
		}

		return retObj;
	}
	
	/**
	 * Open script file that contains all the sql statements run during the upload process
	 * @param engineName name of the engine/db
	 */
	private void openScriptFile(String engineName){
//		try {
//			String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
//			scriptFileName =  dbBaseFolder + "/db/" + engineName + "/" + scriptFileName;
//			if(scriptFile == null) {
//				scriptFile = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(scriptFileName),true)));//set append to true
//			}
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
	}
	
	public static void main(String [] args) throws RepositoryException, SailException, Exception
	{
		// run this test when the server is not running
		// this will create the db and smss file so when you start the server
		// the database created will be picked up and exposed
		TestUtilityMethods.loadDIHelper();

		// set the file
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Clean_Movies.csv";

		// set a bunch of db stuff
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String engineName = "Movie_RDMBS_OWL_NEW";
		String customBase = "http://semoss.org/ontologies";
		RdbmsTypeEnum dbType = RdbmsTypeEnum.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS);


		Hashtable<String, String>[] rdfMapArr = new Hashtable[1];
		Hashtable<String, String> rdfMap = new Hashtable<String, String>();
		rdfMapArr[0] = rdfMap;

		// need to set the prop file data into rdfMap...
		// this is very annoying
		rdfMap.put("NUM_COLUMNS", "17");
		rdfMap.put("DISPLAY_NAME", "");
		rdfMap.put("RELATION_PROP", "");
		rdfMap.put("RELATION", "Title@Title_Producer@Producer;Title@Title_Director@Director;Title@Title_Studio@Studio;Title@Title_Genre_Updated@Genre Updated;Title@Title_Rating_G_PG_PG-13_R@Rating (G, PG, PG-13, R);Title@Title_Nominated@Nominated;");
		rdfMap.put("NODE_PROP", "Title@Title_Revenue_-_International@Revenue - International ;Title%Revenue - Domestic;Title%Metacritic score - Critics;Title%Movie Budget;Title%Metacritic score - Audience;Title%Rotten Tomatoes Score - Critics;Title%Rotten Tomatoes - Audience;Title%IMDB Score - Audience (10);Title%Year;Title%Release Month (MM);");
		rdfMap.put("17", "STRING");
		rdfMap.put("16", "NUMBER");
		rdfMap.put("15", "NUMBER");
		rdfMap.put("14", "NUMBER");
		rdfMap.put("13", "NUMBER");
		rdfMap.put("12", "NUMBER");
		rdfMap.put("11", "NUMBER");
		rdfMap.put("10", "STRING");
		rdfMap.put("9", "NUMBER");
		rdfMap.put("8", "NUMBER");
		rdfMap.put("7", "STRING");
		rdfMap.put("6", "STRING");
		rdfMap.put("5", "NUMBER");
		rdfMap.put("4", "STRING");
		rdfMap.put("3", "STRING");
		rdfMap.put("2", "STRING");
		rdfMap.put("1", "STRING");
		rdfMap.put("START_ROW", "2");

		// do the actual db loading
		RDBMSReader reader = new RDBMSReader();
		reader.setRdfMapArr(rdfMapArr);
		String owlFile = baseFolder + "/" + propWriter.owlFile;
		
		ImportOptions options = new ImportOptions();
	
		options.setSMSSLocation(propWriter.propFileName);
		options.setDbName(engineName);
		options.setFileLocation(fileNames);
		options.setBaseUrl(customBase);
		options.setOwlFileLocation(owlFile);		
		options.setRDBMSDriverType(dbType);
		options.setAllowDuplicates(false);
		//reader.importFileWithOutConnection(propWriter.propFileName, engineName, fileNames, customBase, owlFile, dbType, false);
		reader.importFileWithOutConnection(options);

		// create the smss file and drop temp file
		File propFile = new File(propWriter.propFileName);
		File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
		FileUtils.copyFile(propFile, newProp);
		newProp.setReadable(true);
		FileUtils.forceDelete(propFile);

		//		TestUtilityMethods.loadDIHelper();
		//
		//		RDBMSReader reader = new RDBMSReader();
		//		// DATABASE WILL BE WRITTEN WHERE YOUR DB FOLDER IS IN A FOLDER WITH THE ENGINE NAME
		//		// SMSS file will not be created at the moment.. will add shortly
		//		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Movie Results.csv;C:\\Users\\mahkhalil\\Desktop\\Movie Results Fixed 2.csv";
		//		String smssLocation = "";
		//		String engineName = "test";
		//		String customBase = "http://semoss.org/ontologies";
		//		String owlFile = DIHelper.getInstance().getProperty("BaseFolder") + "\\db\\" + engineName + "\\" + engineName + "_OWL.OWL";
		//		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
		//
		//		reader.importFileWithOutConnection(smssLocation, engineName, fileNames, customBase, owlFile, dbType, false);
	}
}
