package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSReader extends AbstractFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSReader.class.getName());

	private static final DateFormat DATE_DF = new SimpleDateFormat("yyy-MM-dd");
	private static final DateFormat GENERIC_DF = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");

	private String propFile; // the file that serves as the property file
	private FileReader readCSVFile;
	private ICsvMapReader mapReader;
	private String [] header;
	private List<String> headerList;
	private CellProcessor[] processors;
	private static Hashtable <String, CellProcessor> typeHash = new Hashtable<String, CellProcessor>();
	public final static String NUMCOL = "NUM_COLUMNS";
	public final static String NOT_OPTIONAL = "NOT_OPTIONAL";
	private ArrayList<String> relationArrayList = new ArrayList<String>();
	private ArrayList<String> nodePropArrayList = new ArrayList<String>();
	//	private ArrayList<String> relPropArrayList = new ArrayList<String>();
	private boolean propFileExist = true;
	private Hashtable<String, String>[] rdfMapArr;

	private int count = 0;
	private int startRow = 2;
	private int maxRows = 100000;

	private Map<String, String> sqlHash = new Hashtable<String, String>();
	private Map<String, String> types = new Hashtable<String, String>();
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

	protected String scriptFileName = "DBScript.sql";
	protected PrintWriter scriptFile = null;

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
	 * @param dbName 		String grabbed from the user interface that would be used as the name for the database
	 * @param fileNames		Absolute paths of files the user wants to load into SEMOSS, paths are separated by ";"
	 * @param customBase	String grabbed from the user interface that is used as the URI base for all instances 
	 * @param customMap		
	 * @param owlFile		String automatically generated within SEMOSS to determine the location of the OWL file that is produced
	 * @throws RepositoryException 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws SailException 
	 */
	public IEngine importFileWithOutConnection(String smssLocation, String fileNames, String customBase, String owlFile, String engineName, SQLQueryUtil.DB_TYPE dbType, boolean allowDuplicates) 
			throws RepositoryException, FileNotFoundException, IOException, SailException, Exception {
		long start = System.currentTimeMillis();

		boolean error = false;
		queryUtil = SQLQueryUtil.initialize(dbType);
		String[] files = prepareReader(fileNames, customBase, owlFile, smssLocation);
		LOGGER.setLevel(Level.WARN);
		try {
			openRdbmsEngineWithoutConnection(engineName);
			openScriptFile(engineName);
			createTypes();
			createSQLTypes();
			for(int i = 0; i<files.length;i++)
			{
				try {
					String fileName = files[i];
					openCSVFile(fileName);

					if(i ==0 ) {
						scriptFile.println("-- ********* begin load process ********* ");
					}
					scriptFile.println("-- ********* begin load " + fileName + " ********* ");
					LOGGER.info("-- ********* begin load " + fileName + " ********* ");

					// load the prop file for the CSV file 
					if(propFileExist){
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					createProcessors();
					//TODO: same method used in other classes
					parseMetadata();
					// determine the type of data in each column of CSV file
					processDisplayNames();
					skipRows();
					processData(i == 0);

					scriptFile.println("-- ********* completed processing file " + fileName + " ********* ");
					LOGGER.info("-- ********* completed processing file " + fileName + " ********* ");

				} finally {
					closeCSVFile();
					clearTables();
				}
			}
			cleanUpDBTables(engineName, allowDuplicates);
			createBaseRelations();
			RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engine, queryUtil);
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
			if(scriptFile != null) {
				scriptFile.println("-- ********* completed load process ********* ");
				scriptFile.close();
			}
		}

		long end = System.currentTimeMillis();
		LOGGER.info((end - start)/1000 + " seconds to load...");
		
		return engine;
	}

	public void importFileWithConnection(String engineName, String fileNames, String customBase, String owlFile, SQLQueryUtil.DB_TYPE dbType, boolean allowDuplicates) 
			throws RepositoryException, FileNotFoundException, IOException, SailException, Exception {

		long start = System.currentTimeMillis();

		queryUtil = SQLQueryUtil.initialize(dbType);
		String[] files = prepareReader(fileNames, customBase, owlFile, engineName);

		LOGGER.setLevel(Level.WARN);
		try {
			openEngineWithConnection(engineName);
			openScriptFile(engineName);
			createTypes();
			createSQLTypes();
			findIndexes(engine.getEngineName());
			for(int i = 0; i<files.length;i++)
			{
				try {
					String fileName = files[i];
					openCSVFile(fileName);			
					if(i ==0 ) {
						scriptFile.println("-- ********* begin load process ********* ");
					}
					scriptFile.println("-- ********* begin load " + fileName + " ********* ");
					LOGGER.info("-- ********* begin load " + fileName + " ********* ");

					// load the prop file for the CSV file 
					if(propFileExist){
						openProp(propFile);
					} else {
						rdfMap = rdfMapArr[i];
					}
					createProcessors();
					//TODO: same method used in other classes
					parseMetadata();
					// determine the type of data in each column of CSV file
					processDisplayNames();
					skipRows();
					processData(false);

					scriptFile.println("-- ********* completed processing file " + fileName + " ********* ");
					LOGGER.info("-- ********* completed processing file " + fileName + " ********* ");

				} finally {
					closeCSVFile();
					clearTables();
				}
			}
			createBaseRelations();
			addOriginalIndices();
			cleanUpDBTables(engineName, allowDuplicates);
			RDBMSEngineCreationHelper.addToExistingQuestionFile(engine, addedTables, queryUtil);
		} finally {
			if(scriptFile != null) {
				scriptFile.println("-- ********* completed load process ********* ");
				scriptFile.close();
			}
		}

		long end = System.currentTimeMillis();
		LOGGER.info((end - start)/1000 + " seconds to load...");
	}

	private void processData(boolean noExistingData) throws IOException {
		// get existing data is present
		if(!noExistingData) {
			existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine,queryUtil);
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

				//TODO: do I need to also add types for the FK columns created???
//				owler.addProp(cleanConceptName, cleanRel, relColType);
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

					//TODO: do I need to also add types for the FK columns created???
//					owler.addProp(cleanConcept, cleanNewRel, newColsForConcept.get(newRel));
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
			}
		}
	}

	private void processCSVTable(boolean noExistingData) throws IOException{
		try {
			Map<String, String> defaultInsertStatements = getDefaultInsertStatements();
			Map<String, Object> jcrMap;
			while( (jcrMap = mapReader.read(header, processors)) != null && count<(maxRows))
			{
				// loop through all the concepts
				for(String concept : concepts.keySet()) {
					String defaultInsert = defaultInsertStatements.get(concept);

					String sqlQuery = null;
					if(noExistingData) {
						// this is the easiest case, just add all the information
						sqlQuery = createInsertStatement(concept, defaultInsert, jcrMap);
					} else {
						// if the concept is a brand new table, easy, just add
						// for some reason, all existing metadata comes back upper case
						if(!existingRDBMSStructure.containsKey(concept.toUpperCase())) {
							sqlQuery = createInsertStatement(concept, defaultInsert, jcrMap);
						} 
						// other case is if the table is there, but it hasn't been altered, also just add
						else if(existingTableWithAllColsAccounted.contains(concept)) {
							sqlQuery = createInsertStatement(concept, defaultInsert, jcrMap);
						} else {
							// here we add the logic if we should perform an update query vs. an insert query
							sqlQuery = getAlterQuery(concept, jcrMap, defaultInsert);
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
		} catch(SuperCsvCellProcessorException e) {
			e.printStackTrace();
			String errorMessage = "Error processing row number " + count + ". ";
			errorMessage += e.getMessage();
			throw new IOException(errorMessage);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error processing CSV headers");
		}

	}

	private String getAlterQuery(String concept, Map <String, Object> jcrMap, String defaultInsert) {
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
			Object value = getProperlyFormatedForQuery(col, createObject(col, jcrMap), type);

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
				Object value = getProperlyFormatedForQuery(rel, createObject(rel, jcrMap), type);

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
		Object value = getProperlyFormatedForQuery(concept, createObject(concept, jcrMap), conceptType);
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
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, getRowCountQuery);
		if(wrapper.hasNext()){
			ISelectStatement stmt = wrapper.next();
			String rowcount = stmt.getVar(queryUtil.getResultSelectRowCountFromRowCount()) + "";
			if(rowcount.equals("0")){
				isInsert = true;
			}
		}

		String values = valuesBuffer.toString();
		if(isInsert){
			if(values.length()==0) {
				// this is the odd case when no columns are being added, but we need to insert the instance by itself
				return createInsertStatement(concept, defaultInsert, jcrMap);
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
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		while(wrapper.hasNext())
		{
			String tablename = "";
			String dropCurrentIndexText = "";
			ISelectStatement stmt = wrapper.next();
			String indexName = stmt.getVar(queryUtil.getResultAllIndexesInDBIndexName()) + "";
			//only storing off custom indexes, recreating the non custom ones on the fly on the cleanUpDBTables method

			String indexInfoQry = queryUtil.getDialectIndexInfo(indexName, engineName);
			ISelectWrapper indexInfo = WrapperManager.getInstance().getSWrapper(engine, indexInfoQry);
			List<String> columnsInIndex = new Vector<String>();
			String columnName = "";
			while(indexInfo.hasNext()){
				ISelectStatement stmtIndx = indexInfo.next();
				tablename = stmtIndx.getVar(queryUtil.getResultAllIndexesInDBTableName()) + "";
				columnName = stmtIndx.getVar(queryUtil.getResultAllIndexesInDBColumnName()) + "";
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
				if(index.equals(indexOnTable)){//TODO check various order of keys since they are comma seperated
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
		//fill up the availableTables and availableTablesInfo maps
		Map<String, Map<String, String>> existingStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine, queryUtil);
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
				ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, verifyTable);
				while(wrapper.hasNext()){ 
					ISelectStatement stmtTblCount = wrapper.next();
					String numberOfRows = stmtTblCount.getVar(queryUtil.getResultSelectRowCountFromRowCount()) + "";
					if(numberOfRows.equals(0)){
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
			
			//create indexs for ALL tables since we deleted all indexes before
			for(String singleIndex: createIndex){
				insertData(singleIndex);
			}
		}
	}
	
	private String createInsertStatement(String concept, String defaultInsertQuery, Map<String, Object> jcrMap) {

		StringBuilder insertQuery = new StringBuilder(defaultInsertQuery);
		Map<String, String> propMap = concepts.get(concept);
		String type = propMap.get(concept);
		Object propertyValue = createObject(concept, jcrMap);
		insertQuery.append(getProperlyFormatedForQuery(concept, propertyValue, type));

		for(String prop : propMap.keySet()) {
			if(prop.equals(concept)) {
				continue;
			}
			insertQuery.append(",");

			type = propMap.get(prop);
			propertyValue = createObject(prop, jcrMap);
			insertQuery.append(getProperlyFormatedForQuery(prop, propertyValue, type));
		}

		if(relations.containsKey(concept)) {
			List<String> rels = relations.get(concept);
			for(String rel : rels) {
				insertQuery.append(",");

				String relColType = concepts.get(rel).get(rel);
				propertyValue = createObject(rel, jcrMap);
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
			return "'" + RDBMSEngineCreationHelper.escapeForSQLStatement(Utility.cleanString(value.toString(), true)) + "'";
		} else if(type.contains("INT") || type.contains("DECIMAL") || type.contains("DOUBLE") || type.contains("FLOAT") || type.contains("LONG") || type.contains("BIGINT")
				|| type.contains("TINYINT") || type.contains("SMALLINT")){
			return value;
		} else  if(type.contains("DATE")) {
			Date dateValue = null;
			try {
				dateValue = GENERIC_DF.parse(value + "");
			} catch (ParseException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Input value, " + value + " for column " + colName + " cannot be parsed as a date.");
			}
			value = DATE_DF.format(dateValue);
			return "'" + value + "'";
		}

		return "'" + RDBMSEngineCreationHelper.escapeForSQLStatement(value.toString()) + "'";
	}

	/**
	 * Retrieves the data in the CSV file for a specified string
	 * @param object 	String containing the object to retrieve from the CSV data
	 * @param jcrMap 	Map containing the data in the CSV file
	 * @return Object	The CSV data mapped to the object string
	 */
	public Object createObject(String object, Map <String, Object> jcrMap)
	{
		// need to do the class vs. object magic
		if(object.contains("+"))
		{
			StringBuilder strBuilder = new StringBuilder();
			String[] objList = object.split("\\+");
			for(int i = 0; i < objList.length; i++){
				strBuilder.append(jcrMap.get(objList[i])); 
			}
			return Utility.cleanString(strBuilder.toString(), true);
		}

		return jcrMap.get(object);
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

	public void parseMetadata() {
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
				String concept = strSplit[0];
				String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(concept);
				Map<String, String> propMap = null;
				if(concepts.containsKey(concept)) {
					propMap = concepts.get(concept);
				} else {					
					propMap = new LinkedHashMap<String, String>();

					//need to add the actual concept as a column
					if(concept.contains("+")) {
						propMap.put(concept, "VARCHAR(2000)");
					} else {
						propMap.put(concept, types.get(concept));
					}
					owler.addConcept(cleanConceptTableName, propMap.get(concept));
				}
				for(int i = 1; i < strSplit.length; i++) {
					String prop = strSplit[i];
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					// it could be a concat
					// in which case, it is a string
					if(prop.contains("+")) {
						propMap.put(prop, "VARCHAR(2000)");
					} else {
						propMap.put(prop, types.get(prop));
					}
					owler.addProp(cleanConceptTableName, cleanProp, propMap.get(prop));
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
				String fromConcept = strSplit[0];
				String rel = strSplit[1];
				String toConcept = strSplit[2];
				String cleanFromConceptTableName = RDBMSEngineCreationHelper.cleanTableName(fromConcept);
				String cleanToConceptTableName = RDBMSEngineCreationHelper.cleanTableName(toConcept);
				// need to make sure all concepts are there
				if(!concepts.containsKey(fromConcept)) {
					Map<String, String> propMap = new LinkedHashMap<String, String>();
					// don't forget to add the actual column as well
					if(fromConcept.contains("+")) {
						propMap.put(fromConcept, "VARCHAR(2000)");
					} else {
						propMap.put(fromConcept, types.get(fromConcept));
					}
					owler.addConcept(cleanFromConceptTableName, propMap.get(fromConcept));
					concepts.put(fromConcept, propMap);
				}
				if(!concepts.containsKey(toConcept)) {
					Map<String, String> propMap = new LinkedHashMap<String, String>();
					// don't forget to add the actual column as well
					if(toConcept.contains("+")) {
						propMap.put(toConcept, "VARCHAR(2000)");
					} else {
						propMap.put(toConcept, types.get(toConcept));
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
				}

				owler.addRelation(cleanFromConceptTableName, cleanToConceptTableName, predicate);
			}
		}

		//		if(rdfMap.get("RELATION_PROP") != null)
		//		{
		//			String propNames = rdfMap.get("RELATION_PROP");
		//			StringTokenizer propTokens = new StringTokenizer(propNames, ";");
		//			relPropArrayList = new ArrayList<String>();
		//			if(basePropURI.equals("")){
		//				basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		//			}
		//			while(propTokens.hasMoreElements())
		//			{
		//				String relation = propTokens.nextToken();
		//				//just in case the end of the prop string is empty string or spaces
		//				if(!relation.contains("%"))
		//					continue;
		//
		//				relPropArrayList.add(relation);
		//			}
		//		}
	}

	private void insertData(String sql){
		if(!sql.endsWith(";")){
			sql+= ";";
		}
		scriptFile.println(sql);
		engine.insertData(sql);	
	}

	/**
	 * Specifies which rows in the CSV to load based on user input in the prop file
	 * @throws FileReaderException 
	 */
	public void skipRows() throws IOException {
		//start count at 1 just row 1 is the header
		count = 1;
		if (rdfMap.get("START_ROW") != null)
			startRow = Integer.parseInt(rdfMap.get("START_ROW")); 
		try {
			while( count<startRow-1 && mapReader.read(header, processors) != null)// && count<maxRows)
			{
				count++;
				//logger.info("Skipping line: " + count);
			}
		} catch(SuperCsvCellProcessorException e) {
			e.printStackTrace();
			String errorMessage = "Error processing row number " + count + ". ";
			errorMessage += e.getMessage();
			throw new IOException(errorMessage);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error processing CSV headers");
		}
	}

	public void createSQLTypes()
	{
		sqlHash.put("DECIMAL", "FLOAT");
		sqlHash.put("DOUBLE", "FLOAT");
		sqlHash.put("STRING", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		sqlHash.put("TEXT", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		sqlHash.put("DATE", "TIME");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		//		typeHash.put("NUMBER", new ParseInt());
		//		typeHash.put("INTEGER", new ParseInt());
		sqlHash.put("BOOLEAN", "BOOLEAN");

		// not sure the optional is needed
	}

	/**
	 * Stores all possible variable types that the user can input from the CSV file into hashtable
	 * Hashtable is then used to match each column in CSV to a specific type based on user input in prop file
	 */
	public static void createTypes()
	{
		typeHash.put("DECIMAL", new ParseDouble());
		typeHash.put("DOUBLE", new ParseDouble());
		typeHash.put("STRING", new NotNull());
		typeHash.put("TEXT", new NotNull());
		typeHash.put("DATE", new ParseDate("yyyy-MM-dd hh:mm:ss"));
		typeHash.put("SIMPLEDATE", new ParseDate("MM/dd/yyyy"));
		// currently only add in numbers as doubles
		typeHash.put("NUMBER", new ParseDouble());
		typeHash.put("INTEGER", new ParseDouble());
		//		typeHash.put("NUMBER", new ParseInt());
		//		typeHash.put("INTEGER", new ParseInt());
		typeHash.put("BOOLEAN", new ParseBool());

		// now the optionals
		typeHash.put("DECIMAL_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("DOUBLE_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("STRING_OPTIONAL", new Optional());
		typeHash.put("TEXT_OPTIONAL", new Optional());
		typeHash.put("DATE_OPTIONAL", new Optional(new ParseDate("yyyy-MM-dd HH:mm:ss")));
		typeHash.put("SIMPLEDATE_OPTIONAL", new Optional(new ParseDate("MM/dd/yyyy")));
		// currently only add in numbers as doubles
		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseDouble()));
		typeHash.put("INTEGER_OPTIONAL", new Optional(new ParseDouble()));
		//		typeHash.put("NUMBER_OPTIONAL", new Optional(new ParseInt()));
		//		typeHash.put("INTEGER_OPTIONAL", new Optional(new ParseInt()));
		typeHash.put("BOOLEAN_OPTIONAL", new Optional(new ParseBool()));
	}

	/**
	 * Matches user inputed column type in prop file to the specific variable type name within Java SuperCSV API
	 */
	public void createProcessors()
	{
		// get the number columns in CSV file
		int numColumns = Integer.parseInt(rdfMap.get(NUMCOL));
		// Columns in prop file that are NON_OPTIMAL must contain a value
		String optional = ";";
		if(rdfMap.get(NOT_OPTIONAL) != null)
		{
			optional  = rdfMap.get(NOT_OPTIONAL);
		}

		int offset = 0;
		if(propFileExist){
			offset = 1;
		}
		processors = new CellProcessor[numColumns+offset];
		for(int procIndex = 1;procIndex <= processors.length;procIndex++)
		{
			// find the type for each column
			String type = rdfMap.get(procIndex+"");
			boolean opt = true;
			if(optional.indexOf(";" + procIndex + ";") > 1)
				opt = false;

			if(type != null && opt) {
				types.put(header[procIndex-1], sqlHash.get(type));
				processors[procIndex-1] = typeHash.get(type.toUpperCase() + "_OPTIONAL");
			} else if(type != null) {
				types.put(header[procIndex-1], sqlHash.get(type));
				processors[procIndex-1] = typeHash.get(type.toUpperCase());
			} else if(type == null) {
				types.put(header[procIndex-1], sqlHash.get("TEXT"));
				processors[procIndex-1] = typeHash.get("STRING_OPTIONAL");
			}
		}
	}

	/**
	 * Load the CSV file
	 * Gets the headers for each column and reads the property file
	 * @param fileName String
	 * @throws FileNotFoundException 
	 */
	public void openCSVFile(String fileName) throws FileNotFoundException {
		try {
			readCSVFile = new FileReader(fileName);
			mapReader = new CsvMapReader(readCSVFile, CsvPreference.STANDARD_PREFERENCE);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileNotFoundException("Could not find CSV file located at " + fileName);
		}		
		try {
			header = mapReader.getHeader(true);
			//header clean up, consistent with RDBMS headers handling
			/*for(int j = 0; j < header.length; j++){
				String singleHeader = Utility.cleanVariableString(header[j]); //String singleHeader = header[j];
				header[j] = singleHeader;
			}*/
			headerList = Arrays.asList(header);
			// last header in CSV file is the absolute path to the prop file
			propFile = header[header.length-1];
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileNotFoundException("Could not close reader input stream for CSV file " + fileName);
		}
	}

	/**
	 * Closes the CSV file streams
	 * @throws IOException 
	 */
	public void closeCSVFile() throws IOException {
		try {
			readCSVFile.close();
			mapReader.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not close CSV file streams");
		}		
	}

	/**
	 * Setter to store the metamodel created by user as a Hashtable
	 * @param data	Hashtable<String, String> containing all the information in a properties file
	 */
	public void setRdfMapArr(Hashtable<String, String>[] rdfMapArr) {
		this.rdfMapArr = rdfMapArr;
		propFileExist = false;
	}

	/**
	 * Open script file that contains all the sql statements run during the upload process
	 * @param engineName name of the engine/db
	 */
	private void openScriptFile(String engineName){
		try {
			String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
			scriptFileName =  dbBaseFolder + "/db/" + engineName + "/" + scriptFileName;
			if(scriptFile == null) {
				scriptFile = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(scriptFileName),true)));//set append to true
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
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
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", "", "" , ImportDataProcessor.DB_TYPE.RDBMS);
		
		
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
		rdfMap.put("NOT_OPTIONAL", ";");
		rdfMap.put("START_ROW", "2");

		// do the actual db loading
		RDBMSReader reader = new RDBMSReader();
		reader.setRdfMapArr(rdfMapArr);
		String owlFile = baseFolder + "/" + propWriter.owlFile;
		reader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBase, owlFile, engineName, dbType, false);
		
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
//		reader.importFileWithOutConnection(smssLocation, fileNames, customBase, owlFile, engineName, dbType, false);
	}
}
