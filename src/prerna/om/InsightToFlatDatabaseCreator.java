package prerna.om;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.AbstractEngineCreator;
import prerna.poi.main.PropFileWriter;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.ImportOptions;
import prerna.sablecc.PKQLRunner;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class InsightToFlatDatabaseCreator extends AbstractEngineCreator {

	private static final Logger LOGGER = LogManager.getLogger(InsightToFlatDatabaseCreator.class.getName());

	public InsightToFlatDatabaseCreator() {

	}
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();

		// load in the engine
		//TODO: put in correct path and engine name for your database
		String engineName = "Movie_RDBMS";
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + engineName + ".smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName(engineName);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(engineName, coreEngine);
		
		// create the pkql to add data into the insight
		String pkqlExp = "data.import(api:Movie_RDBMS.query([c:Title , c:Genre_Updated , c:Title__Movie_Budget],([c:Title, inner.join, c:Genre_Updated])));";
		// create the insight and load the data
		Insight in = new Insight(coreEngine, "H2Frame", "Grid");
		
		H2Frame dataframe = new H2Frame();
		PKQLRunner run = new PKQLRunner();
		run.runPKQL(pkqlExp, dataframe);
		in.setDataMaker(dataframe);
		
		// TODO: make sure this is a new name
		String newEngineName = "createNewEngineName";

		InsightToFlatDatabaseCreator creator = new InsightToFlatDatabaseCreator();
		creator.createNewFlatDatabaseFromInsight(in, newEngineName);
	}

	public void createNewFlatDatabaseFromInsight(Insight in, String engineName) {
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
			return;
		}

		// get the base folder to create the engine db in
		String baseDirectory = DIHelper.getInstance().getProperty("BaseFolder");
		// hard-coding h2 for time being
		queryUtil = SQLQueryUtil.initialize(SQLQueryUtil.DB_TYPE.H2_DB);
		
		// get the data frame object
		ITableDataFrame dataFrame = (ITableDataFrame) dm;
		
		boolean error = false;
		File tempPropFile = null;
		File newSmssProp = null;
		try {
			// first write the prop file for the new engine
			PropFileWriter propWriter = new PropFileWriter();
			propWriter.setBaseDir(baseDirectory);
			propWriter.setRDBMSType(SQLQueryUtil.DB_TYPE.H2_DB);
			propWriter.runWriter(engineName, "", "", ImportOptions.DB_TYPE.RDBMS);

			// need to go back and clean the prop writer
			String smssLocation = propWriter.propFileName;
			String owlPath = baseDirectory + "/" + propWriter.owlFile;

			// need to create the .temp file object before we upload so we can delete the file if an error occurs
			tempPropFile = new File(smssLocation);

			// need to prepare the builder for some constants...
			// we actually only need the owlPath.. everything else will default appropriately in this case
			prepEngineCreator(null, owlPath, null);
			
			// create the engine and the owler
			openRdbmsEngineWithoutConnection(engineName);
			
			// our fundamental assumption is that this is a single table
			String tableName = "TABLE_NAME_HERE"; // need to use the csv file name -> should store this in insight 
			String[] headers = dataFrame.getColumnHeaders();
			String[] dataTypes = new String[headers.length];
			for(int i = 0; i < headers.length; i++) {
				dataTypes[i] = Utility.convertDataTypeToString(dataFrame.getDataType(headers[i]));
			}
			// create the table now
			createTable(tableName, headers, dataTypes);

			// now loop through and add everything and insert into the table
			bulkInsertIntoTable(tableName, headers, dataTypes, dataFrame.iterator());

			// add the identity column to be the main concept
			String identifyCol = tableName + "_ROW_ID"; 
			addIdentityColumnToTable(tableName, identifyCol);
			// add to the owl file
			createOWL(tableName, identifyCol, headers, dataTypes);
			// export owl file
			createBaseRelations();
			// create the base question sheet
			RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engine, queryUtil);
			
			engine.setOWL(owlPath);
			engine.loadTransformedNodeNames();
			((AbstractEngine) engine).setPropFile(propWriter.propFileName);
			((AbstractEngine) engine).createInsights(baseDirectory);

			// convert the .temp to .smss file
			newSmssProp = new File(smssLocation.replace("temp", "smss"));
			// we just copy over the the .temp file contents into the .smss
			FileUtils.copyFile(tempPropFile, newSmssProp);
			newSmssProp.setReadable(true);
			tempPropFile.delete();
			
			// only after all of this is good, should we add it to DIHelper
			// add engine name to list of names
			DIHelper.getInstance().setLocalProperty(engineName, engine);
			String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames + ";" + engineName;
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
			// also add smss location
			Properties props = DIHelper.getInstance().getCoreProp();
			props.put(engineName + "_" + Constants.STORE, newSmssProp.getAbsolutePath());
			
			Utility.synchronizeEngineMetadata(engineName);
			Utility.addToSolrInsightCore(engine);
			// Do we need this?
			// Commenting it out for now to speed up upload until we find a better way to utilize this
			// Utility.addToSolrInstanceCore(engine);
			
		} catch (IllegalArgumentException e) {
			error = true;
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			error = true;
			e.printStackTrace();
		} catch (IOException e) {
			error = true;
			e.printStackTrace();
		} catch (KeyManagementException e) {
			error = true;
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			error = true;
			e.printStackTrace();
		} catch (KeyStoreException e) {
			error = true;
			e.printStackTrace();
		} finally {
			// something messed up
			if(error) {
				// close the db so we can delete it
				try {
					closeDB();
				} catch (IOException e) {
					e.printStackTrace();
				}
				// delete the engine folder and all its contents
				String engineFolderPath = baseDirectory + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName;
				File engineFolderDir = new File(engineFolderPath);
				if(engineFolderDir.exists()) {
					File[] files = engineFolderDir.listFiles();
					if(files != null) { //some JVMs return null for empty dirs
						for(File f: files) {
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
				Utility.deleteFromSolr(engineName);
			}
		}
	}

	/**
	 * 
	 * @param TABLE_NAME
	 * @param headers
	 * @param dataTypes
	 * @param iterator
	 * @throws IOException
	 */
	private void bulkInsertIntoTable(final String TABLE_NAME, String[] headers, String[] dataTypes, Iterator<Object[]> iterator) throws IOException {
		// now we need to loop through the csv data and cast to the appropriate type and insert
		// let us be smart about this and use a PreparedStatement for bulk insert
		// get the bulk statement

		// the prepared statement requires the table name and then the list of columns
		Object[] getPreparedStatementArgs = new Object[headers.length+1];
		getPreparedStatementArgs[0] = TABLE_NAME;
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			getPreparedStatementArgs[headerIndex+1] = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
		}
		PreparedStatement ps = (PreparedStatement) this.engine.doAction(ACTION_TYPE.BULK_INSERT, getPreparedStatementArgs);

		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;

		// we loop through every row of the csv
		try {
			while(iterator.hasNext()) {
				Object[] nextRow = iterator.next();
				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = dataTypes[colIndex];
					if(type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex] + "");
						if(value != null) {
							ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
						} else {
							// set default as null
							ps.setObject(colIndex+1, null);
						}
					} else if(type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT") || type.equalsIgnoreCase("LONG")) {
						Double value = Utility.getDouble(nextRow[colIndex] + "");
						if(value != null) {
							ps.setDouble(colIndex+1, value);
						} else {
							// set default as null
							ps.setObject(colIndex+1, null);
						}
					} else {
						String value = Utility.cleanString(nextRow[colIndex] + "", false);
						ps.setString(colIndex+1, value + "");
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if(++count % batchSize == 0) {
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param TABLE_NAME
	 * @param headers
	 * @param dataTypes
	 */
	private void createTable(final String TABLE_NAME, String[] headers, String[] dataTypes) {
		// need to first create the table
		// simple sql statement using the col names and their sql data types
		StringBuilder queryBuilder = new StringBuilder("CREATE TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" (");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			queryBuilder.append(cleanHeader.toUpperCase());
			queryBuilder.append(" ");
			queryBuilder.append(dataTypes[headerIndex].toUpperCase());

			// add a comma if NOT the last index
			if(headerIndex != headers.length-1) {
				queryBuilder.append(", ");
			}
		}
		queryBuilder.append(")");
		LOGGER.info("CREATE TABLE QUERY : " + queryBuilder.toString());
		this.engine.insertData(queryBuilder.toString());
	}
	
	/**
	 * Generate the query to append an identity column onto the table
	 * @param TABLE_NAME				The name of the table
	 * @param IDENTITY_COL_NAME			The name of the identity column
	 */
	private void addIdentityColumnToTable(final String TABLE_NAME, final String IDENTITY_COL_NAME) {
		StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" ADD ").append(IDENTITY_COL_NAME).append(" IDENTITY");
		
		System.out.println(queryBuilder.toString());
		this.engine.insertData(queryBuilder.toString());
	}
	
	/**
	 * 
	 * @param tableName
	 * @param colName
	 * @param headers
	 * @param dataTypes
	 */
	private void createOWL(String tableName, String colName, String[] headers, String[] dataTypes) {
		// add the concept
		this.owler.addConcept(tableName, colName, "LONG");
		// add each column as a property
		for(int i = 0; i < headers.length; i++) {
			this.owler.addProp(tableName, colName, headers[i], dataTypes[i]);
		}
	}
}
