package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.poi.main.helper.ImportOptions;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc.meta.FilePkqlMetadata;
import prerna.solr.SolrUtility;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class InsightFilesToDatabaseReader {

	private Set<String> newTables;
	
	public InsightFilesToDatabaseReader() {

	}
	
	public static void main(String[] args) throws IOException {
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

		InsightFilesToDatabaseReader creator = new InsightFilesToDatabaseReader();
		creator.processInsightFiles(in, newEngineName);
	}

	public IEngine processInsightFiles(Insight in, String engineName) throws IOException{
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
		String baseDirectory = DIHelper.getInstance().getProperty("BaseFolder");
		
		// get the data frame object
		// TODO: assumption all the files are csvs
		List<FilePkqlMetadata> filesMeta = in.getFilesUsedInInsight();
		
		String fileLocation = "";
		List<Map<String, String[]>> dataTypeMapList = new Vector<Map<String, String[]>>();
		for(FilePkqlMetadata meta : filesMeta) {
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
		}
		
		boolean error = false;
		IEngine engine = null;
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
			
			ImportOptions options = new ImportOptions();
			options.setSMSSLocation(smssLocation);
			options.setDbName(engineName);
			options.setFileLocation(fileLocation);
			options.setBaseUrl("");
			options.setOwlFileLocation(owlPath);
			options.setRDBMSDriverType(SQLQueryUtil.DB_TYPE.H2_DB);
			options.setAllowDuplicates(true);			

			RDBMSFlatCSVUploader uploader = new RDBMSFlatCSVUploader();
			uploader.setAutoLoad(false);
			uploader.setDataTypeMapList(dataTypeMapList);
			//engine = uploader.importFileWithOutConnection(smssLocation, engineName, fileLocation, "", owlPath, SQLQueryUtil.DB_TYPE.H2_DB, true);
			engine = uploader.importFileWithOutConnection(options);
			
			// need to store the new tables so we know which columns came from where
			// when we update the data.import cvs file pkql to be data.import from the new
			// engine that was created
			this.newTables = uploader.getNewTables();
			
			engine.setOWL(owlPath);
//			engine.loadTransformedNodeNames();
			((AbstractEngine) engine).setPropFile(propWriter.propFileName);
			((AbstractEngine) engine).createInsights(baseDirectory);
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.STORE, smssLocation);
			Utility.synchronizeEngineMetadata(engineName); // replacing this for engine
			SolrUtility.addToSolrInsightCore(engineName);
			// Do we need this?
			// Commenting it out for now to speed up upload until we find a better way to utilize this
//			Utility.addToSolrInstanceCore(engine);

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
				SolrUtility.deleteFromSolr(engineName);
				
				throw new IOException("Error loading files from insight into database");
			}
		}
		
		return engine;
	}
	
	public Set<String> getNewTables() {
		return this.newTables;
	}
	
}
