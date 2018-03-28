package prerna.engine.impl.app.reactor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.app.AppEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.H2QueryUtil;

public class GenerateEmptyAppReactor extends AbstractReactor {

	private static final String CLASS_NAME = GenerateEmptyAppReactor.class.getName();

	/*
	 * This class is used to construct an empty app
	 * This app contains no data (no data file or OWL)
	 * This app only contains insights
	 * The idea being that the insights are parameterized and can be applied to various data sources
	 */

	public GenerateEmptyAppReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		this.organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		if(appName == null || appName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the app");
		}
		// need to make sure the app is unique
		boolean containsApp = true;
		try {
			containsApp = SolrIndexEngine.getInstance().containsApp(appName);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			e.printStackTrace();
		}
		if(containsApp) {
			throw new IllegalArgumentException("App name already exists.  Please provide a unique app name");
		}
		// need to make sure we are not overriding something that already exists in the file system
		final String FILE_SEP = System.getProperty("file.separator");
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// need to make sure app name doesn't already exist
		String appLocation = baseFolder + FILE_SEP + "db" + FILE_SEP + appName;
		File appFolder = new File(appLocation);
		if(appFolder.exists()) {
			throw new IllegalArgumentException("Database folder already contains an app directory with the same name. Please delete the existing app folder or provide a unique app name");
		}

		logger.info("Done validating app");
		logger.info("Starting app creation");

		/*
		 * Things we need to do
		 * 1) make directory
		 * 2) make insights database
		 * 3) make special smss
		 * 4) load into solr
		 */

		logger.info("Start generating app folder");
		appFolder.mkdirs();
		logger.info("Done generating app folder");

		logger.info("Start generating insights database");
		IEngine insightDb = generateInsightsDatabase(appName);
		logger.info("Done generating insights database");

		// add to DIHelper so we dont auto load with the file watcher
		String tempSmssLocation = null;
		logger.info("Start generating temp smss");
		try {
			tempSmssLocation = generateTempSmss(appName);
			DIHelper.getInstance().getCoreProp().setProperty(appName + "_" + Constants.STORE, tempSmssLocation);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done generating temp smss");

		logger.info("Start loading into solr");
		try {
			SolrUtility.addAppToSolr(appName);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			e.printStackTrace();
		}
		logger.info("Done loading into solr");

		AppEngine appEng = new AppEngine();
		appEng.setInsightDatabase(insightDb);
		// only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(appName, appEng);
		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		appNames = appNames + ";" + appName;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);
		
		// and rename .temp to .smss
		File tempFile = new File(tempSmssLocation);
		File smssFile = new File(tempSmssLocation.replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempFile, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempFile.delete();
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Generate the SMSS for the empty app
	 * @param appName
	 * @throws IOException 
	 */
	private String generateTempSmss(String appName) throws IOException {
		final String FILE_SEP = System.getProperty("file.separator");
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String smssFileLocation = baseFolder + FILE_SEP + "db" + FILE_SEP + appName + ".temp";

		// also write the base properties
		// ie ONTOLOGY, DREAMER, ENGINE, ENGINE CLASS
		FileWriter pw = null;
		BufferedReader read = null;
		Reader fileRead = null;
		try {
			File newFile = new File(smssFileLocation);
			pw = new FileWriter(newFile);
			pw.write("Base Properties \n");
			pw.write(Constants.ENGINE + "\t" + appName + "\n");
			pw.write(Constants.ENGINE_TYPE + "\tprerna.engine.impl.app.AppEngine\n");
			pw.write(Constants.RDBMS_INSIGHTS + "\tdb" + System.getProperty("file.separator") + "@engine@" + System.getProperty("file.separator") + "insights_database" + "\n");
			pw.write(Constants.SOLR_RELOAD + "\tfalse\n");
			pw.write(Constants.HIDDEN_DATABASE + "\tfalse\n");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			if (fileRead != null) {
				try {
					fileRead.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (read != null) {
				try {
					read.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (pw != null) {
				try {
					pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return smssFileLocation;
	}

	/**
	 * Generate the insights database
	 * TODO:
	 * TODO:
	 * TODO: NEED TO CONSOLIDATE THIS WITH ABSTRACT ENGINE CREATOR ... 
	 * TODO: LOTS OF CLEAN UP IN LOADING :(
	 * TODO:
	 * @param appName
	 * @return
	 */
	private IEngine generateInsightsDatabase(String appName) {
		H2QueryUtil queryUtil = new H2QueryUtil();
		Properties prop = new Properties();

		final String FILE_SEP = System.getProperty("file.separator");
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String connectionURL = "jdbc:h2:" + baseFolder + FILE_SEP + "db" + FILE_SEP + appName + FILE_SEP + 
				"insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		prop.put(Constants.CONNECTION_URL, connectionURL);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightRdbmsEngine = new RDBMSNativeEngine();
		insightRdbmsEngine.setProperties(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightRdbmsEngine.openDB(null);

		// CREATE TABLE QUESTION_ID (ID VARCHAR(50), QUESTION_NAME VARCHAR(255), QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225), QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP CLOB, QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB, QUESTION_IS_DB_QUERY BOOLEAN, DATA_TABLE_ALIGN VARCHAR(500), QUESTION_PKQL ARRAY)
		String questionTableCreate = "CREATE TABLE QUESTION_ID ("
				+ "ID VARCHAR(50), "
				+ "QUESTION_NAME VARCHAR(255), "
				+ "QUESTION_PERSPECTIVE VARCHAR(225), "
				+ "QUESTION_LAYOUT VARCHAR(225), "
				+ "QUESTION_ORDER INT, "
				+ "QUESTION_DATA_MAKER VARCHAR(225), "
				+ "QUESTION_MAKEUP CLOB, "
				+ "QUESTION_PROPERTIES CLOB, "
				+ "QUESTION_OWL CLOB, "
				+ "QUESTION_IS_DB_QUERY BOOLEAN, "
				+ "DATA_TABLE_ALIGN VARCHAR(500), "
				+ "HIDDEN_INSIGHT BOOLEAN, "
				+ "QUESTION_PKQL ARRAY)";

		insightRdbmsEngine.insertData(questionTableCreate);

		// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY VARCHAR(225), PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS VARCHAR(2000), PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT BOOLEAN, PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), PARAMETER_VIEW_TYPE VARCHAR(255), QUESTION_ID_FK INT)
		String parameterTableCreate = "CREATE TABLE PARAMETER_ID ("
				+ "PARAMETER_ID VARCHAR(255), "
				+ "PARAMETER_LABEL VARCHAR(255), "
				+ "PARAMETER_TYPE VARCHAR(225), "
				+ "PARAMETER_DEPENDENCY VARCHAR(225), "
				+ "PARAMETER_QUERY VARCHAR(2000), "
				+ "PARAMETER_OPTIONS VARCHAR(2000), "
				+ "PARAMETER_IS_DB_QUERY BOOLEAN, "
				+ "PARAMETER_MULTI_SELECT BOOLEAN, "
				+ "PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), "
				+ "PARAMETER_VIEW_TYPE VARCHAR(255), "
				+ "QUESTION_ID_FK INT)";

		insightRdbmsEngine.insertData(parameterTableCreate);

		String feTableCreate = "CREATE TABLE UI ("
				+ "QUESTION_ID_FK INT, "
				+ "UI_DATA CLOB)";

		insightRdbmsEngine.insertData(feTableCreate);

		return insightRdbmsEngine;
	}

}
