package prerna.sablecc2.reactor.app.upload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.datastax.DataStaxGraphEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.app.AppEngine;
import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.solr.SolrIndexEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.SQLQueryUtil;

public class UploadUtilities {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String ENGINE_DIRECTORY;
	static {
		ENGINE_DIRECTORY = DIR_SEPARATOR + "db" + DIR_SEPARATOR;
	}
	
	private UploadUtilities() {

	}
	
	/**
	 * Validate the app name
	 * Does validation that:
	 * 1) The input is not null/empty
	 * 2) That the app name doesn't exist in solr already
	 * 3) That the app folder doesn't exist in the file directory
	 * @param appName
	 * @throws IOException
	 */
	public static void validateApp(String appName) throws IOException {
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
			throw new IOException("App name already exists.  Please provide a unique app name");
		}
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// need to make sure app name doesn't already exist
		String appLocation = baseFolder + ENGINE_DIRECTORY + appName;
		File appFolder = new File(appLocation);
		if(appFolder.exists()) {
			throw new IOException("Database folder already contains an app directory with the same name. "
					+ "Please delete the existing app folder or provide a unique app name");
		}
	}

	/**
	 * Generate the app folder and return teh folder
	 * @param appName
	 * @return
	 */
	public static File generateAppFolder(String appId, String appName) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appLocation = baseFolder + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId);
		File appFolder = new File(appLocation);
		appFolder.mkdirs();
		return appFolder;
	}
	
	/**
	 * Generate an empty OWL file based on the app name
	 * @param appName
	 * @return
	 */
	public static File generateOwlFile(String appId, String appName) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String owlLocation = baseFolder + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + appName + "_OWL.OWL";
		File owlFile = new File(owlLocation);
		
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;

		try {
			writer = new FileWriter(owlFile);
			bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			bufferedWriter.write("\n");
			bufferedWriter.write("<rdf:RDF");
			bufferedWriter.write("\n");
			bufferedWriter.write("\txmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"");
			bufferedWriter.write("\n");
			bufferedWriter.write("\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
			bufferedWriter.write("\n");
			bufferedWriter.write("</rdf:RDF>");
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return owlFile;
	}
	
	public static String getRelativeOwlPath(File owlFile) {
		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		return new File(baseDirectory).toURI().relativize(owlFile.toURI()).getPath();
	}

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Below methods pertain to the smss file
	 */

	
	
	/**
	 * Create a temporary smss file for a rdbms engine
	 * @param rdmbsType
	 * @param appName
	 * @param file
	 * @return
	 */
	public static File createTemporaryRdbmsSmss(String appId, String appName, File owlFile, String rdbmsType, String file) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);
		
		// i am okay with deleting the .temp if it exists
		// we dont leave this around 
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if(appTempSmss.exists()) {
			appTempSmss.delete();
		}
		
		final String newLine = "\n";
		final String tab = "\t";
		
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			writer = new FileWriter(appTempSmss);
			bufferedWriter = new BufferedWriter(writer);
			
			String engineClassName = "";
			if(rdbmsType.equals(RdbmsConnectionHelper.IMPALA)) {
				engineClassName = ImpalaEngine.class.getName();
			} else {
				engineClassName = RDBMSNativeEngine.class.getName();
			}
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, engineClassName, newLine, tab);

			// write the rdbms type
			bufferedWriter.write(Constants.RDBMS_TYPE + tab + rdbmsType + newLine);
			// write the driver
			bufferedWriter.write(Constants.DRIVER + tab + RdbmsConnectionHelper.getDriver(rdbmsType) + "\n");
			// write the username
			bufferedWriter.write(Constants.USERNAME + tab + "sa" + newLine);
			// write the password
			bufferedWriter.write(Constants.PASSWORD + tab + newLine);
			// most important piece
			// the connection url
			bufferedWriter.write(Constants.CONNECTION_URL + "\t" + RDBMSUtility.getH2BaseConnectionURL().replace('\\', '/') + "\n");
			
//			if(queryUtil.getDatabaseType().equals(SQLQueryUtil.DB_TYPE.H2_DB)) {
//				if(fileName == null) {
//					bufferedWriter.write(Constants.CONNECTION_URL + "\t" + RDBMSUtility.getH2BaseConnectionURL() + "\n");
//				} else {
//					bufferedWriter.write("USE_FILE" + "\ttrue\n");
//					fileName = fileName.replace(baseFolder, "@BaseFolder@");
//					fileName = fileName.replace(dbname, SmssUtilities.ENGINE_REPLACEMENT);
//					// strip the stupid ;
//					fileName = fileName.replace(";", "");
//					bufferedWriter.write("DATA_FILE" + "\t" + fileName+"\n");
//					bufferedWriter.write(Constants.CONNECTION_URL + "\t" + RDBMSUtility.getH2BaseConnectionURL2() + "\n");
//				}
//			} else {
//				bufferedWriter.write(Constants.CONNECTION_URL + "\t" + queryUtil.getConnectionURL(baseFolder,dbname) + "\n");
//			}
//			bufferedWriter.write(Constants.USE_OUTER_JOINS + "\t" + queryUtil.getDefaultOuterJoins()+ "\n");
//			//commenting out this item below by default
//			bufferedWriter.write("# " + Constants.USE_CONNECTION_POOLING + "\t" + queryUtil.getDefaultConnectionPooling());

			
		
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not generate temporary smss file for app");
		} finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return appTempSmss;
	}
	
	/**
	 * Generate the SMSS for the empty app
	 * @param appName
	 * @throws IOException 
	 */
	public static File createTemporaryAppSmss(String appId, String appName) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);
		
		// i am okay with deleting the .temp if it exists
		// we dont leave this around 
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if(appTempSmss.exists()) {
			appTempSmss.delete();
		}
		
		final String newLine = "\n";
		final String tab = "\t";
		
		// also write the base properties
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			File newFile = new File(appTempSmssLoc);
			writer = new FileWriter(newFile);
			bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.write("Base Properties" +  newLine);
			bufferedWriter.write(Constants.ENGINE + tab + appId + newLine);
			bufferedWriter.write(Constants.ENGINE_ALIAS + tab + appName + newLine);
			bufferedWriter.write(Constants.ENGINE_TYPE + tab + AppEngine.class.getName() + newLine);
			// write insights rdbms
			bufferedWriter.write(Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation() + newLine);
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return appTempSmss;
	}
	
	/**
	 * Generate a tinker smss
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param tinkerFilePath
	 * @param typeMap
	 * @param nameMap
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	public static File generateTemporaryTinkerSmss(String appId, String appName, File owlFile, String tinkerFilePath, Map<String, String> typeMap, Map<String, String> nameMap, TINKER_DRIVER tinkerDriverType) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around 
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if(appTempSmss.exists()) {
			appTempSmss.delete();
		}
		
		final String newLine = "\n";
		final String tab = "\t";
		
		// also write the base properties
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			File newFile = new File(appTempSmssLoc);
			writer = new FileWriter(newFile);
			bufferedWriter = new BufferedWriter(writer);
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, TinkerEngine.class.getName(), newLine, tab);
			
			// tinker file location
			bufferedWriter.write(Constants.TINKER_FILE + tab + tinkerFilePath + newLine);
			// tinker driver
			bufferedWriter.write(Constants.TINKER_DRIVER + tab + tinkerDriverType + newLine);
			// type map
			Gson gson = new GsonBuilder().create();
			String json = gson.toJson(typeMap);
			bufferedWriter.write("TYPE_MAP" + tab + json + newLine);
			// name map
			json = gson.toJson(nameMap);
			bufferedWriter.write("NAME_MAP" + tab + json + newLine);
			
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return appTempSmss;
	}

	/**
	 * Generate a temporary datastax smss
 	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param graphname
	 * @param typeMap
	 * @param nameMap
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	public static File generateTemporaryDatastaxSmss(String appId, String appName, File owlFile, String host, String port, String username, String password, String graphName, Map<String, String> typeMap, Map<String, String> nameMap) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around 
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if(appTempSmss.exists()) {
			appTempSmss.delete();
		}
		
		final String newLine = "\n";
		final String tab = "\t";
		
		// also write the base properties
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			File newFile = new File(appTempSmssLoc);
			writer = new FileWriter(newFile);
			bufferedWriter = new BufferedWriter(writer);
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, DataStaxGraphEngine.class.getName(), newLine, tab);
			
			// tinker file location
			bufferedWriter.write("HOST" + tab + host + newLine);
			bufferedWriter.write("PORT" + "\t" + port + newLine);
			if(username != null){
				bufferedWriter.write("USERNAME" + tab + username + newLine);
			}
			if(password != null) {
				bufferedWriter.write("PASSWORD" + tab + password + newLine);
			}
			bufferedWriter.write("GRAPH_NAME" + tab + graphName + newLine);
			
			// type map
			Gson gson = new GsonBuilder().create();
			String json = gson.toJson(typeMap);
			bufferedWriter.write("TYPE_MAP" + "\t" + json + "\n");
			// name map
			json = gson.toJson(nameMap);
			bufferedWriter.write("NAME_MAP" + "\t" + json + "\n");
			
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return appTempSmss;
	}
	
	/**
	 * Get the app temporary smss location
	 * @param appName
	 * @return
	 */
	private static String getAppTempSmssLoc(String appId, String appName) {
		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String appTempSmssLoc = baseDirectory + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId) + ".temp";
		return appTempSmssLoc;
	}
	
	/**
	 * Writes the shared properties across majority of engines. This includes:
	 * 1) Engine Name
	 * 2) Engine Type
	 * 3) Insights database locaiton
	 * 4) OWL file locaiton
	 * 
	 * @param bufferedWriter
	 * @param appName
	 * @param owlFile
	 * @param engineClassName
	 * @param newLine
	 * @param tab
	 * @throws IOException
	 */
	private static void writeDefaultSettings(BufferedWriter bufferedWriter, String appId, String appName, File owlFile, String engineClassName, final String newLine, final String tab) throws IOException {
		bufferedWriter.write("Base Properties" +  newLine);
		bufferedWriter.write(Constants.ENGINE + tab + appId + newLine);
		bufferedWriter.write(Constants.ENGINE_ALIAS + tab + appName + newLine);
		bufferedWriter.write(Constants.ENGINE_TYPE + tab + engineClassName + newLine);
		// write insights rdbms
		bufferedWriter.write(Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation() + newLine);
		// write owl
		String paramOwlLoc = getRelativeOwlPath(owlFile).replaceFirst(SmssUtilities.getUniqueName(appName, appId), SmssUtilities.ENGINE_REPLACEMENT);
		bufferedWriter.write(Constants.OWL + tab + paramOwlLoc + newLine);
	}
	
	/**
	 * Get the smss base url 
	 * NOTE : THIS IS NOT THE FULL URL, BUT MEANT TO BE USED ONLY FOR THE SMSS
	 * THE OTHER PORTIONS OF THE FULL URL ARE HARDCODED IN ABSTRACT ENGINE
	 * @param appName
	 * @return
	 */
	private static String getParamedSmssInsightDatabaseLocation() {
		String connectionUrl = "db" + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT + DIR_SEPARATOR + "insights_database";
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		return connectionUrl;
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Below methods pertain to the insights database
	 */
	
	/**
	 * Get the default connection url for the insights database
	 * NOTE : THIS IS HARD CODED FOR H2
	 * NOTE : THIS IS THE ACTUAL FULL CONNECITON URL
	 * @param appName
	 * @return
	 */
	public static String getInsightDatabaseConnectionUrl(String appId, String appName) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String connectionUrl = "jdbc:h2:" + baseFolder + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		return connectionUrl;
	}
	
	/**
	 * Generate an empty insight database
	 * @param appName
	 * @return
	 */
	public static IEngine generateInsightsDatabase(String appId, String appName) {
		Properties prop = new Properties();

		/*
		 * This has hard coded defaults for h2!
		 */
		
		String connectionUrl = getInsightDatabaseConnectionUrl(appId, appName);
		prop.put(Constants.CONNECTION_URL, connectionUrl);
		prop.put(Constants.USERNAME, "sa");
		prop.put(Constants.PASSWORD, "");
		prop.put(Constants.DRIVER, RdbmsConnectionHelper.H2_DRIVER);
		prop.put(Constants.RDBMS_TYPE, SQLQueryUtil.DB_TYPE.H2_DB.toString());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setProperties(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightEngine.openDB(null);

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

		insightEngine.insertData(questionTableCreate);

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

		insightEngine.insertData(parameterTableCreate);

		String feTableCreate = "CREATE TABLE UI ("
				+ "QUESTION_ID_FK INT, "
				+ "UI_DATA CLOB)";

		insightEngine.insertData(feTableCreate);

		return insightEngine;
	}

	/**
	 * Add explore an instance to the insights database
	 * @param appId
	 * @param insightEngine
	 */
	public static void addExploreInstanceInsight(String appId, IEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String insightName = "Explore an instance of a selected node type";
		String layout = "Graph";
		String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\ExploreInstanceDefaultWidget.json";
		File exploreF = new File(exploreLoc);
		if(exploreF.exists()) {
			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(exploreF.toPath()))
						.replaceAll("\n|\r|\t", "")
						.replaceAll("\\s\\s+", "")
						.replace("<<ENGINE>>", appId);
				newPixel += "} </encode>\" ) ;";
				String[] pkqlRecipeToSave = {newPixel};
				admin.addInsight(insightName, layout, pkqlRecipeToSave);
				insightEngine.commit();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
