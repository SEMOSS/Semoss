package prerna.sablecc2.reactor.app.upload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.app.AppEngine;
import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
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
	public static File generateAppFolder(String appName) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appLocation = baseFolder + ENGINE_DIRECTORY + appName;
		File appFolder = new File(appLocation);
		appFolder.mkdirs();
		return appFolder;
	}
	
	/**
	 * Generate an empty OWL file based on the app name
	 * @param appName
	 * @return
	 */
	public static File generateOwlFile(String appName) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String owlLocation = baseFolder + ENGINE_DIRECTORY + appName + DIR_SEPARATOR + appName + "_OWL.OWL";
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
	public static File createTemporaryRdbmsSmss(String appName, File owlFile, String rdbmsType, String file) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appName);
		
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
			
			bufferedWriter.write("Base Properties" + newLine);
			// write engine
			bufferedWriter.write(Constants.ENGINE + tab + appName + newLine);
			// write owl
			String paramOwlLoc = getRelativeOwlPath(owlFile).replace(appName, "@engine@");
			bufferedWriter.write(Constants.OWL + tab + paramOwlLoc + newLine);

			// write insights rdbms
			bufferedWriter.write(Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation() + newLine);
			
			// write the engine type
			bufferedWriter.write(Constants.ENGINE_TYPE + tab);
			if(rdbmsType.equals(RdbmsConnectionHelper.IMPALA)) {
				bufferedWriter.write(ImpalaEngine.class.getName());
			} else {
				bufferedWriter.write(RDBMSNativeEngine.class.getName());
			}
			bufferedWriter.write(newLine);

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
//					fileName = fileName.replace(dbname, "@ENGINE@");
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
	public static File createTemporaryAppSmss(String appName) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appName);
		
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
			bufferedWriter = new BufferedWriter(writer);
			writer = new FileWriter(newFile);
			writer.write("Base Properties" +  newLine);
			writer.write(Constants.ENGINE + tab + appName + newLine);
			writer.write(Constants.ENGINE_TYPE + tab + AppEngine.class.getName() + newLine);
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
	 * Get the app temporary smss location
	 * @param appName
	 * @return
	 */
	private static String getAppTempSmssLoc(String appName) {
		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String appTempSmssLoc = baseDirectory + ENGINE_DIRECTORY + appName + ".temp";
		return appTempSmssLoc;
	}
	
	/**
	 * Get the smss base url 
	 * NOTE : THIS IS NOT THE FULL URL, BUT MEANT TO BE USED ONLY FOR THE SMSS
	 * THE OTHER PORTIONS OF THE FULL URL ARE HARDCODED IN ABSTRACT ENGINE
	 * @param appName
	 * @return
	 */
	private static String getParamedSmssInsightDatabaseLocation() {
		String connectionUrl = "db" + DIR_SEPARATOR + "@engine@" + DIR_SEPARATOR + "insights_database";
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
	public static String getInsightDatabaseConnectionUrl(String appName) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String connectionUrl = "jdbc:h2:" + baseFolder + ENGINE_DIRECTORY + appName + DIR_SEPARATOR + "insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		return connectionUrl;
	}
	
	/**
	 * Generate an empty insight database
	 * @param appName
	 * @return
	 */
	public static IEngine generateInsightsDatabase(String appName) {
		Properties prop = new Properties();

		/*
		 * This has hard coded defaults for h2!
		 */
		
		String connectionUrl = getInsightDatabaseConnectionUrl(appName);
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
	 * @param appName
	 * @param insightEngine
	 */
	public static void addExploreInstanceInsight(String appName, IEngine insightEngine) {
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
						.replace("<<ENGINE>>", appName);
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
