package prerna.sablecc2.reactor.app.upload;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.app.AppEngine;
import prerna.engine.impl.datastax.DataStaxGraphEngine;
import prerna.engine.impl.neo4j.Neo4jEngine;
import prerna.engine.impl.r.RNativeEngine;
import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.tinker.JanusEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.om.MosfetFile;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.poi.main.helper.ImportOptions;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;
import prerna.util.gson.GsonUtility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class UploadUtilities {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String ENGINE_DIRECTORY;
	static {
		ENGINE_DIRECTORY = DIR_SEPARATOR + "db" + DIR_SEPARATOR;
	}
	
	public static final String INSIGHT_USAGE_STATS_INSIGHT_NAME = "View insight usage stats";
	public static final String INSIGHT_USAGE_STATS_LAYOUT = "Grid";
	
	public static final String EXPLORE_INSIGHT_INSIGHT_NAME = "Explore an instance of a selected node type";
	public static final String EXPLORE_INSIGHT_LAYOUT = "Graph";

	public static final String GRID_DELTA_INSIGHT_NAME = "Grid Delta";
	public static final String GRID_DELTA_LAYOUT = "Grid";

	public static final String AUDIT_MODIFICATION_VIEW_INSIGHT_NAME = "What are the modifications made to specific column(s)?";
	public static final String AUDIT_MODIFICATION_VIEW_LAYOUT = "Bar";
	
	public static final  String AUDIT_TIMELINE_INSIGHT_NAME = "What are the modifications made to the specific column(s) over time?";
	public static final  String AUDIT_TIMELINE_LAYOUT = "Line";
	
	private UploadUtilities() {

	}
	
	/**
	 * Used to update DIHelper
	 * Should only be used when making new app
	 * @param newAppName
	 * @param engine
	 * @param smssFile
	 */
	public static void updateDIHelper(String newAppId, String newAppName, IEngine engine, File smssFile) {
		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		DIHelper.getInstance().setLocalProperty(newAppId, engine);
		String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		engineNames = engineNames + ";" + newAppId;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
	}
	
	/**
	 * Update local master
	 * @param appId
	 * @throws Exception 
	 */
	public static void updateMetadata(String appId) throws Exception {
		Utility.synchronizeEngineMetadata(appId);
		SecurityUpdateUtils.addApp(appId, !AbstractSecurityUtils.securityEnabled());
	}

	
	/**
	 * Validate the app name
	 * Does validation that:
	 * 1) The input is not null/empty
	 * 2) That the app folder doesn't exist in the file directory
	 * @param appName
	 * @throws IOException
	 */
	public static void validateApp(User user, String appName, String appId) throws IOException {
		if(appName == null || appName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the app");
		}
		// need to make sure the app is unique
		boolean containsApp = false;
		if(AbstractSecurityUtils.securityEnabled()) {
			containsApp = AbstractSecurityUtils.userContainsEngineName(user, appName);
		} else {
			containsApp = AbstractSecurityUtils.containsEngineName(appName);
		}
		if(containsApp) {
			throw new IOException("App name already exists.  Please provide a unique app name");
		}
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// need to make sure app folder doesn't already exist
		String appLocation = baseFolder + ENGINE_DIRECTORY +  SmssUtilities.getUniqueName(appName, appId);
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
	
	/**
	 * 
	 * @param owler
	 * @param nodeProps
	 * @param descriptions
	 * @param logicalNames
	 */
	public static void insertOwlMetadataToGraphicalEngine(Owler owler, Map<String, List<String>> nodeProps, Map<String, String> descriptions, Map<String, List<String>> logicalNames) {
		// NOTE ::: We require the OWL to be loaded with the concepts and properties
		// to get the proper physical URLs
		
		Hashtable<String, String> conceptHash = owler.getConceptHash();
		Hashtable<String, String> propHash = owler.getPropHash();
		// take the node props
		// so we know what is a concept
		// and what is a property
		for(String table : nodeProps.keySet()) {
			// this is just to grab the concept
			String tablePhysicalUri = conceptHash.get(table);
			if(tablePhysicalUri == null) {
				System.err.println("Error with adding owl metadata on upload");
				continue;
			}
			
			// adding metadata to table physical uri
			if(descriptions != null && descriptions.containsKey(table)) {
				String desc = descriptions.get(table);
				owler.addDescription(tablePhysicalUri, desc);
			}
			
			if(logicalNames != null && logicalNames.containsKey(table)) {
				owler.addLogicalNames(tablePhysicalUri, logicalNames.get(table));
			}
			
			List<String> properties = nodeProps.get(table);
			if(!properties.isEmpty()) {
				for(int i = 0; i < properties.size(); i++) {
					String property = properties.get(i);
					String propertyPhysicaluri = propHash.get(table + "%" + property);
					if(propertyPhysicaluri == null) {
						System.err.println("Error with adding owl metadata on upload");
						continue;
					}
					
					// adding metadata to property physical uri
					if(descriptions != null && descriptions.containsKey(property)) {
						String desc = descriptions.get(property);
						owler.addDescription(propertyPhysicaluri, desc);
					}
					
					if(logicalNames != null && logicalNames.containsKey(property)) {
						owler.addLogicalNames(propertyPhysicaluri, logicalNames.get(property));
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param owler
	 * @param tableName
	 * @param headers
	 * @param descriptions
	 * @param logicalNames
	 */
	public static void insertFlatOwlMetadata(Owler owler, String tableName, String[] headers, Map<String, String> descriptions, Map<String, List<String>> logicalNames) {
		// NOTE ::: We require the OWL to be loaded with the concepts and properties
		// to get the proper physical URLs
		
		Hashtable<String, String> propHash = owler.getPropHash();

		// we have already loaded everything into a single table
		// so we will grab all the properties for that table
		for(int i = 0; i < headers.length; i++) {
			String property = headers[i];
			String propertyPhysicaluri = propHash.get(tableName + "%" + property);
			if(propertyPhysicaluri == null) {
				System.err.println("Error with adding owl metadata on upload");
				continue;
			}
			
			// adding metadata to property physical uri
			if(descriptions != null && descriptions.containsKey(property)) {
				String desc = descriptions.get(property);
				owler.addDescription(propertyPhysicaluri, desc);
			}
			
			if(logicalNames != null && logicalNames.containsKey(property)) {
				owler.addLogicalNames(propertyPhysicaluri, logicalNames.get(property));
			}
		}
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
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param rdbmsType
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryRdbmsSmss(String appId, String appName, File owlFile, RdbmsTypeEnum rdbmsType, String file) throws IOException {
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
			if(rdbmsType == RdbmsTypeEnum.IMPALA) {
				engineClassName = ImpalaEngine.class.getName();
			} else {
				engineClassName = RDBMSNativeEngine.class.getName();
			}
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, engineClassName, newLine, tab);

			// write the rdbms type
			bufferedWriter.write(Constants.RDBMS_TYPE + tab + rdbmsType + newLine);
			// write the driver
			bufferedWriter.write(Constants.DRIVER + tab + rdbmsType.getDriver() + "\n");
			// write the username
			bufferedWriter.write(Constants.USERNAME + tab + "sa" + newLine);
			// write the password
			bufferedWriter.write(Constants.PASSWORD + tab + newLine);
			// most important piece
			// the connection url
			bufferedWriter.write(Constants.CONNECTION_URL + "\t" + RDBMSUtility.getH2BaseConnectionURL().replace('\\', '/') + "\n");
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
	 * 
	 * @param appId
	 * @param appName
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryAppSmss(String appId, String appName, boolean isAssetApp) throws IOException {
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
			bufferedWriter.write("#Base Properties" +  newLine);
			bufferedWriter.write(Constants.ENGINE + tab + appId + newLine);
			bufferedWriter.write(Constants.ENGINE_ALIAS + tab + appName + newLine);
			bufferedWriter.write(Constants.ENGINE_TYPE + tab + AppEngine.class.getName() + newLine);
			if(isAssetApp) {
				bufferedWriter.write(Constants.IS_ASSET_APP + tab + true + newLine);
			}
			String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
			if(rdbmsTypeStr == null) {
				// default will be h2
				rdbmsTypeStr = "H2_DB";
			}
			bufferedWriter.write(Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation(rdbmsTypeStr) + newLine);
			bufferedWriter.write(Constants.RDBMS_INSIGHTS_TYPE + tab + rdbmsTypeStr + newLine);
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
	 * Create a temporary smss file for a tinker engine
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	public static File generateTemporaryTinkerSmss(String appId, String appName, File owlFile, TINKER_DRIVER tinkerDriverType) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if (appTempSmss.exists()) {
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

			// tinker-specific properties
			// neo4j does not have an extension
			// basefolder/db/engine/engine
			String tinkerFilePath = " @BaseFolder@" + DIR_SEPARATOR + "db" + DIR_SEPARATOR + "@ENGINE@" + DIR_SEPARATOR + appName;
			if(tinkerFilePath.contains("\\")) {
				tinkerFilePath = tinkerFilePath.replace("\\", "/");
			}
			
			// if neo4j, point to the folder
			if (tinkerDriverType == TINKER_DRIVER.NEO4J) {
				bufferedWriter.write(Constants.TINKER_FILE + tinkerFilePath + "\n");
			} else {
				// basefolder/db/engine/engine.driverTypeExtension
				bufferedWriter.write(Constants.TINKER_FILE + tinkerFilePath + "." + tinkerDriverType + "\n");
			}
			bufferedWriter.write(Constants.TINKER_DRIVER + "\t" + tinkerDriverType + "\n");

		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return appTempSmss;
	}
	
	/**
	 * Create a temporary smss file for a rdf engine
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryRdfSmss(String appId, String appName, File owlFile) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if (appTempSmss.exists()) {
			appTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;

		FileReader fileRead = null;
		BufferedReader bufferedReader = null;

		try {
			writer = new FileWriter(appTempSmss);
			bufferedWriter = new BufferedWriter(writer);

			String engineClassName = BigDataEngine.class.getName();
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, engineClassName, newLine, tab);
			// get additional RDF default properties
			String defaultDBPropName = "db" + DIR_SEPARATOR + "Default" + DIR_SEPARATOR + "Default.properties";
			String jnlName = "db" + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT + DIR_SEPARATOR + appName + ".jnl";
			jnlName = jnlName.replace('\\', '/'); // Needed as prop file cannot contain single back slash
			String rdfDefaultProps = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + defaultDBPropName;
			
			fileRead = new FileReader(rdfDefaultProps);
			bufferedReader = new BufferedReader(fileRead);
			String currentLine;
			while ((currentLine = bufferedReader.readLine()) != null) {
				if (currentLine.contains("@FileName@")) {
					currentLine = currentLine.replace("@FileName@", jnlName);
				}
				bufferedWriter.write(currentLine + "\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not generate temporary smss file for app");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
					writer.close();
				}
				if (fileRead != null) {
					fileRead.close();
				}
				if (bufferedReader != null) {
					bufferedReader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return appTempSmss;
	}

	
	/**
	 * Generate a janus smss
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param janusConfPath
	 * @param typeMap
	 * @param nameMap
	 * @return
	 * @throws IOException
	 */
	public static File generateTemporaryJanusGraphSmss(String appId, String appName, File owlFile, String janusConfPath, Map<String, String> typeMap, Map<String, String> nameMap, boolean useLabel) throws IOException {
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if (appTempSmss.exists()) {
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
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, JanusEngine.class.getName(), newLine, tab);

			// janus conf file location
			// we will want to parameterize this
			File f = new File(janusConfPath);
			String fileBasePath = f.getParent();
			janusConfPath = janusConfPath.replace(fileBasePath, "@BaseFolder@" + ENGINE_DIRECTORY + "@ENGINE@");

			if (janusConfPath.contains("\\")) {
				janusConfPath = janusConfPath.replace("\\", "\\\\");
			}
			bufferedWriter.write(Constants.JANUS_CONF + tab + janusConfPath + newLine);
			// tinker driver
			// bufferedWriter.write(Constants.TINKER_DRIVER + tab +
			// tinkerDriverType + newLine);
			// type map
			Gson gson = new GsonBuilder().create();
			// if we use the label we do not need the type map
			if (useLabel) {
				bufferedWriter.write(Constants.TINKER_USE_LABEL + tab + useLabel + newLine);
			} else {
				String json = gson.toJson(typeMap);
				bufferedWriter.write(Constants.TYPE_MAP + tab + json + newLine);
			}
			// name map
			String json = gson.toJson(nameMap);
			bufferedWriter.write(Constants.NAME_MAP + tab + json + newLine);

		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
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
	 * 
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
	public static File generateTemporaryExternalTinkerSmss(String appId, String appName, File owlFile, String tinkerFilePath, Map<String, String> typeMap, Map<String, String> nameMap, TINKER_DRIVER tinkerDriverType, boolean useLabel) throws IOException {
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
			// we will want to parameterize this
			// if it is not a Neo4j as we do not move this onto the server yet
			if(tinkerDriverType != ImportOptions.TINKER_DRIVER.NEO4J) {
				File f = new File(tinkerFilePath);
				String fileBasePath = f.getParent();
				tinkerFilePath = tinkerFilePath.replace(fileBasePath, "@BaseFolder@" + ENGINE_DIRECTORY + "@ENGINE@");
			}
			if(tinkerFilePath.contains("\\")) {
				tinkerFilePath = tinkerFilePath.replace("\\", "\\\\");
			}
			bufferedWriter.write(Constants.TINKER_FILE + tab + tinkerFilePath + newLine);
			// tinker driver
			bufferedWriter.write(Constants.TINKER_DRIVER + tab + tinkerDriverType + newLine);
			// type map
			Gson gson = new GsonBuilder().create();
			// if we use the label we do not need the type map
			if(useLabel) {
				bufferedWriter.write(Constants.TINKER_USE_LABEL + tab + useLabel + newLine);
			} else {
				String json = gson.toJson(typeMap);
				bufferedWriter.write(Constants.TYPE_MAP + tab + json + newLine);
			}
			// name map
			String json = gson.toJson(nameMap);
			bufferedWriter.write(Constants.NAME_MAP + tab + json + newLine);
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
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
	public static File generateTemporaryDatastaxSmss(String appId, String appName, File owlFile, String host, String port, String username, String password, String graphName, Map<String, String> typeMap, Map<String, String> nameMap, boolean useLabel) throws IOException {
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
			
			// host + port
			if(host.contains("\\")) {
				host = host.replace("\\", "\\\\");
			}
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
			if (useLabel) {
				bufferedWriter.write(Constants.TINKER_USE_LABEL + tab + useLabel + newLine);
			} else {
				String json = gson.toJson(typeMap);
				bufferedWriter.write(Constants.TYPE_MAP + tab + json + newLine);
			}
			// name map
			String json = gson.toJson(nameMap);
			bufferedWriter.write(Constants.NAME_MAP+ "\t" + json + "\n");
			
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
	 * Generate a neo4j smss
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param connectionStringKey
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public static File generateTemporaryExternalNeo4jSmss(String appId, String appName, File owlFile,
			String connectionStringKey, String username, String password, Map<String, String> typeMap,
			Map<String, String> nameMap, boolean useLabel) throws IOException {
		String appTempNeo4jLoc = getAppTempSmssLoc(appId, appName);

		File appTempSmss = new File(appTempNeo4jLoc);
		if (appTempSmss.exists()) {
			appTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			File newFile = new File(appTempNeo4jLoc);
			writer = new FileWriter(newFile);
			bufferedWriter = new BufferedWriter(writer);
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, Neo4jEngine.class.getName(), newLine, tab);
			// neo4j external properties
			bufferedWriter.write(Constants.CONNECTION_URL + tab + connectionStringKey + newLine);
			bufferedWriter.write(Constants.USERNAME + tab + username + newLine);
			bufferedWriter.write(Constants.PASSWORD + tab + password + newLine);
			// type map
			Gson gson = new GsonBuilder().create();
			if (useLabel) {
				bufferedWriter.write(Constants.TINKER_USE_LABEL + tab + useLabel + newLine);
			} else {
				String json = gson.toJson(typeMap);
				bufferedWriter.write(Constants.TYPE_MAP + tab + json + newLine);
			}
			// name map
			String json = gson.toJson(nameMap);
			bufferedWriter.write(Constants.NAME_MAP + "\t" + json + "\n");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return appTempSmss;
	}
	
	/**
	 * Generate a neo4j smss
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static File generateTemporaryEmbeddedNeo4jSmss(String appId, String appName, File owlFile, String filePath, Map<String, String> typeMap, Map<String, String> nameMap, boolean useLabel)
			throws IOException {
		String appTempNeo4jLoc = getAppTempSmssLoc(appId, appName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempNeo4jLoc);
		if (appTempSmss.exists()) {
			appTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			File newFile = new File(appTempNeo4jLoc);
			writer = new FileWriter(newFile);
			bufferedWriter = new BufferedWriter(writer);
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, Neo4jEngine.class.getName(), newLine, tab);
			bufferedWriter.write(Constants.NEO4J_FILE + tab + filePath + newLine);
			Gson gson = new GsonBuilder().create();
			if (useLabel) {
				bufferedWriter.write(Constants.TINKER_USE_LABEL + tab + useLabel + newLine);
			} else {
				String json = gson.toJson(typeMap);
				bufferedWriter.write(Constants.TYPE_MAP + tab + json + newLine);
			}
			// name map
			// Name map
			String json = gson.toJson(nameMap);
			bufferedWriter.write(Constants.NAME_MAP + tab + json + newLine);
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate app smss file");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return appTempSmss;
	}
	
	/**
	 * Create a temporary smss file for an external rdbms engine
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param engineClassName
	 * @param dbType
	 * @param connectionUrl
	 * @param username
	 * @param password
	 * @param jdbcPropertiesMap
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public static File createTemporaryExternalRdbmsSmss(String appId, String appName, File owlFile,
			String engineClassName, RdbmsTypeEnum dbType, String connectionUrl, 
			Map<String, Object> connectionDetails, Map<String, Object> jdbcPropertiesMap) throws IOException, SQLException {
		
		String appTempSmssLoc = getAppTempSmssLoc(appId, appName);

		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(dbType);
		
		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File appTempSmss = new File(appTempSmssLoc);
		if (appTempSmss.exists()) {
			appTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;
		try {
			writer = new FileWriter(appTempSmss);
			bufferedWriter = new BufferedWriter(writer);
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, engineClassName, newLine, tab);
			// seperate for connection details
			bufferedWriter.write(newLine);
			bufferedWriter.write(Constants.DRIVER + tab + dbType.getDriver() + newLine);

			String customUrl = (String) connectionDetails.get(AbstractSqlQueryUtil.CONNECTION_STRING);
			if(customUrl != null && !customUrl.isEmpty()) {
				// keys can be username/password
				// but some will have it as accessKey/secretKey
				// so accounting for that here
				String usernameKey = queryUtil.getConnectionUserKey();
				String passwordKey = queryUtil.getConnectionPasswordKey();
				if(connectionDetails.containsKey(usernameKey)) {
					bufferedWriter.write(usernameKey + tab + connectionDetails.get(usernameKey) + newLine);
				} else {
					bufferedWriter.write(usernameKey + tab + newLine);
				}
				if(connectionDetails.containsKey(passwordKey)) {
					bufferedWriter.write(passwordKey + tab + connectionDetails.get(passwordKey) + newLine);
				} else {
					bufferedWriter.write(passwordKey + tab + newLine);
				}
			} else {
				String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
				if(host != null && !host.isEmpty()) {
					File f = new File(host);
					if(f.exists()) {
						String fileBasePath = f.getParent();
						connectionUrl = connectionUrl.replace(fileBasePath, "@BaseFolder@" + ENGINE_DIRECTORY + "@ENGINE@");
					}
				}
				// connection details
				for(String key : connectionDetails.keySet()) {
					if(key.equals(Constants.CONNECTION_URL) 
							|| connectionDetails.get(key) == null 
							|| connectionDetails.get(key).toString().isEmpty()) {
						continue;
					}
					bufferedWriter.write(key.toUpperCase() + tab + connectionDetails.get(key) + newLine);
				}
			}
			
			// connection url
			if(connectionUrl.contains("\\")) {
				connectionUrl = connectionUrl.replace("\\", "\\\\");
			}
			bufferedWriter.write(Constants.CONNECTION_URL + tab + connectionUrl + newLine);
			bufferedWriter.write(newLine);
			
			// write the additonal jdbc properties at the end of the properties file
			for (String key: jdbcPropertiesMap.keySet()) {
				if (jdbcPropertiesMap.get(key) == null || jdbcPropertiesMap.get(key).toString().isEmpty()) {
					continue;
				}
				bufferedWriter.write(key + tab + jdbcPropertiesMap.get(key) + newLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not generate temporary smss file for app");
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return appTempSmss;
	}
	
	/**
	 * 
	 * @param appId
	 * @param appName
	 * @param owlFile
	 * @param fileName
	 * @param newHeaders
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryRSmss(String appId, String appName, File owlFile, String fileName, Map<String, String> newHeaders, Map<String, String> dataTypesMap, Map<String, String> additionalDataTypeMap) throws IOException {
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
			
			String engineClassName = RNativeEngine.class.getName();
			writeDefaultSettings(bufferedWriter, appId, appName, owlFile, engineClassName, newLine, tab);
			String dataFile = "db" + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT + DIR_SEPARATOR + fileName;
			bufferedWriter.write(AbstractEngine.DATA_FILE + tab + dataFile.replace('\\', '/') + newLine);
			// stringify maps
			Gson gson = new GsonBuilder().create();
			if (newHeaders != null && !newHeaders.isEmpty()) {
				bufferedWriter.write(Constants.NEW_HEADERS + tab + gson.toJson(newHeaders) + newLine);
			}
			if (dataTypesMap != null && !dataTypesMap.isEmpty()) {
				bufferedWriter.write(Constants.SMSS_DATA_TYPES + tab + gson.toJson(dataTypesMap) + newLine);
			}
			if (additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
				bufferedWriter.write(Constants.ADDITIONAL_DATA_TYPES + tab + gson.toJson(additionalDataTypeMap) + newLine);
			}
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
	 * Get the app temporary smss location
	 * 
	 * @param appId
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
		bufferedWriter.write("#Base Properties" +  newLine);
		bufferedWriter.write(Constants.ENGINE + tab + appId + newLine);
		bufferedWriter.write(Constants.ENGINE_ALIAS + tab + appName + newLine);
		bufferedWriter.write(Constants.ENGINE_TYPE + tab + engineClassName + newLine);
		// write insights rdbms
		String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
		if(rdbmsTypeStr == null) {
			// default will be h2
			rdbmsTypeStr = "H2_DB";
		}
		bufferedWriter.write(Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation(rdbmsTypeStr) + newLine);
		bufferedWriter.write(Constants.RDBMS_INSIGHTS_TYPE + tab + rdbmsTypeStr + newLine);
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
	private static String getParamedSmssInsightDatabaseLocation(String rdbmsTypeStr) {
		String connectionUrl = "db" + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT + DIR_SEPARATOR + "insights_database";
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		
		// append it on so it looks nicer
		if(rdbmsTypeStr.equalsIgnoreCase("SQLITE")) {
			connectionUrl += ".sqlite";
		}
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
	 * NOTE : ONLY ALLOWING FOR H2 OR SQLITE STORAGE OPTIONS AT THE MOMENT
	 * NOTE : THIS IS THE ACTUAL FULL CONNECITON URL
	 * TODO: expand how we store this information to be able to keep in another database option / shared database
	 * @param appName
	 * @return
	 */
	private static String getNewInsightDatabaseConnectionUrl(RdbmsTypeEnum rdbmsType, String appId, String appName) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		String connectionUrl = null;
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			// append .sqlite so it looks nicer - realize it is not required
			connectionUrl = "jdbc:sqlite:" + baseFolder + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "insights_database.sqlite";
		} else {
			connectionUrl = "jdbc:h2:nio:" + baseFolder + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		}
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		return connectionUrl;
	}
	
	/**
	 * Generate an empty insight database
	 * @param appName
	 * @return
	 */
	public static RDBMSNativeEngine generateInsightsDatabase(String appId, String appName) {
		String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
		if(rdbmsTypeStr == null) {
			// default will be h2
			rdbmsTypeStr = "H2_DB";
		}
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);
		
		Properties prop = new Properties();

		/*
		 * This must be either H2 or SQLite
		 */
		
		String connectionUrl = getNewInsightDatabaseConnectionUrl(rdbmsType, appId, appName);
		prop.put(Constants.CONNECTION_URL, connectionUrl);
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			// sqlite has no username/password
			prop.put(Constants.USERNAME, "");
			prop.put(Constants.PASSWORD, "");
		} else {
			prop.put(Constants.USERNAME, "sa");
			prop.put(Constants.PASSWORD, "");
		}
		prop.put(Constants.DRIVER, rdbmsType.getDriver());
		prop.put(Constants.RDBMS_TYPE, rdbmsType.getLabel());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setProp(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightEngine.openDB(null);
		insightEngine.setBasic(true);
		
		runInsightCreateTableQueries(insightEngine);
		return insightEngine;
	}
	
	/**
	 * Run the create table queries for the insights database
	 * @param insightEngine
	 */
	public static void runInsightCreateTableQueries(RDBMSNativeEngine insightEngine) {
		// CREATE TABLE QUESTION_ID (ID VARCHAR(50), QUESTION_NAME VARCHAR(255), QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225), QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP CLOB, QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB, QUESTION_IS_DB_QUERY BOOLEAN, DATA_TABLE_ALIGN VARCHAR(500), QUESTION_PKQL ARRAY)
		AbstractSqlQueryUtil queryUtil = insightEngine.getQueryUtil();
		String[] columns = null;
		String[] types = null;
		
		try {
			if(!queryUtil.tableExists(insightEngine.getConnection(), "QUESTION_ID", insightEngine.getSchema())) {
				columns = new String[]{"ID", "QUESTION_NAME", "QUESTION_PERSPECTIVE", "QUESTION_LAYOUT", "QUESTION_ORDER", 
						"QUESTION_DATA_MAKER", "QUESTION_MAKEUP", "DATA_TABLE_ALIGN", "HIDDEN_INSIGHT", "CACHEABLE", "QUESTION_PKQL"};
				types = new String[]{"VARCHAR(50)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "INT", "VARCHAR(255)", "CLOB",
						"VARCHAR(500)", "BOOLEAN", "BOOLEAN", "ARRAY"};
				// this is annoying
				// need to adjust if the engine allows array data types
				if(!queryUtil.allowArrayDatatype()) {
					types[types.length-1] = "CLOB";
				}
				
				insightEngine.insertData(queryUtil.createTable("QUESTION_ID", columns, types));
			}
			
			// adding new insight metadata
			if(!queryUtil.tableExists(insightEngine.getConnection(), "INSIGHTMETA", insightEngine.getSchema())) {
				columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"};
				types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "CLOB", "INT"};
				insightEngine.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		/*
		 * NOTE : THESE TABLES ARE LEGACY!!!!
		 */
		
		{
			/*
			 * Whenever we are finally done / have removed all our playsheet insights
			 * We can officially delete this portion
			 */
			
			// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY VARCHAR(225), PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS VARCHAR(2000), PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT BOOLEAN, PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), PARAMETER_VIEW_TYPE VARCHAR(255), QUESTION_ID_FK INT)
			
			try {
				if(!queryUtil.tableExists(insightEngine.getConnection(), "PARAMETER_ID", insightEngine.getSchema())) {
					columns = new String[]{"PARAMETER_ID", "PARAMETER_LABEL", "PARAMETER_TYPE", "PARAMETER_DEPENDENCY", "PARAMETER_QUERY", 
							"PARAMETER_OPTIONS", "PARAMETER_IS_DB_QUERY", "PARAMETER_MULTI_SELECT", "PARAMETER_COMPONENT_FILTER_ID", "PARAMETER_VIEW_TYPE", "QUESTION_ID_FK"};
					types = new String[]{"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(2000)", "VARCHAR(2000)", "BOOLEAN",
							"BOOLEAN", "VARCHAR(255)", "VARCHAR(255)", "INT"};
					insightEngine.insertData(queryUtil.createTable("PARAMETER_ID", columns, types));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			try {
				if(!queryUtil.tableExists(insightEngine.getConnection(), "UI", insightEngine.getSchema())) {
					columns = new String[]{"QUESTION_ID_FK", "UI_DATA"};
					types = new String[]{"INT", "CLOB"};
					insightEngine.insertData(queryUtil.createTable("UI", columns, types));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
		insightEngine.commit();
	}

	/**
	 * Add explore an instance to the insights database
	 * @param appId
	 * @param insightEngine
	 * @return 				String containing the new insight id
	 */
	public static String addExploreInstanceInsight(String appId, String appName, RDBMSNativeEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "ExploreInstanceDefaultWidget.json";
		File exploreF = new File(exploreLoc);
		if(exploreF.exists()) {
			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(exploreF.toPath()))
						.replaceAll("\n|\r|\t", "")
						.replaceAll("\\s\\s+", "")
						.replace("<<ENGINE>>", appId);
				newPixel += "} </encode>\" ) ;";
				List<String> pixelRecipeToSave = new Vector<>();
				pixelRecipeToSave.add(newPixel);
				String insightId = admin.addInsight(EXPLORE_INSIGHT_INSIGHT_NAME, EXPLORE_INSIGHT_LAYOUT, pixelRecipeToSave);
				//write recipe to file
				MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, EXPLORE_INSIGHT_INSIGHT_NAME, EXPLORE_INSIGHT_LAYOUT, pixelRecipeToSave, false);
				// add the git here
				String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
				List<String> files = new Vector<>();
				files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
				GitRepoUtils.addSpecificFiles(gitFolder, files);				
				GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ EXPLORE_INSIGHT_INSIGHT_NAME +" insight on"));
				return insightId;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static String addInsightUsageStats(String appId, String appName, RDBMSNativeEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		List<String> pixelRecipeToSave = new Vector<>();
		pixelRecipeToSave.add("AddPanel(panel = [ 0 ] , sheet = [ \"0\" ] );");
		pixelRecipeToSave.add("Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] );");
		pixelRecipeToSave.add("Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;");
		pixelRecipeToSave.add("useageFrame = InsightUsageStatistics ( app = [ \"" + appId + "\" ] , panel = [ \"0\" ] ) ;");
		pixelRecipeToSave.add("Frame(useageFrame) | QueryAll() | AutoTaskOptions(panel = [ \"0\" ] , layout = [ \"GRID\" ] ) | Collect(-1);");
		pixelRecipeToSave.add("SetInsightConfig({\"panels\":{\"0\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"0\":{\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"0\"}}]}]}]}}},\"sheet\":\"0\"});");
		try {
			String insightId = admin.addInsight(INSIGHT_USAGE_STATS_INSIGHT_NAME, INSIGHT_USAGE_STATS_LAYOUT, pixelRecipeToSave, false, false);
			// write recipe to file
			MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, INSIGHT_USAGE_STATS_INSIGHT_NAME, INSIGHT_USAGE_STATS_LAYOUT, pixelRecipeToSave, false);
			// add the git here
			String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
			List<String> files = new Vector<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved " + INSIGHT_USAGE_STATS_INSIGHT_NAME + " insight on"));
			return insightId;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Add grid delta to the insights database
	 * @param appId
	 * @param insightEngine
	 * @return 				String containing the new insight id
	 */
	public static String addGridDeltaInsight(String appId, String appName, RDBMSNativeEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		List<String> pixelRecipeToSave = new Vector<>();
		pixelRecipeToSave.add("AddPanel(0); Panel(0)|SetPanelView(\"grid-delta\",\"<encode>{\"database\":\"" + appId + "\"}</encode>\");");
		String insightId = admin.addInsight(GRID_DELTA_INSIGHT_NAME, GRID_DELTA_LAYOUT, pixelRecipeToSave);
		// write recipe to file
		try {
			MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, GRID_DELTA_INSIGHT_NAME, GRID_DELTA_LAYOUT, pixelRecipeToSave, false);
			// add the insight to git
			String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
			List<String> files = new Vector<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);				
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ GRID_DELTA_INSIGHT_NAME +" insight on"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return insightId;
	}
	
	/**
	 * Add the insight to check the modifications made to a column from audit db
	 * 
	 * @param appId
	 * @param insightEngine
	 */
	public static String addAuditModificationView(String appId, String appName, RDBMSNativeEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String jsonLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "AuditModificationView.json";
		File jsonFile = new File(jsonLoc);
		if (jsonFile.exists()) {
			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(jsonFile.toPath()))
						.replaceAll("\n|\r|\t", "")
						.replace("<<ENGINE>>", appId).
						replace("<<INSIGHT_NAME>>", AUDIT_MODIFICATION_VIEW_INSIGHT_NAME);
				newPixel += "} </encode>\" ) ;";
				List<String> pixelRecipeToSave = new Vector<>();
				pixelRecipeToSave.add(newPixel);
				String insightId = admin.addInsight(AUDIT_MODIFICATION_VIEW_INSIGHT_NAME, AUDIT_MODIFICATION_VIEW_LAYOUT, pixelRecipeToSave);
				//write recipe to file
				MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, AUDIT_MODIFICATION_VIEW_INSIGHT_NAME, AUDIT_MODIFICATION_VIEW_LAYOUT, pixelRecipeToSave, false);
				// add the insight to git
				String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
				List<String> files = new Vector<>();
				files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
				GitRepoUtils.addSpecificFiles(gitFolder, files);				
				GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ AUDIT_MODIFICATION_VIEW_INSIGHT_NAME +" insight on"));
				return insightId;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * Add the insight to check the modifications made to a column over time from audit db
	 * 
	 * @param appId
	 * @param insightEngine
	 */
	public static String addAuditTimelineView(String appId, String appName, RDBMSNativeEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String jsonLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "AuditTimelineView.json";
		File jsonFile = new File(jsonLoc);
		if (jsonFile.exists()) {
			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(jsonFile.toPath())).replaceAll("\n|\r|\t", "")
						.replace("<<ENGINE>>", appId)
						.replace("<<INSIGHT_NAME>>", AUDIT_TIMELINE_INSIGHT_NAME);
				newPixel += "} </encode>\" ) ;";
				List<String> pixelRecipeToSave = new Vector<>();
				pixelRecipeToSave.add(newPixel);
				String insightId = admin.addInsight(AUDIT_TIMELINE_INSIGHT_NAME, AUDIT_TIMELINE_LAYOUT, pixelRecipeToSave);
				// write recipe to file
				MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, AUDIT_TIMELINE_INSIGHT_NAME, AUDIT_TIMELINE_LAYOUT, pixelRecipeToSave, false);
				// add the insight to git
				String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
				List<String> files = new Vector<>();
				files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
				GitRepoUtils.addSpecificFiles(gitFolder, files);				
				GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ AUDIT_TIMELINE_INSIGHT_NAME +" insight on"));
				return insightId;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * Add insert form for csv
	 * 
	 * @param appId
	 * @param insightEngine
	 * @param owl
	 * @param headers
	 */
	public static void addInsertFormInsight(String appId, String appName, RDBMSNativeEngine insightEngine, Owler owl, String[] headers) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		Map<String, Map<String, SemossDataType>> metamodel = getExistingMetamodel(owl);
		// assuming single sheet
		String sheetName = metamodel.keySet().iterator().next();
		String insightName = getInsightFormName(sheetName);
		String layout = "form-builder";
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "AddPanel(0);Panel(0)|" + "SetPanelView(\"" + layout + "\", \"<encode>{\"json\":"
				+ gson.toJson(createInsertForm(appId, metamodel, headers)) + "}</encode>\");";
		List<String> pixelRecipeToSave = new Vector<>();
		pixelRecipeToSave.add(newPixel);
		String insightId = admin.addInsight(insightName, layout, pixelRecipeToSave);
		insightEngine.commit();
		// write recipe to file
		try {
			MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, insightName, layout, pixelRecipeToSave, false);
			// add the insight to git
			String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
			List<String> files = new Vector<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);				
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Add insert form for excel
	 * 
	 * @param insightDatabase
	 * @param appId
	 * @param sheetName
	 * @param propMap
	 * @param headers
	 */
	public static void addInsertFormInsight(RDBMSNativeEngine insightDatabase, String appId, String appName, String sheetName, Map<String, SemossDataType> propMap, String[] headers) {
		InsightAdministrator admin = new InsightAdministrator(insightDatabase);
		Map<String, Map<String, SemossDataType>> metamodel = new HashMap<>();
		metamodel.put(sheetName, propMap);
		// assuming single sheet
		String insightName = getInsightFormName(sheetName);
		String layout = "form-builder";
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "AddPanel(0);Panel(0)|" + "SetPanelView(\"" + layout + "\", \"<encode>{\"json\":"
				+ gson.toJson(createInsertForm(appId, metamodel, headers)) + "}</encode>\");";
		List<String> pixelRecipeToSave = new Vector<>();
		pixelRecipeToSave.add(newPixel);
		String insightId = admin.addInsight(insightName, layout, pixelRecipeToSave);
		insightDatabase.commit();
		// write recipe to file
		try {
			MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, insightName, layout, pixelRecipeToSave, false);
			// add the insight to git
			String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
			List<String> files = new Vector<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);				
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create Excel form insight using data validation map
	 * 
	 * @param insightEngine
	 * @param appId
	 * @param sheetName
	 * @param dataValidationMap
	 */
	public static void addInsertFormInsight(RDBMSNativeEngine insightEngine, String appId, String appName, String sheetName, Map<String, Object> widgetJson) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String insightName = getInsightFormName(sheetName);
		String layout = "form-builder";
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "AddPanel(0);Panel(0)|" + "SetPanelView(\"" + layout + "\", \"<encode>{\"json\":"
				+ gson.toJson(widgetJson) + "}</encode>\");";
		List<String> pixelRecipeToSave = new Vector<>();
		pixelRecipeToSave.add(newPixel);
		String insightId = admin.addInsight(insightName, layout, pixelRecipeToSave);
		insightEngine.commit();
		// write recipe to file
		try {
			MosfetSyncHelper.makeMosfitFile(appId, appName, insightId, insightName, layout, pixelRecipeToSave, false);
			// add the insight to git
			String gitFolder = AssetUtility.getAppAssetVersionFolder(appName, appId);
			List<String> files = new Vector<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);				
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * The name of the form insight
	 * 
	 * @param sheetName
	 * @return
	 */
	public static String getInsightFormName(String sheetName) {
		// sheetNames are inserted as tables all caps
		return "Insert into " + sheetName.toUpperCase() + " Form";
	}
	
	/**
	 * Map of concept to propMap with db type
	 * 
	 * @param owl
	 * @return
	 */
	public static Map<String, Map<String, SemossDataType>> getExistingMetamodel(Owler owl) {
		// need to get property types from the owl
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owl.getOwlPath(), null, null);
		// we create the meta helper to facilitate querying the engine OWL
		MetaHelper helper = new MetaHelper(rfse, null, null);
		
		List<String> conceptsList = helper.getPhysicalConcepts();
		Map<String, Map<String, SemossDataType>> existingMetaModel = new HashMap<>();
		for (String conceptPhysicalUri : conceptsList) {
			// so grab the conceptual name
			String conceptName = helper.getPixelSelectorFromPhysicalUri(conceptPhysicalUri);
			// and grab its properties
			List<String> properties = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);
			
			Map<String, SemossDataType> propMap = new HashMap<>();
			for (String prop : properties) {
				// grab the conceptual name
				String propertyPixelName = helper.getPixelSelectorFromPhysicalUri(prop);
				String owlType = helper.getDataTypes(prop);
				owlType = owlType.replace("TYPE:", "");
				SemossDataType type = SemossDataType.convertStringToDataType(owlType);
				// property conceptual uris are always /Column/Table
				String propertyConceptualName = propertyPixelName.split("__")[1];
				propMap.put(propertyConceptualName, type);
			}
			existingMetaModel.put(conceptName, propMap);
		}
		return existingMetaModel;
	}
	
	/**
	 * 
	 * @param appId
	 * @param existingMetamodel
	 * @param headers
	 * @return
	 */
	public static Map<String, Object> createInsertForm(String appId, Map<String, Map<String, SemossDataType>> existingMetamodel, String[] headers) {
		Map<String, Object> formMap = new HashMap<>();
		formMap.put("js", new Vector<>());
		formMap.put("css", new Vector<>());
		Map<String, SemossDataType> propMap = new HashMap<>();
		// assuming this is a flat table so there is only one concept
		String conceptualName = existingMetamodel.keySet().iterator().next();
		propMap = existingMetamodel.get(conceptualName);
		List<String> propertyList = new ArrayList<String>();
		// order params by header order
		for (String header : headers) {
			if (propMap.containsKey(header)) {
				propertyList.add(header);
			}
		}

		// create values and into strings for query
		StringBuilder intoString = new StringBuilder();
		StringBuilder valuesString = new StringBuilder();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			intoString.append(conceptualName + "__" + property);
			valuesString.append("(<" + property + ">)");
			if (i < propertyList.size() - 1) {
				intoString.append(",");
				valuesString.append(",");
			}
		}
		// create insert pixel map
		Map<String, Object> pixelMap = new HashMap<>();
		Map<String, Object> insertMap = new HashMap<>();
		insertMap.put("name", "Insert");
		insertMap.put("pixel", "Database(database=[\"" + appId + "\"]) | Insert (into=[" + intoString + "], values=["
				+ valuesString + "]);");
		pixelMap.put("Insert", insertMap);

		formMap.put("pixel", pixelMap);

		StringBuilder htmlSb = new StringBuilder();
		Map<String, Object> dataMap = new HashMap<>();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			htmlSb.append(FormUtility.getTextComponent(property));
			SemossDataType propType = propMap.get(property);
			// build html based on input component
			if (propType == SemossDataType.DATE) {
				htmlSb.append(FormUtility.getDatePickerComponent(property));
			} else if (propType == SemossDataType.STRING) {
				htmlSb.append(FormUtility.getInputComponent(property));
			} else if (Utility.isNumericType(propType.toString())) {
				htmlSb.append(FormUtility.getNumberPickerComponent(property));
			}
			
			// build data property map for data binding
			Map<String, Object> propertyMap = new HashMap<>();
			propertyMap.put("defaultValue", "");
			propertyMap.put("options", new Vector());
			propertyMap.put("name", property);
			propertyMap.put("dependsOn", new Vector());
			propertyMap.put("required", true);
			propertyMap.put("autoPopulate", false);
			Map<String, Object> configMap = new HashMap<>();
			configMap.put("table", conceptualName);
			Map<String, Object> appMap = new HashMap<>();
			appMap.put("value", appId);
			configMap.put("app", appMap);
			propertyMap.put("config", configMap);
			propertyMap.put("pixel", "");

			// adding pixel data binding for non-numeric values
			if (propType == SemossDataType.STRING) {
				String pixel = "Database( database=[\"" + appId + "\"] )|" + "Select(" + conceptualName + "__"
						+ property + ").as([" + property + "])| Collect(-1);";
				propertyMap.put("pixel", pixel);
			} else if (Utility.isNumericType(propType.toString())) {
				propertyMap.put("defaultValue", "0");
			}
			dataMap.put(property, propertyMap);
		}
		htmlSb.append(FormUtility.getSubmitComponent("Insert"));
		formMap.put("html", htmlSb.toString());
		formMap.put("data", dataMap);
		return formMap;
	}

	public static Map<String, Object> createUpdateMap(String appId, Owler owl, String concept,
			Map<String, SemossDataType> propMap) {
		Map<String, Object> updateMap = new HashMap<>();
		updateMap.put("database", appId);
		updateMap.put("table", concept);
		// config map
		Map<String, Object> configMap = new HashMap<>();
		for (String property : propMap.keySet()) {

			Map<String, Object> configPropMap = new HashMap<>();

			SemossDataType type = propMap.get(property);
			boolean readOnly = false;
			if (property.equals(concept)) {
				// assume this is the auto generated column
				// users should not modify this
				readOnly = true;
			}
			configPropMap.put("read-only", readOnly);
			if (type == SemossDataType.DOUBLE) {
				ArrayList<String> validationList = new ArrayList<>();
				String regex = "^\\d+(\\.\\d*)?$";
				validationList.add(regex);
				configPropMap.put("validation", validationList);
			} else if (type == SemossDataType.INT) {
				ArrayList<String> validationList = new ArrayList<>();
				String regex = "^\\d*$";
				validationList.add(regex);
				configPropMap.put("validation", validationList);
			} else if (type == SemossDataType.STRING) {
//				configPropMap.put("selection-type", "database");
			} else if(type == SemossDataType.DATE) {
				// yyyy-mm-dd
				ArrayList<String> validationList = new ArrayList<>();
				String regex = "^\\d{4}-\\d{2}-\\d{2}$";
				validationList.add(regex);
				configPropMap.put("validation", validationList);
			}
			configMap.put(property, configPropMap);
		}
		updateMap.put("config", configMap);
		return updateMap;
	}

	
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Parse the file
	 * @param filePath
	 * @param delimiter
	 * @param dataTypesMap
	 * @param newHeaders
	 * @return
	 */
	public static CSVFileHelper getHelper(final String filePath, final String delimiter, 
			Map<String, String> dataTypesMap, Map<String, String> newHeaders) {
		CSVFileHelper csvHelper = new CSVFileHelper();
		csvHelper.setDelimiter(delimiter.charAt(0));
		csvHelper.parse(filePath);
		
		// if the user has cleaned any headers
		if(newHeaders != null && !newHeaders.isEmpty()) {
			csvHelper.modifyCleanedHeaders(newHeaders);
		}
		
		// specify the columns to use
		// default will include all
		if(dataTypesMap != null && !dataTypesMap.isEmpty()) {
			Set<String> headersToUse = new TreeSet<String>(dataTypesMap.keySet());
			csvHelper.parseColumns(headersToUse.toArray(new String[]{}));
		}
		return csvHelper;
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
	public static Object[] getHeadersAndTypes(CSVFileHelper helper, Map<String, String> dataTypesMap, Map<String, String> additionalDataTypeMap) {
		String[] headers = helper.getHeaders();
		int numHeaders = headers.length;
		// we want types
		// and we want additional types
		SemossDataType[] types = new SemossDataType[numHeaders];
		String[] additionalTypes = new String[numHeaders];

		// get the types
		if(dataTypesMap == null || dataTypesMap.isEmpty()) {
			Map[] retMap = FileHelperUtil.generateDataTypeMapsFromPrediction(headers, helper.predictTypes());
			dataTypesMap = retMap[0];
			additionalDataTypeMap = retMap[1];
		}
		
		for(int i = 0; i < numHeaders; i++) {
			types[i] = SemossDataType.convertStringToDataType(dataTypesMap.get(headers[i]));
		}

		// get additional type information
		if(additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
			for(int i = 0 ; i < numHeaders; i++) {
				additionalTypes[i] = additionalDataTypeMap.get(headers[i]);
			}
		}

		return new Object[]{headers, types, additionalTypes};
	}
	//////////////////////////////////////////////
	/////////////////////////////////////////////
	
	/**
	 * Save metamodel structure to json in app folder
	 * @param appId
	 * @param appName
	 * @param csvFileName
	 * @param metamodel
	 * @return
	 */
	public static boolean createPropFile(String appId, String appName, String csvFilePath, Map<String, Object> metamodel) {
		String csvFileName = new File(csvFilePath).getName().replace(".csv", "");
		Date currDate = Calendar.getInstance().getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmssZ");
		String dateName = sdf.format(currDate);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appLocation = baseFolder + ENGINE_DIRECTORY + SmssUtilities.getUniqueName(appName, appId);
		String metaModelFilePath = appLocation + DIR_SEPARATOR + appName + "_" + csvFileName + "_" + dateName + "_PROP.json";
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(metamodel);
		// create file
		File f = new File(Utility.normalizePath(metaModelFilePath));
		try {
			// write json to file
			FileUtils.writeStringToFile(f, json);
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Return map for uploading a new app
	 * @param appId
	 * @return
	 */
	public static Map<String, Object> getAppReturnData(User user, String appId) {
		List<Map<String, Object>> baseInfo = SecurityQueryUtils.getUserDatabaseList(user, appId);
		Map<String, Object> retMap = baseInfo.get(0);
		return retMap;
	}

}
