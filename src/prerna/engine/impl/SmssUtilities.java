package prerna.engine.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class SmssUtilities {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String ENGINE_REPLACEMENT = "@" + Constants.ENGINE + "@";

	private SmssUtilities() {
		
	}
	
	/**
	 * Get the owl file
	 * @param prop
	 * @return
	 */
	public static File getOwlFile(Properties prop) {
		if(prop.getProperty(Constants.OWL) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String owlFile = baseFolder + DIR_SEPARATOR + prop.getProperty(Constants.OWL);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(owlFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	/**
	 * Get the insights rdbms file
	 * @param prop
	 * @return
	 */
	public static File getInsightsRdbmsFile(Properties prop) {
		if(prop.getProperty(Constants.RDBMS_INSIGHTS) == null) {
			return null;
		}
		String rdbmsInsightsType = prop.getProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsInsightsType);

		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String rdbmsInsights = Utility.normalizePath(baseFolder) + DIR_SEPARATOR + Utility.normalizePath(prop.getProperty(Constants.RDBMS_INSIGHTS));
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		
		rdbmsInsights = rdbmsInsights.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId));
		File rdbms = null;
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			if(rdbmsInsights.endsWith(".sqlite")) {
				rdbms = new File(rdbmsInsights);
			} else {
				rdbms = new File(rdbmsInsights + ".sqlite");
			}
		} else {
			// must be H2
			rdbms = new File(rdbmsInsights + ".mv.db");
		}
		return rdbms;
	}
	
	/**
	 * Get the engine properties file
	 * @param prop
	 * @return
	 */
	public static File getEngineProperties(Properties prop) {
		if(prop.getProperty(Constants.ENGINE_PROPERTIES) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String engineProps = baseFolder + DIR_SEPARATOR + prop.getProperty(Constants.ENGINE_PROPERTIES);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(engineProps.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	/**
	 * Get the unique name for the engine
	 * This is the engine id __ engine name
	 * @param prop
	 * @return
	 */
	public static String getUniqueName(Properties prop) {
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		return getUniqueName(engineName, engineId);
	}
	
	/**
	 * Get the unique name for the engine
	 * This is the engine id __ engine name
	 * @param prop
	 * @return
	 */
	public static String getUniqueName(String engineName, String engineId) {
		if(engineName == null) {
			return Utility.normalizePath(engineId);
		}
		return Utility.normalizePath(engineName + "__" + engineId);
	}
	
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * RDF specific methods
	 */
	
	/**
	 * Get the JNL location
	 * @param prop
	 * @return
	 */
	public static File getSysTapJnl(Properties prop) {
		final String PROP_NAME = "com.bigdata.journal.AbstractJournal.file";
		if(prop.getProperty(PROP_NAME) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String jnlLocation = baseFolder + DIR_SEPARATOR + prop.getProperty(PROP_NAME);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(jnlLocation.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	/**
	 * Get the rdf file location
	 * @param prop
	 * @return
	 */
	public static File getRdfFile(Properties prop) {
		if(prop.getProperty(Constants.RDF_FILE_NAME) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String rdfFileLoc = baseFolder + DIR_SEPARATOR + prop.getProperty(Constants.RDF_FILE_NAME);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(rdfFileLoc.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * File specific methods
	 */
	
	/**
	 * Get the data file 
	 * @param prop
	 * @return
	 */
	public static File getDataFile(Properties prop) {
		if(prop.getProperty(AbstractEngine.DATA_FILE) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String dataSuffix = prop.getProperty(AbstractEngine.DATA_FILE);
		if(dataSuffix.startsWith("@BaseFolder@/")) {
			dataSuffix = dataSuffix.substring("@BaseFolder@/".length());
		}
		String dataFile = baseFolder + DIR_SEPARATOR + dataSuffix;
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(dataFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	
	
	
	
	
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * Tinker specific methods
	 */

	/**
	 * Get the data file 
	 * @param prop
	 * @return
	 */
	public static File getTinkerFile(Properties prop) {
		if(prop.getProperty(Constants.TINKER_FILE) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String tinkerFile = null;
		String tinkerStr = prop.getProperty(Constants.TINKER_FILE);
		if(tinkerStr.contains("@BaseFolder@")) {
			tinkerFile = tinkerStr.replace("@BaseFolder@", baseFolder);
		} else {
			// could be external file outside of semoss base folder
			tinkerFile = tinkerStr;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(tinkerFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	/**
	 * Get the data file for an embedded neo4j graph
	 * @param prop
	 * @return
	 */
	public static File getNeo4jFile(Properties prop) {
		if(prop.getProperty(Constants.NEO4J_FILE) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String neo4jFile = null;
		String neoConfPath = prop.getProperty(Constants.NEO4J_FILE);
		if(neoConfPath.contains("@BaseFolder@")) {
			neo4jFile = neoConfPath.replace("@BaseFolder@", baseFolder);
		} else {
			// could be external file outside of semoss base folder
			neo4jFile = neoConfPath;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(neo4jFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	/**
	 * Get the data file 
	 * @param prop
	 * @return
	 */
	public static File getJanusFile(Properties prop) {
		if(prop.getProperty(Constants.JANUS_CONF) == null) {
			return null;
		}
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String janusFile = null;
		String janusConfPath = prop.getProperty(Constants.JANUS_CONF);
		if(janusConfPath.contains("@BaseFolder@")) {
			janusFile = janusConfPath.replace("@BaseFolder@", baseFolder);
		} else {
			// could be external file outside of semoss base folder
			janusFile = janusConfPath;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(Utility.normalizePath(janusFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}
	
	/**
	 * Custom file reader/writer to modify the app name and keep the same order
	 * of the smss properties. Need to change the engine alias
	 * 
	 * @param smssFile
	 * @param newSmssFile
	 * @param newAppName
	 * @throws IOException
	 */
	public static void changeAppName(String smssFile, String newSmssFile, String newAppName) throws IOException {
		final String newLine = "\n";
		final String tab = "\t";
		File f1 = new File(smssFile);
		FileReader fr = new FileReader(f1);
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		FileWriter fw = new FileWriter(newSmssFile);
		BufferedWriter out = new BufferedWriter(fw);
		while ((line = br.readLine()) != null) {
			if (line.contains(Constants.ENGINE_ALIAS)) {
				line = Constants.ENGINE_ALIAS + tab + newAppName;
			}
//			if (line.startsWith(Constants.OWL)) {
//				String owlLocation = "db" + DIR_SEPARATOR + ENGINE_REPLACEMENT + DIR_SEPARATOR + newAppName
//						+ "_OWL.OWL";
//				owlLocation = owlLocation.replace('\\', '/');
//				line = Constants.OWL + tab + owlLocation;
//			}
			out.write(line + newLine);

		}
		fr.close();
		br.close();
		out.flush();
		out.close();
	}
	
}
