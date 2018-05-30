package prerna.engine.impl;

import java.io.File;
import java.util.Properties;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SmssUtilities {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String ENGINE_REPLACEMENT = "@" + Constants.ENGINE + "@";
	
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
		File owl = new File(owlFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)));
		return owl;
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
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String rdbmsInsights = baseFolder + DIR_SEPARATOR + prop.getProperty(Constants.RDBMS_INSIGHTS);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		File rdbms = new File(rdbmsInsights.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)) + ".mv.db");
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
		File eProps = new File(engineProps.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)));
		return eProps;
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
			return engineId;
		}
		return engineName + "__" + engineId;
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
		File jnl = new File(jnlLocation.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)));
		return jnl;
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
		File rdfFile = new File(rdfFileLoc.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)));
		return rdfFile;
	}
	
	
	
	
	
	
	
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * RDBMS specific methods
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
		String dataFile = baseFolder + DIR_SEPARATOR + prop.getProperty(AbstractEngine.DATA_FILE);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		File owl = new File(dataFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)));
		return owl;
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
			tinkerFile = baseFolder + DIR_SEPARATOR + tinkerStr;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		File tinker = new File(tinkerFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId)));
		return tinker;
	}

	
	
}
