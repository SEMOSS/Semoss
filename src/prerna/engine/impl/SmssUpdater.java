package prerna.engine.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.forms.AbstractFormBuilder;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

@Deprecated
public class SmssUpdater {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/*
	 * Only used to update the smss files
	 * Should hopefully be able to delete this soon
	 * 
	 */

	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		// update to include engine id in each smss file
		SmssUpdater.run();
	}

	public static void run() {
		System.out.println("LOOKING TO UPDATE THE FOLLOWING SMSS FILES!");
		Map<String, String> aliasToId = MasterDatabaseUtility.getEngineAliasToId();
		for(String alias : aliasToId.keySet()) {
			System.out.println(alias + " <---> " + aliasToId.get(alias));
		}

		addEngineIdToSmss(aliasToId);
		updateSmssFileName();
		updateEngineFolderName();
		updateLegacyPropValues();
	}

	/**
	 * Inlucde the engine id in each smss file
	 */
	private static void addEngineIdToSmss(Map<String, String> aliasToId) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		System.out.println("BASE FOLDER = " + baseFolder);
		String dbBaseFolder = baseFolder + DIR_SEPARATOR + "db";
		System.out.println("BASE FOLDER = " + dbBaseFolder);

		File dbFolder = new File(dbBaseFolder);
		FilenameFilter smssFilter = new WildcardFileFilter("*.smss");
		File[] smssFiles = dbFolder.listFiles(smssFilter );
		for(File smss : smssFiles) {
			Properties prop = Utility.loadProperties(smss.getAbsolutePath());

			if(prop.getProperty(Constants.ENGINE_ALIAS) == null) {
				System.out.println("UPDATING SMSS FILE = " + smss.getAbsolutePath());
				String engineName = prop.getProperty(Constants.ENGINE);
				if(engineName != null && aliasToId.containsKey(engineName)) {
					System.out.println("BEGIN UPDATE!");
					// add current engine to be engine name
					Utility.updateSMSSFile(smss.getAbsolutePath(), Constants.ENGINE, Constants.ENGINE_ALIAS, engineName);
					// change engine to point to the id
					Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.ENGINE, aliasToId.get(engineName));
					System.out.println("DONE!");
				} else if(!engineName.equals(Constants.LOCAL_MASTER_DB_NAME) && 
						!engineName.equals(Constants.SECURITY_DB) &&
						!engineName.equals(AbstractFormBuilder.FORM_BUILDER_ENGINE_NAME) && 
						!engineName.equals(Constants.THEMING_DB)){
					
					// we will make a unique id
					String newId = UUID.randomUUID().toString();
					System.out.println("BEGIN UPDATE!");
					// add current engine to be engine name
					Utility.updateSMSSFile(smss.getAbsolutePath(), Constants.ENGINE, Constants.ENGINE_ALIAS, engineName);
					// change engine to point to the id
					Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.ENGINE, newId);
					System.out.println("DONE!");
				}
			}
		}
	}

	/**
	 * Make the smss file be the engine id and the engine name
	 */
	private static void updateSmssFileName() {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		System.out.println("BASE FOLDER = " + baseFolder);
		String dbBaseFolder = baseFolder + DIR_SEPARATOR + "db";
		System.out.println("BASE FOLDER = " + dbBaseFolder);

		File dbFolder = new File(dbBaseFolder);
		FilenameFilter smssFilter = new WildcardFileFilter("*.smss");
		File[] smssFiles = dbFolder.listFiles(smssFilter );
		for(File smss : smssFiles) {
			Properties prop = Utility.loadProperties(smss.getAbsolutePath());
			String engineId = prop.getProperty(Constants.ENGINE);
			String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
			if(engineId != null && engineName != null) {
				String smssFilePath = smss.getAbsolutePath();
				String fileName = FilenameUtils.getBaseName(smssFilePath);
				if(!fileName.equals(SmssUtilities.getUniqueName(engineName, engineId))) {
					System.out.println("RENAME ORIG SMSS = " + fileName + " to NEW SMSS = " + SmssUtilities.getUniqueName(engineName, engineId));
					smss.renameTo(new File(smssFilePath.replace(fileName, SmssUtilities.getUniqueName(engineName, engineId))));
				}
			}
		}
	}
	
	
	/**
	 * Make the engine folder be engine id and engine name
	 */
	private static void updateEngineFolderName() {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		System.out.println("BASE FOLDER = " + baseFolder);
		String dbBaseFolder = baseFolder + DIR_SEPARATOR + "db";
		System.out.println("BASE FOLDER = " + dbBaseFolder);

		File dbFolder = new File(dbBaseFolder);
		FilenameFilter smssFilter = new WildcardFileFilter("*.smss");
		File[] smssFiles = dbFolder.listFiles(smssFilter );
		for(File smss : smssFiles) {
			Properties prop = Utility.loadProperties(smss.getAbsolutePath());
			String engineId = prop.getProperty(Constants.ENGINE);
			String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
			if(engineId != null && engineName != null) {
				// find the folder for the engine
				File engineFolder = new File(dbBaseFolder + DIR_SEPARATOR + engineName);
				if(engineFolder.exists() && engineFolder.isDirectory()) {
					System.out.println("RENAME ENGINE FOLDER = " + engineId + " to NEW SMSS = " + SmssUtilities.getUniqueName(engineName, engineId));
					// let us rename this to be the engine name and engine id
					engineFolder.renameTo(new File(dbBaseFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId)));
				}
			}
		}
	}
	
	/**
	 * Make sure owl components are using relative paths for owl + rdbms insights path
	 */
	private static void updateLegacyPropValues() {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		System.out.println("BASE FOLDER = " + baseFolder);
		String dbBaseFolder = baseFolder + DIR_SEPARATOR + "db";
		System.out.println("BASE FOLDER = " + dbBaseFolder);

		File dbFolder = new File(dbBaseFolder);
		FilenameFilter smssFilter = new WildcardFileFilter("*.smss");
		File[] smssFiles = dbFolder.listFiles(smssFilter );
		for(File smss : smssFiles) {
			Properties prop = Utility.loadProperties(smss.getAbsolutePath());

			// can't update those without an engine id
			String engineId = prop.getProperty(Constants.ENGINE_ALIAS);
			if(engineId == null) {
				continue;
			}
			
			// replace owl
			{
				String owl = prop.getProperty(Constants.OWL);
				if(owl != null) {
					if(owl.contains("@engine@")) {
						System.out.println("UPDATE SMSS OWL VALUE");
						String newOwl = owl.replace("@engine@", "@ENGINE@");
						Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.OWL, newOwl);
					} else if(!owl.contains("@ENGINE@")) {
						System.out.println("UPDATE SMSS OWL VALUE");
						String newOwl = "db/@ENGINE@/" + owl.substring(owl.lastIndexOf("/")+1);
						Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.OWL, newOwl);
					}
				}
			}
			// replace rdbms
			{
				String rdbms = prop.getProperty(Constants.RDBMS_INSIGHTS);
				if(rdbms != null) {
					if(rdbms.contains("@engine@")) {
						System.out.println("UPDATE SMSS RDMBS INSIGHTS VALUE");
						String newRdbms = rdbms.replace("@engine@", "@ENGINE@");
						Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.RDBMS_INSIGHTS, newRdbms);
					} else if(!rdbms.contains("@ENGINE@")) {
						System.out.println("UPDATE SMSS RDMBS INSIGHTS VALUE");
						String newRdbms = "db/@ENGINE@/" + rdbms.substring(rdbms.lastIndexOf("/")+1);
						Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.RDBMS_INSIGHTS, newRdbms);
					}
				}
			}
			
			// replace properties
			{
				String props = prop.getProperty(Constants.ENGINE_PROPERTIES);
				if(props != null && !props.contains("@ENGINE@")) {
					System.out.println("UPDATE SMSS RDMBS INSIGHTS VALUE");
					String newProps = "db/@ENGINE@/" + props.substring(props.lastIndexOf("/")+1);
					Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.ENGINE_PROPERTIES, newProps);
				}
			}
			
			// SYSTAP RDF specific ones
			{
				String jnlLocation = prop.getProperty("com.bigdata.journal.AbstractJournal.file");
				if(jnlLocation != null && !jnlLocation.contains("@ENGINE@")) {
					System.out.println("UPDATE BIG DATA ENGINE PATH");
					String newJnl = "db/@ENGINE@/" + jnlLocation.substring(jnlLocation.lastIndexOf("/")+1);
					Utility.changePropMapFileValue(smss.getAbsolutePath(), "com.bigdata.journal.AbstractJournal.file", newJnl, true);
				}
			}
			
			// OTHER RDF ONES
			{
				String owlLocation = prop.getProperty(Constants.RDF_FILE_NAME);
				if(owlLocation != null && !owlLocation.contains("@ENGINE@")) {
					System.out.println("UPDATE BIG DATA ENGINE PATH");
					String newLocation = "db/@ENGINE@/" + owlLocation.substring(owlLocation.lastIndexOf("/")+1);
					Utility.changePropMapFileValue(smss.getAbsolutePath(), Constants.RDF_FILE_NAME, newLocation);
				}
			}
			
			System.out.println("SMSS values are up to date");
		}		
	}
	
}
