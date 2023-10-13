package prerna.reactor.masterdatabase.util;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class GenerateMetamodelUtility {

	private static final Logger classLogger = LogManager.getLogger(SmssUtilities.class);
	private static final Gson gson = new GsonBuilder().create();
	
	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static Map<String, Object> getMetamodelPositions(String databaseId) {
		Map<String, Object> positions = MasterDatabaseUtility.getMetamodelPositions(databaseId);
		
		// Could not find in database, read from file
		if (positions.size() == 0) {
			classLogger.info("Pulling database positions for database " + databaseId);
			positions = EngineSyncUtility.getMetamodelPositions(databaseId);
			if(positions == null) {
				positions = getOwlMetamodelPositions(databaseId);
			}
			
			// Save positions read from file to database
			if (positions.size() > 0) {
				MasterDatabaseUtility.saveMetamodelPositions(databaseId, positions);
			}
		}
		return positions;
	}
	
	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static Map<String, Object> getOwlMetamodelPositions(String databaseId) {
//		// if on cloud - we have to pull these files so they exist
//		// even if we dont need the full database object
//		if(ClusterUtil.IS_CLUSTER) {
//			Utility.getDatabase(databaseId);
//		}
		
		Map<String, Object> positions = new HashMap<>();
		String smssFile = DIHelper.getInstance().getEngineProperty(databaseId + "_" + Constants.STORE) + "";
		if(!new File(smssFile).exists()) {
			classLogger.warn("Could not find database smss '"+smssFile+"'");
			classLogger.warn("Could not find database smss '"+smssFile+"'");
			classLogger.warn("Could not find database smss '"+smssFile+"'");
			return positions;
		}
		Properties smssProp = Utility.loadProperties(smssFile);
		if(smssProp == null) {
			classLogger.warn("Could not load smss at '"+smssFile+"'");
			classLogger.warn("Could not load smss at '"+smssFile+"'");
			classLogger.warn("Could not load smss at '"+smssFile+"'");
			return positions;
		}
		// if the file is present, pull it and load
		File owlF = SmssUtilities.getOwlFile(smssProp);
		if(owlF != null && owlF.isFile()) {
			// position file is in same folder as OWL
			String baseFolder = owlF.getParent();
			String positionJson = baseFolder + "/" + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
			File positionFile = new File(positionJson);
			// try to make the file
			if(!positionFile.exists() && !positionFile.isFile()) {
				try {
					classLogger.info("Generating metamodel layout for database " + databaseId);
					classLogger.info("This process may take some time");
					GenerateMetamodelLayout.generateLayout(databaseId);
					classLogger.info("Metamodel layout has been generated");
				} catch(Exception e) {
					classLogger.info("Exception in creating database metamodel layout");
					classLogger.error(Constants.STACKTRACE, e);
				} catch(NoClassDefFoundError e) {
					classLogger.info("Error in creating database metamodel layout");
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			
			if(positionFile.exists() && positionFile.isFile()) {
				// load the file
				Path path = positionFile.toPath();
				try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
					positions = gson.fromJson(reader, Map.class);
					EngineSyncUtility.setMetamodelPositions(databaseId, positions);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return positions;
	}

	
}
