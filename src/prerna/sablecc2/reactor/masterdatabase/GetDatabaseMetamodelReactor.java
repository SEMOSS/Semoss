package prerna.sablecc2.reactor.masterdatabase;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class GetDatabaseMetamodelReactor extends AbstractReactor {

	/*
	 * PAYLOAD MUST MATCH THAT OF 
	 * {@link  prerna.sablecc2.reactor.frame.GetFrameMetamodelReactor}
	 */
	
	private static final String CLASS_NAME = GetDatabaseMetamodelReactor.class.getName();
	
	private static final Logger classLogger = LogManager.getLogger(GetDatabaseMetamodelReactor.class);
	private static final Gson gson = new GsonBuilder().create();

	/*
	 * Get the database metamodel + meta options
	 * OPTIONS include datatypes, logicalnames, descriptions
	 */

	public GetDatabaseMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.OPTIONS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(getDatabase());
		List<String> options = getOptions();

		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!	
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityDatabaseUtils.userCanViewDatabase(this.insight.getUser(), databaseId) && 
					!SecurityDatabaseUtils.databaseIsDiscoverable(databaseId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to database");
			}
		}
		Logger logger = getLogger(CLASS_NAME);
		boolean includeDataTypes = options.contains("datatypes");

		logger.info("Pulling database metadata for database " + databaseId);
		Map<String, Object> metamodelObject = new HashMap<>();
		{
			Map<String, Object> cacheMetamodel = EngineSyncUtility.getMetamodel(databaseId);
			if(cacheMetamodel != null) {
				metamodelObject.putAll(cacheMetamodel);
			} else {
				includeDataTypes = true;
				Map<String, Object> metamodel = MasterDatabaseUtility.getMetamodelRDBMS(databaseId, includeDataTypes);
				metamodelObject.putAll(metamodel);
				EngineSyncUtility.setMetamodel(databaseId, metamodel);
			}
		}

		// add logical names
		if(options.contains("logicalnames")) {
			logger.info("Pulling database logical names for database " + databaseId);
			Map<String, List<String>> logicalNames = EngineSyncUtility.getMetamodelLogicalNamesCache(databaseId);
			if(logicalNames == null) {
				logicalNames = MasterDatabaseUtility.getDatabaseLogicalNames(databaseId);
				EngineSyncUtility.setMetamodelLogicalNames(databaseId, logicalNames);
			}
			metamodelObject.put("logicalNames", logicalNames);
		}
		// add descriptions
		if(options.contains("descriptions")) {
			logger.info("Pulling database descriptions for database " + databaseId);
			Map<String, String> descriptions = EngineSyncUtility.getMetamodelDescriptionsCache(databaseId);
			if(descriptions == null) {
				descriptions = MasterDatabaseUtility.getDatabaseDescriptions(databaseId);
				EngineSyncUtility.setMetamodelDescriptions(databaseId, descriptions);
			}
			metamodelObject.put("descriptions", descriptions);
		}

		// this is for the OWL positions for the new layout
		if(options.contains("positions")) {
			logger.info("Pulling database positions for database " + databaseId);
			IEngine app = Utility.getEngine(databaseId);
			// if the file is present, pull it and load
			File owlF = SmssUtilities.getOwlFile(app.getProp());
			if(owlF == null) {
				metamodelObject.put("positions", new HashMap<String, Object>());
			} else {
				File positionFile = app.getOwlPositionFile();
				// try to make the file
				if(!positionFile.exists()) {
					try {
						logger.info("Generating metamodel layout for database " + databaseId);
						logger.info("This process may take some time");
						GenerateMetamodelLayout.generateLayout(databaseId);
						logger.info("Metamodel layout has been generated");
					} catch(Exception e) {
						classLogger.info("Error in creating database metamodel layout");
						classLogger.error(Constants.STACKTRACE, e);
					} catch(NoClassDefFoundError e) {
						classLogger.info("Error in creating database metamodel layout");
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
				
				if(positionFile.exists()) {
					// load the file
					Path path = positionFile.toPath();
					try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
						Map<String, Object> positionMap = gson.fromJson(reader, Map.class);
						metamodelObject.put("positions", positionMap);
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		return new NounMetadata(metamodelObject, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_METAMODEL);
	}

	private String getDatabase() {
		GenRowStruct eGrs = this.store.getNoun(this.keysToGet[0]);
		if(eGrs != null && !eGrs.isEmpty()) {
			if(eGrs.size() > 1) {
				throw new IllegalArgumentException("Can only define one database within this call");
			}
			return eGrs.get(0).toString();
		}

		if(this.curRow.isEmpty()) {
			throw new IllegalArgumentException("Need to define the database to get the concepts from");
		}

		return this.curRow.get(0).toString();
	}

	private List<String> getOptions() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues().stream().map(p -> p.toLowerCase()).collect(Collectors.toList());
		}

		List<String> options = new Vector<String>();
		for(int i = 1; i < this.curRow.size(); i++) {
			options.add(this.curRow.get(i).toString().toLowerCase());
		}
		return options;
	}

}
