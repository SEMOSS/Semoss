package prerna.sablecc2.reactor.masterdatabase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.masterdatabase.util.GenerateMetamodelUtility;
import prerna.util.EngineSyncUtility;

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
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId) && 
				!SecurityEngineUtils.engineIsDiscoverable(databaseId)) {
			throw new IllegalArgumentException("Database does not exist or user does not have access to database");
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
			logger.info("Done pulling database logical names for database " + databaseId);
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
			logger.info("Done pulling database descriptions for database " + databaseId);
		}

		// this is for the OWL positions for the new layout
		if(options.contains("positions")) {
			Map<String, Object> positions = GenerateMetamodelUtility.getMetamodelPositions(databaseId);
			metamodelObject.put("positions", positions);
			logger.info("Done pulling database positions for database " + databaseId);
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
