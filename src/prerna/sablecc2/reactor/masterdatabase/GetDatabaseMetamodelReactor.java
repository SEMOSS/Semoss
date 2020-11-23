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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
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
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class GetDatabaseMetamodelReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().create();

	/*
	 * Get the database metamodel + meta options
	 * OPTIONS include datatypes, logicalnames, descriptions
	 */

	public GetDatabaseMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.OPTIONS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String engineId = MasterDatabaseUtility.testEngineIdIfAlias(getDatabase());
		List<String> options = getOptions();

		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!	
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to database");
			}
		}
		boolean includeDataTypes = options.contains("datatypes");

		Map<String, Object> metamodelObject = new HashMap<>();
		{
			Map<String, Object> cacheMetamodel = EngineSyncUtility.getMetamodel(engineId);
			if(cacheMetamodel != null) {
				metamodelObject.putAll(cacheMetamodel);
			} else {
				includeDataTypes = true;
				Map<String, Object> metamodel = MasterDatabaseUtility.getMetamodelRDBMS(engineId, includeDataTypes);
				metamodelObject.putAll(metamodel);
				EngineSyncUtility.setMetamodel(engineId, metamodel);
			}
		}

		// add logical names
		if(options.contains("logicalnames")) {
			Map<String, List<String>> logicalNames = EngineSyncUtility.getMetamodelLogicalNamesCache(engineId);
			if(logicalNames == null) {
				logicalNames = MasterDatabaseUtility.getEngineLogicalNames(engineId);
				EngineSyncUtility.setMetamodelLogicalNames(engineId, logicalNames);
			}
			metamodelObject.put("logicalNames", logicalNames);
		}
		// add descriptions
		if(options.contains("descriptions")) {
			Map<String, String> descriptions = EngineSyncUtility.getMetamodelDescriptionsCache(engineId);
			if(descriptions == null) {
				descriptions = MasterDatabaseUtility.getEngineDescriptions(engineId);
				EngineSyncUtility.setMetamodelDescriptions(engineId, descriptions);
			}
			metamodelObject.put("descriptions", descriptions);
		}

		// this is for the OWL positions for the new layout
		if(options.contains("positions")) {
			IEngine app = Utility.getEngine(engineId);
			// if the file is present, pull it and load
			File owlF = SmssUtilities.getOwlFile(app.getProp());
			if(owlF == null) {
				metamodelObject.put("positions", new HashMap<String, Object>());
			} else {
				File positionFile = app.getOwlPositionFile();
				// try to make the file
				if(!positionFile.exists()) {
					GenerateMetamodelLayout.generateLayout(engineId);
				}
				
				if(positionFile.exists()) {
					// load the file
					Path path = positionFile.toPath();
					try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
						Map<String, Object> positionMap = GSON.fromJson(reader, Map.class);
						metamodelObject.put("positions", positionMap);
					} catch (IOException e) {
						e.printStackTrace();
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
