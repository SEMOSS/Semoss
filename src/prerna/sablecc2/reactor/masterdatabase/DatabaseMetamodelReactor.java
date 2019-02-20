package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseMetamodelReactor extends AbstractReactor {
	
	/*
	 * Get the database metamodel + meta options
	 * OPTIONS include datatypes, logicalnames, descriptions
	 */
	
	public DatabaseMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.OPTIONS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String engineId = MasterDatabaseUtility.testEngineIdIfAlias(getDatabase());
		List<String> options = getOptions();
		
		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!	
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to database");
			}
		}
		
		boolean includeDataTypes = options.contains("datatypes");
		Map<String, Object> metamodelObject = MasterDatabaseUtility.getMetamodelRDBMS(engineId, includeDataTypes);
		// add logical names
		if(options.contains("logicalnames")) {
			metamodelObject.put("logicalNames", MasterDatabaseUtility.getEngineLogicalNames(engineId));
		}
		// add descriptions
		if(options.contains("descriptions")) {
			metamodelObject.put("descriptions", MasterDatabaseUtility.getEngineDescriptions(engineId));
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
