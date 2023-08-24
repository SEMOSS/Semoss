package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetDatabaseConnectionsReactor extends AbstractReactor {
	
	public GetDatabaseConnectionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String databaseId = getDatabaseId();
		if(databaseId != null) {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
		}
		
		List<String> appliedDatabaseFilters = new Vector<String>();
		
		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
		List<String> databaseFilters = SecurityEngineUtils.getFullUserDatabaseIds(this.insight.getUser());
		if(!databaseFilters.isEmpty()) {
			if(databaseId != null) {
				// need to make sure it is a valid engine id
				if(!databaseFilters.contains(databaseId)) {
					throw new IllegalArgumentException("Database does not exist or user does not have access to database");
				}
				// we are good
				appliedDatabaseFilters.add(databaseId);
			} else {
				// set default as filters
				appliedDatabaseFilters = databaseFilters;
			}
		} else {
			if(databaseId != null) {
				appliedDatabaseFilters.add(databaseId);
			}
		}
		
		List<String> inputColumnValues = getColumns();
		List<String> localConceptIds = MasterDatabaseUtility.getLocalConceptIdsFromLogicalName(inputColumnValues);
		localConceptIds.addAll(MasterDatabaseUtility.getConceptualIdsWithSimilarLogicalNames(localConceptIds));
		
		List<Map<String, Object>> data = MasterDatabaseUtility.getDatabaseConnections(localConceptIds, appliedDatabaseFilters);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
	}
	
	/**
	 * Getter for the list
	 * @return
	 */
	private List<String> getColumns() {
		// is it defined within store
		{
			GenRowStruct cGrs = this.store.getNoun(this.keysToGet[0]);
			if(cGrs != null && !cGrs.isEmpty()) {
				List<String> columns = new Vector<String>();
				for(int i = 0; i < cGrs.size(); i++) {
					String value = cGrs.get(i).toString().toLowerCase();
					if(value.contains("__")) {
						columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
					} else {
						columns.add(value.replaceAll("\\s+", "_"));
					}
				}
				return columns;
			}
		}
		
		// is it inline w/ currow
		List<String> columns = new Vector<String>();
		for(int i = 0; i < this.curRow.size(); i++) {
			String value = this.curRow.get(i).toString().toLowerCase();
			if(value.contains("__")) {
				columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
			} else {
				columns.add(value.replaceAll("\\s+", "_"));
			}
		}
		return columns;
	}
	
	private String getDatabaseId() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}
}
