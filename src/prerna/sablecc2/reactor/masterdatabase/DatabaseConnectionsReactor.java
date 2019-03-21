package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseConnectionsReactor extends AbstractReactor {
	
	public DatabaseConnectionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String engineId = getApp();
		if(engineId != null) {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
		}
		
		List<String> appliedAppFilters = new Vector<String>();
		
		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
		List<String> appFilters = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			appFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			if(!appFilters.isEmpty()) {
				if(engineId != null) {
					// need to make sure it is a valid engine id
					if(!appFilters.contains(engineId)) {
						throw new IllegalArgumentException("Database does not exist or user does not have access to database");
					}
					// we are good
					appliedAppFilters.add(engineId);
				} else {
					// set default as filters
					appliedAppFilters = appFilters;
				}
			} else {
				if(engineId != null) {
					appliedAppFilters.add(engineId);
				}
			}
		} else if(engineId != null) {
			appliedAppFilters.add(engineId);
		}
		
		List<String> conceptualNames = getColumns();
		List<String> logicalNames = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(conceptualNames, null);
		List<Map<String, Object>> data = MasterDatabaseUtility.getDatabaseConnections(logicalNames, appliedAppFilters);
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
					String value = cGrs.get(0).toString();
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
			String value = this.curRow.get(i).toString();
			if(value.contains("__")) {
				columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
			} else {
				columns.add(value.replaceAll("\\s+", "_"));
			}
		}
		return columns;
	}
	
	private String getApp() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}
}
