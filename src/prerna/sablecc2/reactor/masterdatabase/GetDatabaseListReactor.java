package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabase;
import prerna.engine.api.IDatabase.DATABASE_TYPE;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.sql.RdbmsTypeEnum;

public class GetDatabaseListReactor extends AbstractReactor {
	
	public GetDatabaseListReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP_TYPE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<Map<String, Object>> retList = null;
		List<String> appTypeFilter = getAppTypeFilter();
		if(AbstractSecurityUtils.securityEnabled()) {
			retList = SecurityEngineUtils.getUserDatabaseList(this.insight.getUser(), appTypeFilter);
		} else {
			retList = SecurityEngineUtils.getAllDatabaseList(appTypeFilter);
		}
		
		return new NounMetadata(retList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
	}
	
	private List<String> getAppTypeFilter() {
		// get input list
		List<String> retTypes = new Vector<String>();
		GenRowStruct headerTypesGrs = this.store.getNoun(this.keysToGet[0]);
		if(headerTypesGrs != null && !headerTypesGrs.isEmpty()) {
			retTypes = headerTypesGrs.getAllStrValues();
		}
		
		if(retTypes.isEmpty()) {
			retTypes = this.curRow.getAllStrValues();
		}
		
		// create filters
		List<String> appTypeFilter = new Vector<String>();
		for(String appFilter : retTypes) {
			DATABASE_TYPE dbType = IDatabase.DATABASE_TYPE.valueOf(appFilter);
			if(dbType == IDatabase.DATABASE_TYPE.RDBMS) {
				// grab lower level rdbms types to filter
				for( RdbmsTypeEnum rdbmsType: RdbmsTypeEnum.values()) {
					appTypeFilter.add(rdbmsType.getLabel());
				}
			} else {
				appTypeFilter.add(dbType.toString());
			}
		}
		

		
		return appTypeFilter;
	}

}
