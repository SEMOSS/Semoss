package prerna.sablecc2.reactor.qs.source;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class DatabaseReactor extends AbstractQueryStructReactor {

	public DatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		// get the selectors
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have access to database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist");
			}
		}

		this.qs.setEngineId(engineId);
		
		// need to account if this is a hard query struct
		if(this.qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || 
				this.qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		}
		return this.qs;
	}
}
