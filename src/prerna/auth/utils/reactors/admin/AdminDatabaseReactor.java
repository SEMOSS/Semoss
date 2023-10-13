package prerna.auth.utils.reactors.admin;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

public class AdminDatabaseReactor extends AbstractQueryStructReactor {
	
	public AdminDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		// get the selectors
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null || engineId.isEmpty()) {
			throw new NullPointerException("The engine id cannot be null for this operation");
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