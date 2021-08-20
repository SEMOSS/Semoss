package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseInfoReactor extends AbstractReactor {
	
	public DatabaseInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must input an database id");
		}
		
		List<Map<String, Object>> baseInfo = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityDatabaseUtils.userCanViewDatabase(this.insight.getUser(), databaseId)) {
				// you dont have access
				throw new IllegalArgumentException("Database does not exist or user does not have access to the database");
			}
			// user has access!
			baseInfo = SecurityQueryUtils.getUserDatabaseList(this.insight.getUser(), databaseId);
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			// just grab the info
			baseInfo = SecurityQueryUtils.getAllDatabaseList(databaseId);
		}
		
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any database data");
		}
		
		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		databaseInfo.putAll(SecurityDatabaseUtils.getAggregateDatabaseMetadata(databaseId));
		databaseInfo.putIfAbsent("description", "");
		databaseInfo.putIfAbsent("tags", new Vector<String>());
		return new NounMetadata(databaseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

}