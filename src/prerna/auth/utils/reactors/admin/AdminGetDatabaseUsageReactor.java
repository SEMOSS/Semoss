package prerna.auth.utils.reactors.admin;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;

public class AdminGetDatabaseUsageReactor extends AbstractReactor {

	public AdminGetDatabaseUsageReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must input a database id");
		}
		
		List<Map<String, Object>> databaseUsage = UserTrackingUtils.getDatabaseUsage(databaseId);
		
		return new NounMetadata(databaseUsage, PixelDataType.FORMATTED_DATA_SET); 
	}
	
	@Override
	public String getReactorDescription() {
		return "Get the usage metrics for a database";
	}

}
