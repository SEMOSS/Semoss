package prerna.usertracking.reactors;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Utility;

public class GetDatabaseUsageReactor extends AbstractReactor {

	public GetDatabaseUsageReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();

		if (!Utility.isUserTrackingEnabled()) {
			throw new IllegalArgumentException("User Tracking Must be Enabled For this Report");
		}
		
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must input a database id");
		}
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);	
		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
		
		List<Map<String, Object>> databaseUsage = UserTrackingUtils.getDatabaseUsage(databaseId, limit, offset, startDate, endDate);
		
		return new NounMetadata(databaseUsage, PixelDataType.FORMATTED_DATA_SET); 
	}
	
	@Override
	public String getReactorDescription() {
		return "Get the usage metrics for a database";
	}

}
