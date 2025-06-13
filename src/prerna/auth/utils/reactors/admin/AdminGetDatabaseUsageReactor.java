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
import prerna.util.Utility;

public class AdminGetDatabaseUsageReactor extends AbstractReactor {

	public AdminGetDatabaseUsageReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		if (!Utility.isUserTrackingEnabled()) {
			throw new IllegalArgumentException("User tracking must be enabled for this report");
		}

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null || databaseId.isEmpty()) {
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
		return "Get the usage metrics for a database for admin users.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Database id of a database " + ReactorKeysEnum.ENGINE.getKey();
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Limit of an engine " + ReactorKeysEnum.LIMIT.getKey();
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Offset of an engine " + ReactorKeysEnum.OFFSET.getKey();
		} else if (key.equals(ReactorKeysEnum.START_DATE.getKey())) {
			return "Start date of an engine " + ReactorKeysEnum.START_DATE.getKey();
		} else if (key.equals(ReactorKeysEnum.END_DATE.getKey())) {
			return "End date of an engine " + ReactorKeysEnum.END_DATE.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
