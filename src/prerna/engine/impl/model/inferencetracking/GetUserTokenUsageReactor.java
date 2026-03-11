package prerna.engine.impl.model.inferencetracking;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetUserTokenUsageReactor extends AbstractReactor {

	public GetUserTokenUsageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "days"// time period in days

		};
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		organizeKeys();

		String daysParam = this.keyValue.get("days");
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		if (!SecurityEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user is not an owner of Engine");
		}

		// Parse days parameter, default to 30
		int days;
		try {
			if (daysParam == null || daysParam.isEmpty()) {
				days = 30;
			} else {
				days = Integer.parseInt(daysParam);
				if (days <= 0) {
					days = 30;
				}
			}
		} catch (NumberFormatException e) {
			days = 30;
		}

		// Calculate the start date based on the number of days
		ZonedDateTime currentDate = Utility.getCurrentZonedDateTimeUTC();
		ZonedDateTime startDate = currentDate.minusDays(days);

		String userId = user.getAccessToken(user.getLogins().get(0)).getId();

		// Calculate total token usage for this user across the specified engines
		int totalTokens = ModelInferenceLogsUtils.getUserTokenUsageForPeriod(userId, engineId, startDate, currentDate);
		Map<String, Object> result = new HashMap<>();
		result.put("user_id", userId);
		result.put("total_tokens_used", totalTokens);
		result.put("days", days);
		result.put("engineId", engineId);
		result.put("period_start", startDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
		result.put("period_end", currentDate.format(DateTimeFormatter.ISO_LOCAL_DATE));

		return new NounMetadata(result, PixelDataType.MAP);
	}

}
