package prerna.logging;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.date.SemossDate;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetAuditLogsReportFilterOptionListReactor extends AbstractReactor {

	public GetAuditLogsReportFilterOptionListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (!Utility.isAuditLogsDatabaseEnabled()) {
			throw new IllegalArgumentException("Audit logs have not been enabled on this instance");
		}

		organizeKeys();

		Map<String, Object> map = getMap();

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}

		// which dropdown to populate (methodName, engineType, user) and an optional
		// type-ahead term to narrow it
		String filterName = getString(map, SemossLogUtils.FILTER_NAME);
		String search = getString(map, "search");

		// scope the values to a specific project/engine/engine type when provided
		String projectId = getString(map, SemossLogUtils.PROJECT_ID);
		String engineId = getString(map, SemossLogUtils.ENGINE_ID);
		String engineType = getString(map, SemossLogUtils.ENGINE_TYPE);
		String roomId = getString(map, SemossLogUtils.ROOM_ID);

		// validate access to the project/engine and resolve which user's logs the
		// caller is allowed to see (non-owners are restricted to their own)
		AuditLogReportSecurityUtils.AuditLogAccess access = AuditLogReportSecurityUtils.authorize(this.insight,
				projectId, engineId, roomId, getString(map, SemossLogUtils.FILTER_USER_ID));
		String filterUserId = access.getFilterUserId();

		// resolve the same date range as the table so the dropdown only offers values
		// that actually appear in the currently visible rows. This endpoint is never
		// room-scoped, so with no date range supplied it defaults to a single day
		// rather than scanning the full audit logs table.
		int dateRangeValue = parseIntWithDefault(getString(map, SemossLogUtils.DATE_RANGE_VALUE), 1);
		Map<String, SemossDate> dateTimeMap = AuditLogsDateRangeMode.resolveDateRange(
				getString(map, SemossLogUtils.DATE_RANGE_TYPE), dateRangeValue,
				getString(map, SemossLogUtils.START_DATE), getString(map, SemossLogUtils.END_DATE), false);
		SemossDate startDate = dateTimeMap.get(SemossLogUtils.START_DATE);
		SemossDate endDate = dateTimeMap.get(SemossLogUtils.END_DATE);

		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		int limit = parseIntWithDefault(limitStr, -1);
		int offset = parseIntWithDefault(offsetStr, -1);

		List<String[]> resultList = AuditLogsDbUtils.getAuditLogFilterOptionList(filterUserId, projectId, engineId,
				engineType, filterName, search, startDate, endDate, limit, offset);

		return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.LOGGING_DATA);
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	/**
	 *
	 * @param map
	 * @param key
	 * @return
	 */
	private String getString(Map<String, Object> map, String key) {
		if (map == null || key == null) {
			return "";
		}
		Object val = map.get(key);
		return (val != null && !StringUtils.isBlank(val.toString())) ? val.toString().trim() : "";
	}

	/**
	 * Safely parse integer with default fallback.
	 *
	 * @param val
	 * @param defaultValue
	 * @return
	 */
	private int parseIntWithDefault(String val, int defaultValue) {
		if (val == null || val.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			return Integer.parseInt(val.trim());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

}
