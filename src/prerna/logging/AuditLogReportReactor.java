package prerna.logging;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.date.SemossDate;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AuditLogReportReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AuditLogReportReactor.class);

	private final ZoneId utcZone = ZoneId.of("UTC");

	public AuditLogReportReactor() {
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
		User user = this.insight.getUser();

		String projectId = getString(map, SemossLogUtils.PROJECT_ID);
		String engineId = getString(map, SemossLogUtils.ENGINE_ID);
		String roomId = getString(map, SemossLogUtils.ROOM_ID);
		String sessionId = getString(map, SemossLogUtils.SESSION_ID);

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}

		Integer userPermissionLvl = null;
		if (projectId != null && !projectId.equals("")) {
			userPermissionLvl = SecurityProjectUtils.getUserProjectPermission(user.getPrimaryLoginToken().getId(),
					projectId);
			if (userPermissionLvl == null) {
				if (SecurityProjectUtils.projectIsGlobal(projectId)) {
					userPermissionLvl = AccessPermissionEnum.READ_ONLY.getId();
				}
			}
		} else if (engineId != null && !engineId.equals("")) {
			userPermissionLvl = SecurityEngineUtils.getUserEnginePermission(user.getPrimaryLoginToken().getId(),
					engineId);
			if (userPermissionLvl == null) {
				if (SecurityEngineUtils.engineIsGlobal(engineId)) {
					userPermissionLvl = AccessPermissionEnum.READ_ONLY.getId();
				}
			}
		} else {
			// throw error if no project and engine id
			throw new IllegalArgumentException("Engine or Project id must be passed in");
		}

		if (userPermissionLvl == null) {
			throw new IllegalArgumentException("User does not have access");
		}

		String filterUserId = null;
		if (AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// If Author selected a specific user in the filter
			filterUserId = getString(map, SemossLogUtils.FILTER_USER_ID);
		} else {
			filterUserId = user.getPrimaryLoginToken().getId();
		}

		String limitStr = getString(map, ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = getString(map, ReactorKeysEnum.OFFSET.getKey());
		int limit = parseIntWithDefault(limitStr, -1);
		int offset = parseIntWithDefault(offsetStr, 0);

		// dateRangeType: "day"|"week"|"month"|"custom"
		String dateRangeType = getString(map, SemossLogUtils.DATE_RANGE_TYPE);
		AuditLogsDateRangeMode mode = AuditLogsDateRangeMode.from(dateRangeType);

		// number value for dateRangeType (ignored for custom). If null -> default 1
		int dateRangeValue = parseIntWithDefault(getString(map, SemossLogUtils.DATE_RANGE_VALUE), 1);
		if (mode == AuditLogsDateRangeMode.CUSTOM && dateRangeValue < 1) {
			throw new IllegalArgumentException("dateRangeValue must be > 1");
		}
		// used only when dateRangeType is custom
		String startDateCustom = getString(map, SemossLogUtils.START_DATE);
		String endDateCustom = getString(map, SemossLogUtils.END_DATE);

		Map<String, SemossDate> dateTimeMap = determineDateRangeFilter(mode, dateRangeValue, startDateCustom,
				endDateCustom);
		SemossDate startDate = dateTimeMap.get(SemossLogUtils.START_DATE);
		SemossDate endDate = dateTimeMap.get(SemossLogUtils.END_DATE);
		List<LogActivityDto> result = Collections.emptyList();
		long totalCount = 0;
		try {
			result = AuditLogsDbUtils.getAuditLogsTimeLineDatas(filterUserId, projectId, engineId, startDate, endDate,
					roomId, sessionId, limit, offset);
			// Get total record count
			totalCount = AuditLogsDbUtils.getAuditLogsCount(filterUserId, projectId, engineId, startDate, endDate,
					roomId, sessionId);
		} catch (SQLException e) {
			classLogger.error("Error executing audit log fetch: {}", e.getMessage(), e);
		}
		// combine logs and totalCount
		Map<String, Object> responseMap = new HashMap<>();
		responseMap.put("totalCount", totalCount);
		responseMap.put("logs", result);
		String json = GSON.toJson(responseMap);
		return new NounMetadata(json, PixelDataType.JSON_OBJECT, PixelOperationType.LOGGING_DATA);
	}

	/**
	 * Returns start & end DateTime strings in UTC (ISO_INSTANT). If dateRangeType
	 * CUSTOM: parse startDate & endDate (both required). Else: use current datetime
	 * and subtract from it
	 * 
	 * @param mode
	 * @param dateRangeValue
	 * @param startDateCustom
	 * @param endDateCustom
	 * @return
	 */
	private Map<String, SemossDate> determineDateRangeFilter(AuditLogsDateRangeMode mode, int dateRangeValue,
			String startDateCustom, String endDateCustom) {

		if (mode == AuditLogsDateRangeMode.CUSTOM) {
			// validate inputs
			if (startDateCustom == null || endDateCustom == null) {
				throw new IllegalArgumentException("For custom mode, startDate and endDate are required.");
			}
			// Convert start and end UTC dates String to Instant
			Instant startInstant = Instant.parse(startDateCustom);
			Instant endInstant = Instant.parse(endDateCustom);

			if (!startInstant.isBefore(endInstant)) {
				throw new IllegalArgumentException("Start date must be before End date");
			}

			return Map.of(SemossLogUtils.START_DATE, new SemossDate(startInstant, utcZone), SemossLogUtils.END_DATE,
					new SemossDate(endInstant, utcZone));
		}

		// Non-custom modes
		dateRangeValue = (dateRangeValue <= 0) ? 1 : dateRangeValue;

		// 2. Convert to end of day time in system timezone
		ZonedDateTime currentDateTime = ZonedDateTime.now(utcZone);
		ZonedDateTime targetDateTime = null;

		switch (mode) {
		case DAY:
			targetDateTime = currentDateTime.minusDays(dateRangeValue);
			break;
		case WEEK:
			targetDateTime = currentDateTime.minusWeeks(dateRangeValue);
			break;
		case MONTH:
		default:
			targetDateTime = currentDateTime.minusMonths(dateRangeValue);
			break;
		}

		return Map.of(SemossLogUtils.START_DATE, new SemossDate(targetDateTime));
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
		Object val = map.get(key);
		if (map == null || key == null) {
			return "";
		}
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
			classLogger.warn("Invalid number '{}', using default {}", val, defaultValue);
			return defaultValue;
		}
	}
}
