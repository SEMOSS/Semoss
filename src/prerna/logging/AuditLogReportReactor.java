package prerna.logging;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AuditLogReportReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AuditLogReportReactor.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	// system default zone is assumed to be the user's timezone.
	private final ZoneId utcZone = ZoneId.of("UTC");

	public AuditLogReportReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1,0,0 };
	}

	@Override
	public NounMetadata execute() {
		if (!Utility.isAuditLogsDatabaseEnabled()) {
			throw new IllegalArgumentException("Audit logs have not been enabled on this instance");
		}

		organizeKeys();

		Map<String, Object> map = getMap();
		Integer userPermissionLvl = null;
		User user = this.insight.getUser();
		
		String projectId = getString(map, SemossLogUtils.PROJECT_ID);
		String engineId = getString(map, SemossLogUtils.ENGINE_ID);		
		String roomId = getString(map, SemossLogUtils.ROOM_ID);
		String sessionId = getString(map, SemossLogUtils.SESSION_ID);

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		
		if(projectId !=null && !projectId.equals("")) {
			 userPermissionLvl = SecurityProjectUtils.getUserProjectPermission(user.getPrimaryLoginToken().getId(), projectId);
		}else if(engineId != null && !engineId.equals("")){
			 userPermissionLvl = SecurityEngineUtils.getUserEnginePermission(user.getPrimaryLoginToken().getId(), engineId);
		}else {
			if(projectId == null || projectId.trim().isEmpty()) {
				throw  new IllegalArgumentException("Project ID must not be null or empty.");
			}else if(engineId == null || !engineId.trim().isEmpty()) {
				throw  new IllegalArgumentException("Engine ID must not be null or empty.");
			}
			
		}
		//Throw error if no project and engine id
		
		String filterUserId = null;
		if(userPermissionLvl != null && AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// If Author selected a specific user in the filter			
	        filterUserId =  getString(map, SemossLogUtils.FILTER_USER_ID);	        
		}else {
			filterUserId  = user.getPrimaryLoginToken().getId();
		}
		
		// mode: "day"|"week"|"month"|"custom"
		String dateRangeType = getString(map, SemossLogUtils.DATE_RANGE_TYPE);
		// number from textbox (ignored for custom). If null -> default 1
		int dateRangeValue = parseIntWithDefault(getString(map, SemossLogUtils.DATE_RANGE_VALUE), 1);
		// used for mode != custom: end datetime (frontend local) e.g. "2025-11-19
		// 22:23:37".
		// If null, uses current local datetime.
		LocalDate utcLocalEndDate = ZonedDateTime.now(utcZone).toLocalDate();
		
		String endDate = null;

		// used only when mode == custom:
		String startDate = getString(map, SemossLogUtils.START_DATE);
		String endDateCustom = getString(map, SemossLogUtils.END_DATE); // avoid name clash with endDateTime above
		
		String limitStr = getString(map, ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = getString(map, ReactorKeysEnum.OFFSET.getKey());

		
		int limit = parseIntWithDefault(limitStr, -1);
		int offset = parseIntWithDefault(offsetStr, 0);

		
		Map<String, String> dateTimeMap = getLogsBasedOnFilterValues(dateRangeType, dateRangeValue, utcLocalEndDate, startDate,endDateCustom);
		startDate = dateTimeMap.get(SemossLogUtils.START_DATE);
		endDate = dateTimeMap.get(SemossLogUtils.END_DATE);
		List<LogActivityDto> result = Collections.emptyList();
		long totalCount = 0;
		try {
			result = AuditLogsDbUtils.getAuditLogsTimeLineDatas(filterUserId, projectId, engineId, startDate, endDate,roomId, sessionId, limit, offset);
			// Get total record count
			totalCount = AuditLogsDbUtils.getAuditLogsCount(filterUserId, projectId, engineId, startDate, endDate,roomId, sessionId);

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
	
	/*
	 * Returns start & end DateTime strings in UTC (ISO_INSTANT).
	 *
	 * - If mode == CUSTOM: parse startDate & endDateCustom (both required).
	 * - Else: use endDate or current local time if null; subtract count units
	 * (default 1).
	 *
	 * mode: "day" / "week" / "month" / "custom" (case-insensitive)
	 */
	public Map<String, String> getLogsBasedOnFilterValues(String dateRangeType, int dateRangeValue,LocalDate utcLocalEndDate,String startDateCustom, String endDateCustom) {
		DateRangeMode mode = DateRangeMode.from(dateRangeType);

		if (dateRangeType.equals(SemossLogUtils.CUSTOM)) {
			// validate inputs
			if (startDateCustom == null || endDateCustom == null) {
				throw new IllegalArgumentException(
						"For custom mode, startDateTime and endDateTimeCustom are required.");
			}
			
			/*
			 * LocalDate startCustomDate = LocalDate.parse(startDate);
			 * 
			 * ZonedDateTime startOfDayZdt = startCustomDate.atStartOfDay(utcZone);
			 * 
			 * 
			 * Instant startUtc = startOfDayZdt.toInstant();
			 * 
			 * // 1. Parse string - LocalDate LocalDate endCustomDate =
			 * LocalDate.parse(endDateCustom);
			 * 
			 * // 2. Convert to end of day time in system timezone ZonedDateTime endOfDayZdt
			 * = endCustomDate.atTime(23, 59, 59, 999_999_999).atZone(utcZone);
			 * 
			 * Instant endUtc = endOfDayZdt.toInstant();
			 * 
			 * if (endUtc.isBefore(startUtc)) { throw new
			 * IllegalArgumentException("endDate must be after or equal to startDate."); }
			 */
			return Map.of(SemossLogUtils.START_DATE, startDateCustom,SemossLogUtils.END_DATE, endDateCustom);
		}

		// Non-custom modes
		dateRangeValue = (dateRangeValue <= 0) ? 1 : dateRangeValue;
		
		// 2. Convert to end of day time in system timezone
	    ZonedDateTime endOfDayZdt = utcLocalEndDate
	            .atTime(23, 59, 59, 999_999_999)
	            .atZone(utcZone);
		Instant endUtc = endOfDayZdt.toInstant();
		Instant startUtc;
		
		switch (mode) {
		case DAY:
			 // Subtract (days - 1) days because:
		    // For 1 day = same day at 00:00
		    // For 5 days = 5 days back at 00:00
		    LocalDate targetDay = endOfDayZdt.toLocalDate().minusDays(dateRangeValue - 1);
		    // Start of day in the same timezone (00:00:00)
		    ZonedDateTime startOfDayZdt = targetDay.atStartOfDay(utcZone);
			startUtc = startOfDayZdt.toInstant();
			break;
		case WEEK:
			// Subtract (days - 1) days because:
		    // For 1 day = same day at 00:00
		    // For 5 days = 5 days back at 00:00
		    LocalDate targetWeek = endOfDayZdt.toLocalDate().minusWeeks(dateRangeValue);
		    // Start of day in the same timezone (00:00:00)
		    ZonedDateTime startOfWeekZdt = targetWeek.atStartOfDay(utcZone);
			startUtc = startOfWeekZdt.toInstant();
			break;
		case MONTH:
		default:
			 LocalDate targetMonth = endOfDayZdt.toLocalDate().minusMonths(dateRangeValue);
		    // Start of day in the same timezone (00:00:00)
		    ZonedDateTime startOfMonthZdt = targetMonth.atStartOfDay(utcZone);
			startUtc = startOfMonthZdt.toInstant();
			break;
		}

		// ensure start <= end
		if (startUtc.isAfter(endUtc)) {
			// This can happen if frontend provides end in the past and count is negative
			// (we prevented negative),
			// or when months arithmetic produces same instant — keep sanity: swap if
			// necessary
			Instant tmp = startUtc;
			startUtc = endUtc;
			endUtc = tmp;
		}

		return Map.of(SemossLogUtils.START_DATE, startUtc.toString(),SemossLogUtils.END_DATE, endUtc.toString());
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
