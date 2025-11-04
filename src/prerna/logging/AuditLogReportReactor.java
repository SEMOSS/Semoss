package prerna.logging;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.logging.AuditLogsDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AuditLogReportReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AuditLogReportReactor.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	public AuditLogReportReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Map<String, Object> map = getMap();
		String userId = getString(map, SemossLogUtils.USER_ID);
		String engineId = getString(map, SemossLogUtils.ENGINE_ID);
		String projectId = getString(map, SemossLogUtils.PROJECT_ID);
		String roomId = getString(map, SemossLogUtils.ROOM_ID);
		String sessionId = getString(map, SemossLogUtils.SESSION_ID);
		String dateTime = getString(map, SemossLogUtils.DATE_TIME);

		// Handle pagination safely (default limit = 20, offset = 0)
		String limitStr = getString(map, ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = getString(map, ReactorKeysEnum.OFFSET.getKey());

		int limit = parseOrDefault(limitStr, 20);
		int offset = parseOrDefault(offsetStr, 0);

		int safeLimit = Math.min(limit, 20);

		List<LogActivityDto> result = Collections.emptyList();
		try {
			result = AuditLogsDbUtils.getAuditLogsTimeLineDatas(userId, projectId, engineId, dateTime, roomId,
					sessionId, safeLimit, offset);
		} catch (SQLException e) {
			classLogger.error("Error executing audit log fetch: {}", e.getMessage(), e);
		}

		String json = GSON.toJson(result);
		return new NounMetadata(json, PixelDataType.JSON_OBJECT, PixelOperationType.LOGGING_DATA);
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
	private int parseOrDefault(String val, int defaultValue) {
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
