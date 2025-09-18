package prerna.logging;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.logging.AuditLogsDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AuditLogReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AuditLogReactor.class);

	//private String loggerMicroserviceUrl = null;

	private final ObjectMapper objectMapper = new ObjectMapper();

	public AuditLogReactor() {
		/*
		 * this.keysToGet = new String[]{ ReactorKeysEnum.AGENT.getKey(),
		 * ReactorKeysEnum.ROOM.getKey(), ReactorKeysEnum.DATE_TIME_FIELD.getKey() };
		 */
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1 };
		//this.loggerMicroserviceUrl = Utility.getDIHelperProperty(Settings.LOGGER_MICROSERVICE_URL);
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Map<String, Object> map = getMap();
		String userId = map.get(SemossLogUtils.USER_ID) != null && !map.get(SemossLogUtils.USER_ID).equals("")
				? (String) map.get(SemossLogUtils.USER_ID)
				: "";
		String engineId = map.get(SemossLogUtils.ENGINE_ID) != null && !map.get(SemossLogUtils.ENGINE_ID).equals("")
				? (String) map.get(SemossLogUtils.ENGINE_ID)
				: "";
		String projectId = map.get(SemossLogUtils.PROJECT_ID) != null && !map.get(SemossLogUtils.PROJECT_ID).equals("")
				? (String) map.get(SemossLogUtils.PROJECT_ID)
				: "";
		String roomId = map.get(SemossLogUtils.ROOM_ID) != null && !map.get(SemossLogUtils.ROOM_ID).equals("")
				? (String) map.get(SemossLogUtils.ROOM_ID)
				: "";
		String sessionId = map.get(SemossLogUtils.SESSION_ID) != null && !map.get(SemossLogUtils.SESSION_ID).equals("")
				? (String) map.get(SemossLogUtils.SESSION_ID)
				: "";
		String dateTime = map.get(SemossLogUtils.DATE_TIME) != null && !map.get(SemossLogUtils.DATE_TIME).equals("")
				? (String) map.get(SemossLogUtils.DATE_TIME)
				: "";

		List<LogActivityDto> result = null;
		String json = null;
		try {
			result = AuditLogsDbUtils.getAuditLogsTimeLineDatas(userId, projectId, engineId, dateTime, roomId,
					sessionId);
			try {
				json = objectMapper.writeValueAsString(result);
			} catch (JsonProcessingException e) {
				classLogger.error(e.getMessage());
			}
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
		}
		return new NounMetadata(json, PixelDataType.JSON_OBJECT, PixelOperationType.LOGGING_DATA);
	}

	/**
	 *
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
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
}
