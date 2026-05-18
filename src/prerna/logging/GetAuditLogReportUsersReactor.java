package prerna.logging;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetAuditLogReportUsersReactor extends AbstractReactor {

	public GetAuditLogReportUsersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);

		// get engine
		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// ignore
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
		}

		if (engine == null) {
			throw new NullPointerException("Unknown engine or project with id " + engineId);
		}

		IEngine.CATALOG_TYPE engineType = engine.getCatalogType();
		User user = this.insight.getUser();
		// check security
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		List<Map<String, Object>> existingUsers = new ArrayList<>();
		List<Map<String, Object>> auditLogsReportUsers = new ArrayList<>();
		if (engineType == CATALOG_TYPE.PROJECT) {
			try {
				existingUsers = SecurityProjectUtils.getProjectUsers(user, engineId, null, null, 0, 0);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			auditLogsReportUsers = AuditLogsDbUtils.getAuditLogsReportUsers(engineId, null);
		} else {
			try {
				existingUsers = SecurityEngineUtils.getEngineUsers(user, engineId, null, null, 0, 0);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			auditLogsReportUsers = AuditLogsDbUtils.getAuditLogsReportUsers(null, engineId);
		}

		// only userId, userType & userName from existingUsers
		List<Map<String, Object>> filteredExistingUsers = existingUsers.stream().map(member -> {
			Map<String, Object> filtered = new LinkedHashMap<>();
			filtered.put("id", member.get("id"));
			filtered.put("type", member.get("type"));
			filtered.put("name", member.get("name"));
			return filtered;
		}).collect(Collectors.toList());
		// merge users' lists
		Map<String, Map<String, Object>> mergedMap = new LinkedHashMap<>();
		for (Map<String, Object> u : filteredExistingUsers) {
			String key = u.get("id") + "_" + u.get("type");
			mergedMap.putIfAbsent(key, u);
		}

		for (Map<String, Object> u : auditLogsReportUsers) {
			String key = u.get("id") + "_" + u.get("type");
			mergedMap.putIfAbsent(key, u);
		}

		List<Map<String, Object>> mergedUsers = new ArrayList<>(mergedMap.values());

		return new NounMetadata(mergedUsers, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.LOGGING_DATA);
	}

}
