package prerna.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xhtmlrenderer.pdf.ITextRenderer;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.date.SemossDate;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExportAuditLogReportReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExportAuditLogReportReactor.class);
	private final ZoneId utcZone = ZoneId.of("UTC");

	public ExportAuditLogReportReactor() {
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

		// validate access to the project/engine/room and resolve which user's logs
		// the caller is allowed to see (non-owners are restricted to their own)
		AuditLogReportSecurityUtils.AuditLogAccess access = AuditLogReportSecurityUtils.authorize(this.insight,
				projectId, engineId, roomId, getString(map, SemossLogUtils.FILTER_USER_ID));
		String filterUserId = access.getFilterUserId();

		// limit and offset are passed as their own top-level reactor keys, not inside
		// the param values map
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
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
		Map<String, Object> searchMap = null;

		if (map.containsKey("search") && map.get("search") instanceof Map) {
			searchMap = (Map<String, Object>) map.get("search");
		}

		List<String> methodNames = getListFromSearch(searchMap, SemossLogUtils.METHOD_NAME);
		List<String> engineTypes = getListFromSearch(searchMap, SemossLogUtils.ENGINE_TYPE);

		String searchTerm = getString(map, "searchTerm");

		List<LogActivityRecord> result = Collections.emptyList();
		try {
			result = AuditLogsDbUtils.getAuditLogsTimeLineData(filterUserId, projectId, engineId, startDate, endDate,
					roomId, sessionId, limit, offset, methodNames, engineTypes, searchTerm);
		} catch (SQLException e) {
			classLogger.error("Error executing audit log fetch: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Error executing audit log fetch: " + e.getMessage());
		}

		// Generate the CSV file
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);

		ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeForUser(user);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-Z");
		String dateFormatted = currentDateTime.format(formatter);
		boolean pdfFormat = false;

		String isPdfFormat = getString(map, "pdfFormat");
		pdfFormat = Boolean.parseBoolean(isPdfFormat);

		String exportExtension = pdfFormat ? ".pdf" : ".csv";
		String exportName = Utility.normalizePath("AuditLogReport_" + dateFormatted) + exportExtension;

		String insightFolder = this.insight.getInsightFolder();
		File f = new File(insightFolder);
		if (!f.exists()) {
			f.mkdirs();
		}
		String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(fileLocation);

		try {
			if (pdfFormat) {
				writePdf(result, fileLocation);
			} else {
				writeCsv(result, fileLocation);
			}
		} catch (Exception e) {
			classLogger.error("Error writing audit log report: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Error writing audit log report: " + e.getMessage());
		}

		this.insight.addExportFile(downloadKey, insightFile);

		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	private void writeCsv(List<LogActivityRecord> records, String filePath) throws IOException {
		File f = new File(filePath);
		if (!f.exists()) {
			f.createNewFile();
		}

		try (FileWriter writer = new FileWriter(f); BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			// Write Headers
			bufferedWriter.write(
					"\"RequestId\",\"StartTime\",\"EndTime\",\"Request\",\"Response\",\"Tokens\",\"Latency\",\"Status\",\"EngineName\",\"EngineType\",\"MethodName\",\"UserName\",\"UserId\",\"SessionId\",\"SpanId\",\"LogTimestamp\"\n");

			// Write Data
			for (LogActivityRecord record : records) {
				StringBuilder builder = new StringBuilder();
				builder.append(escapeCsv(record.requestId())).append(",");
				builder.append(escapeCsv(record.startTime() != null ? record.startTime().toString() : "")).append(",");
				builder.append(escapeCsv(record.endTime() != null ? record.endTime().toString() : "")).append(",");
				builder.append(escapeCsv(record.request())).append(",");
				builder.append(escapeCsv(record.response())).append(",");
				builder.append(record.tokens()).append(",");
				builder.append(record.latency()).append(",");
				builder.append(record.status()).append(",");
				builder.append(escapeCsv(record.engineName())).append(",");
				builder.append(escapeCsv(record.engineType())).append(",");
				builder.append(escapeCsv(record.methodName())).append(",");
				builder.append(escapeCsv(record.userName())).append(",");
				builder.append(escapeCsv(record.userId())).append(",");
				builder.append(escapeCsv(record.sessionId())).append(",");
				builder.append(escapeCsv(record.spanId())).append(",");
				builder.append(escapeCsv(record.logTimestamp() != null ? record.logTimestamp().toString() : ""));
				builder.append("\n");

				bufferedWriter.write(builder.toString());
			}
		}
	}

	private void writePdf(List<LogActivityRecord> records, String filePath) throws Exception {
		StringBuilder htmlBuilder = new StringBuilder();
		htmlBuilder.append("<html><head><style>");
		htmlBuilder.append("body { font-family: Arial, Helvetica, sans-serif; margin: 15px; } ");
		htmlBuilder.append(
				".header { text-align: center; padding-bottom: 20px; border-bottom: 1px solid #ddd; margin-bottom: 20px; } ");
		htmlBuilder.append(".header h1 { margin: 0; font-size: 20px; } ");
		htmlBuilder.append(
				"table { width: 100%; border-collapse: collapse; margin-top: 15px; table-layout: fixed; font-size: 8px; } ");
		htmlBuilder.append(
				"th, td { padding: 4px; text-align: left; border: 1px solid #ddd; word-wrap: break-word; line-height: 1.1; overflow: hidden; } ");
		htmlBuilder.append("thead th { background-color: #4CAF50; color: white; font-weight: bold; } ");
		htmlBuilder.append("tbody tr:nth-child(even) { background-color: #f2f2f2; } ");
		htmlBuilder.append("</style></head><body>");

		htmlBuilder.append("<div class='header'><h1>Audit Log Report</h1></div>");

		htmlBuilder.append("<table>");
		htmlBuilder.append("<thead><tr>");
		htmlBuilder.append(
				"<th>Request ID</th><th>Start Time</th><th>End Time</th><th>Request</th><th>Response</th><th>Tokens</th><th>Latency</th>");
		htmlBuilder.append(
				"<th>Status</th><th>Engine Name</th><th>Engine Type</th><th>Method Name</th><th>User Name</th><th>User ID</th><th>Session ID</th><th>Span ID</th><th>Log Timestamp</th>");
		htmlBuilder.append("</tr></thead><tbody>");

		for (LogActivityRecord record : records) {
			htmlBuilder.append("<tr>");
			htmlBuilder.append("<td>").append(escapeHtml(record.requestId())).append("</td>");
			htmlBuilder.append("<td>")
					.append(escapeHtml(record.startTime() != null ? record.startTime().toString() : ""))
					.append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.endTime() != null ? record.endTime().toString() : ""))
					.append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.request())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.response())).append("</td>");
			htmlBuilder.append("<td>").append(record.tokens()).append("</td>");
			htmlBuilder.append("<td>").append(record.latency()).append("</td>");
			htmlBuilder.append("<td>").append(record.status()).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.engineName())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.engineType())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.methodName())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.userName())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.userId())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.sessionId())).append("</td>");
			htmlBuilder.append("<td>").append(escapeHtml(record.spanId())).append("</td>");
			htmlBuilder.append("<td>")
					.append(escapeHtml(record.logTimestamp() != null ? record.logTimestamp().toString() : ""))
					.append("</td>");
			htmlBuilder.append("</tr>");
		}
		htmlBuilder.append("</tbody></table></body></html>");

		String insightFolder = this.insight.getInsightFolder();
		String tempXhtmlPath = insightFolder + DIR_SEPARATOR + UUID.randomUUID().toString() + ".html";
		File tempXhtml = new File(tempXhtmlPath);

		try {
			FileUtils.writeStringToFile(tempXhtml, htmlBuilder.toString(), StandardCharsets.UTF_8);

			try (FileOutputStream fos = new FileOutputStream(filePath)) {
				DocumentBuilderFactory factory = Utility.getDocumentBuilderFactory();
				DocumentBuilder builder = factory.newDocumentBuilder();
				Document document = builder.parse(tempXhtml);

				ITextRenderer renderer = new ITextRenderer();
				renderer.setDocument(document);
				renderer.layout();
				renderer.createPDF(fos);
			}
		} finally {
			if (tempXhtml.exists()) {
				FileUtils.forceDelete(tempXhtml);
			}
		}
	}

	private String escapeHtml(String value) {
		if (value == null) {
			return "";
		}
		return StringEscapeUtils.escapeHtml4(value);
	}

	private String escapeCsv(String value) {
		if (value == null) {
			return "";
		}
		return "\"" + value.replace("\"", "\"\"") + "\"";
	}

	private Map<String, SemossDate> determineDateRangeFilter(AuditLogsDateRangeMode mode, int dateRangeValue,
			String startDateCustom, String endDateCustom) {

		if (mode == AuditLogsDateRangeMode.CUSTOM) {
			if (startDateCustom == null || endDateCustom == null) {
				throw new IllegalArgumentException("For custom mode, startDate and endDate are required.");
			}
			Instant startInstant = Instant.parse(startDateCustom);
			Instant endInstant = Instant.parse(endDateCustom);

			if (!startInstant.isBefore(endInstant)) {
				throw new IllegalArgumentException("Start date must be before End date");
			}

			return Map.of(SemossLogUtils.START_DATE, new SemossDate(startInstant, utcZone), SemossLogUtils.END_DATE,
					new SemossDate(endInstant, utcZone));
		}

		dateRangeValue = (dateRangeValue <= 0) ? 1 : dateRangeValue;
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

	private String getString(Map<String, Object> map, String key) {
		Object val = map.get(key);
		if (map == null || key == null) {
			return "";
		}
		return (val != null && !StringUtils.isBlank(val.toString())) ? val.toString().trim() : "";
	}

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

	private List<String> getListFromSearch(Map<String, Object> searchMap, String key) {
		if (searchMap == null || !searchMap.containsKey(key)) {
			return Collections.emptyList();
		}

		Object val = searchMap.get(key);

		if (val instanceof List<?>) {
			return ((List<?>) val).stream().filter(Objects::nonNull).map(Object::toString).map(String::trim)
					.filter(s -> !s.isEmpty()).collect(Collectors.toList());
		}

		return Collections.emptyList();
	}
}
