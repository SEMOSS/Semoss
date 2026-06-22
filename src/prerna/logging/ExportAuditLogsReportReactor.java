package prerna.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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

public class ExportAuditLogsReportReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExportAuditLogsReportReactor.class);

	public ExportAuditLogsReportReactor() {
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

		// the "null" sentinel ("only logs with no room") is not a real room, so it is
		// excluded from room validation and from bounding the query
		boolean nullRoomFilter = AuditLogsDbUtils.NULL_ROOM_ID.equalsIgnoreCase(roomId);
		String realRoomId = nullRoomFilter ? "" : roomId;

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}

		// validate access to the project/engine/room and resolve which user's logs
		// the caller is allowed to see (non-owners are restricted to their own)
		AuditLogReportSecurityUtils.AuditLogAccess access = AuditLogReportSecurityUtils.authorize(this.insight,
				projectId, engineId, realRoomId, getString(map, SemossLogUtils.FILTER_USER_ID));
		String filterUserId = access.getFilterUserId();

		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		int limit = parseIntWithDefault(limitStr, -1);
		int offset = parseIntWithDefault(offsetStr, 0);

		// dateRangeType: "day"|"week"|"month"|"custom". When the query is not bounded
		// by a roomId and no date range is supplied, this defaults to a single day so
		// we never scan the full audit logs table.
		String dateRangeType = getString(map, SemossLogUtils.DATE_RANGE_TYPE);
		// number value for dateRangeType (ignored for custom). If null -> default 1
		int dateRangeValue = parseIntWithDefault(getString(map, SemossLogUtils.DATE_RANGE_VALUE), 1);
		// used only when dateRangeType is custom
		String startDateCustom = getString(map, SemossLogUtils.START_DATE);
		String endDateCustom = getString(map, SemossLogUtils.END_DATE);

		boolean queryIsBounded = realRoomId != null && !realRoomId.isBlank();
		Map<String, SemossDate> dateTimeMap = AuditLogsDateRangeMode.resolveDateRange(dateRangeType, dateRangeValue,
				startDateCustom, endDateCustom, queryIsBounded);
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
				builder.append(escapeCsv(record.startTime())).append(",");
				builder.append(escapeCsv(record.endTime())).append(",");
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
				builder.append(escapeCsv(record.logTimestamp()));
				builder.append("\n");

				bufferedWriter.write(builder.toString());
			}
		}
	}

	private void writePdf(List<LogActivityRecord> records, String filePath) throws Exception {
		StringBuilder htmlBuilder = new StringBuilder();
		htmlBuilder.append("<html><head><style>");
		htmlBuilder.append(
				"body { font-family: Arial, Helvetica, sans-serif; margin: 20px; font-size: 11px; color: #222; } ");
		htmlBuilder.append(
				".header { text-align: center; padding-bottom: 12px; border-bottom: 2px solid #4CAF50; margin-bottom: 16px; } ");
		htmlBuilder.append(".header h1 { margin: 0; font-size: 20px; } ");
		htmlBuilder.append(".header .sub { color: #666; font-size: 10px; margin-top: 4px; } ");
		htmlBuilder.append(".record { border: 1px solid #ddd; margin-bottom: 16px; padding: 10px 12px; } ");
		htmlBuilder.append(
				".record .title { font-size: 13px; font-weight: bold; border-bottom: 1px solid #eee; padding-bottom: 6px; margin-bottom: 8px; } ");
		htmlBuilder.append(".status-ok { color: #2e7d32; } ");
		htmlBuilder.append(".status-fail { color: #c62828; } ");
		htmlBuilder.append(".meta { width: 100%; border-collapse: collapse; margin-bottom: 4px; } ");
		htmlBuilder
				.append(".meta td { padding: 2px 6px; vertical-align: top; font-size: 10px; word-wrap: break-word; } ");
		htmlBuilder.append(".meta td.k { color: #666; width: 90px; white-space: nowrap; } ");
		htmlBuilder.append(".io-label { font-weight: bold; font-size: 10px; color: #444; margin-top: 8px; } ");
		htmlBuilder.append(
				".io-block { white-space: pre-wrap; word-wrap: break-word; background: #f7f7f7; border: 1px solid #eee; padding: 6px; font-family: 'Courier New', monospace; font-size: 9px; margin-top: 2px; } ");
		htmlBuilder.append("</style></head><body>");

		htmlBuilder.append("<div class='header'><h1>Audit Log Report</h1>");
		htmlBuilder.append("<div class='sub'>").append(records.size()).append(" record(s)</div></div>");

		for (LogActivityRecord record : records) {
			htmlBuilder.append("<div class='record'>");

			// title line: method name + success/failure badge
			String statusClass = record.status() ? "status-ok" : "status-fail";
			String statusText = record.status() ? "Success" : "Failed";
			htmlBuilder.append("<div class='title'>")
					.append(escapeXml(defaultIfEmpty(record.methodName(), "(method name n/a)")))
					.append(" <span class='").append(statusClass).append("'>[").append(statusText).append("]</span>")
					.append("</div>");

			// scalar fields as a compact two-pair-per-row key/value grid
			htmlBuilder.append("<table class='meta'>");
			appendMetaRow(htmlBuilder, "Request ID", record.requestId(), "Log Timestamp", record.logTimestamp());
			appendMetaRow(htmlBuilder, "Engine Name", record.engineName(), "Engine Type", record.engineType());
			appendMetaRow(htmlBuilder, "User Name", record.userName(), "User ID", record.userId());
			appendMetaRow(htmlBuilder, "Start Time", record.startTime(), "End Time", record.endTime());
			appendMetaRow(htmlBuilder, "Latency (s)", String.valueOf(record.latency()), "Tokens",
					String.valueOf(record.tokens()));
			appendMetaRow(htmlBuilder, "Session ID", record.sessionId(), "Span ID", record.spanId());
			htmlBuilder.append("</table>");

			// request / response as full-width, wrapped, newline-preserving blocks
			htmlBuilder.append("<div class='io-label'>Request</div>");
			htmlBuilder.append("<div class='io-block'>").append(escapeXml(record.request())).append("</div>");
			htmlBuilder.append("<div class='io-label'>Response</div>");
			htmlBuilder.append("<div class='io-block'>").append(escapeXml(record.response())).append("</div>");

			htmlBuilder.append("</div>");
		}

		htmlBuilder.append("</body></html>");

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

	/**
	 * Append a metadata row holding two key/value pairs to the record card grid.
	 */
	private void appendMetaRow(StringBuilder sb, String key1, String value1, String key2, String value2) {
		sb.append("<tr>");
		sb.append("<td class='k'>").append(escapeXml(key1)).append("</td><td>").append(escapeXml(value1))
				.append("</td>");
		sb.append("<td class='k'>").append(escapeXml(key2)).append("</td><td>").append(escapeXml(value2))
				.append("</td>");
		sb.append("</tr>");
	}

	private String defaultIfEmpty(String value, String fallback) {
		return (value == null || value.trim().isEmpty()) ? fallback : value;
	}

	/**
	 * Escape for XML (not HTML): the PDF html is parsed as XML by the renderer, so
	 * we may only emit the five built-in XML entities. HTML-named entities like
	 * {@code &mdash;}/{@code &nbsp;} (which escapeHtml4 produces for chars such as
	 * an em-dash) are undeclared in XML and would fail parsing - escapeXml10 leaves
	 * those characters as literal UTF-8 instead.
	 */
	private String escapeXml(String value) {
		if (value == null) {
			return "";
		}
		return StringEscapeUtils.escapeXml10(value);
	}

	private String escapeCsv(String value) {
		if (value == null) {
			return "";
		}
		return "\"" + value.replace("\"", "\"\"") + "\"";
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
