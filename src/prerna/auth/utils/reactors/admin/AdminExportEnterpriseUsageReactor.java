/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils.reactors.admin;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.openpdf.text.pdf.BaseFont;
import org.xhtmlrenderer.pdf.ITextRenderer;

import com.google.gson.JsonObject;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.EnterpriseUsageUtils;
import prerna.auth.utils.EnterpriseUsageUtils.Source;
import prerna.auth.utils.EnterpriseUsageUtils.View;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.logging.SemossLogUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.AdminThemeUtils;

/**
 * Generates scoped enterprise reports on the server and audits each export
 * attempt.
 */
public class AdminExportEnterpriseUsageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminExportEnterpriseUsageReactor.class);

	private static final String NOTES = "Requests Count INPUT Rows; Token Totals Sum Recorded INPUT And RESPONSE Token Counts. "
			+ "Raw Latency Is In Milliseconds. Cache And Thinking Tokens Are Provider-Reported Detail, Not Additional Total Tokens. "
			+ "Activity Events Are Separate From Model Requests. Missing Values Are Unavailable. "
			+ "Coverage Depends On Enabled Logging And Retention. No Message Bodies Or Billed Spend Are Included. "
			+ "When Period Lengths Differ, Volume Comparisons Use Daily Averages; Distinct Counts Are Not Normalized.";

	public AdminExportEnterpriseUsageReactor() {
		this.keysToGet = new String[] { "source", "view", "format", "startDate", "endDate", "user", "app", "model",
				"engine", "dimension", "comparisonStartDate", "comparisonEndDate" };
	}

	@Override
	public NounMetadata execute() {
		Instant started = Instant.now();
		Path file = null;
		boolean succeeded = false;
		int rowCount = 0;
		try {
			if (SecurityAdminUtils.getInstance(insight.getUser()) == null) {
				throw new IllegalArgumentException("User Must Be An Admin To Export Enterprise Usage");
			}
			if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(insight.getUser())) {
				throw new IllegalArgumentException("User Does Not Have Export Permission");
			}
			organizeKeys();
			// Model engines and other engine types share one filter and audit field.
			keyValue.putIfAbsent("engine", keyValue.getOrDefault("model", ""));
			keyValue.remove("model");
			String format = keyValue.getOrDefault("format", "csv").toLowerCase(Locale.ROOT);
			String view = keyValue.getOrDefault("view", "overview").toLowerCase(Locale.ROOT);
			if (!Set.of("csv", "pdf").contains(format) || !Set.of("overview", "ranking", "logs").contains(view)
					|| (format.equals("pdf") && !view.equals("overview"))) {
				throw new IllegalArgumentException("Unsupported Export Format Or View");
			}
			Source source = EnterpriseUsageUtils.option(Source.class, keyValue.getOrDefault("source", "model"));
			keyValue.put("format", format);
			keyValue.put("view", view);
			keyValue.put("source", source.name().toLowerCase(Locale.ROOT));
			if (view.equals("overview") && !keyValue.containsKey("comparisonStartDate")
					&& !keyValue.containsKey("comparisonEndDate")) {
				LocalDate start = LocalDate.parse(keyValue.get("startDate"));
				long days = ChronoUnit.DAYS.between(start, LocalDate.parse(keyValue.get("endDate"))) + 1;
				keyValue.put("comparisonStartDate", start.minusDays(days).toString());
				keyValue.put("comparisonEndDate", start.minusDays(1).toString());
			}
			Map<String, List<Map<String, Object>>> sections = new LinkedHashMap<>();
			if (view.equals("overview")) {
				overview(source, sections, format.equals("pdf"));
			} else if (view.equals("ranking")) {
				if (source != Source.MODEL) {
					throw new IllegalArgumentException("Rankings Require Model Usage");
				}
				EnterpriseUsageUtils.option(EnterpriseUsageUtils.Dimension.class, keyValue.get("dimension"));
				sections.put("Top 20 " + title(keyValue.get("dimension")) + " Consumers",
						query(source, View.RANKING, false, 20));
			} else {
				sections.put(title(source.name()) + " Log Metadata (Newest 5,000 Maximum)",
						query(source, View.LOGS, false, EnterpriseUsageUtils.MAX_LIMIT));
			}
			rowCount = sections.values().stream().mapToInt(List::size).sum();
			String downloadKey = UUID.randomUUID().toString();
			Path folder = Path.of(insight.getInsightFolder());
			Files.createDirectories(folder);
			file = folder.resolve("Enterprise-Usage-" + downloadKey + "." + format);
			Map<String, Object> scope = scope();
			scope.put("Brand", activeBrandName());
			scope.put("Generated At", started.toString());
			var actor = insight.getUser().getPrimaryLoginToken();
			scope.put("Generated By", actor != null && actor.getName() != null ? actor.getName() : "Administrator");
			if (format.equals("csv")) {
				writeCsv(file, sections, scope);
			} else {
				writePdf(file, sections, scope);
			}
			InsightFile exported = new InsightFile();
			exported.setFileKey(downloadKey);
			exported.setFilePath(file.toString());
			exported.setDeleteOnInsightClose(true);
			insight.addExportFile(downloadKey, exported);
			succeeded = true;
			return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} catch (Exception e) {
			if (file != null) {
				try {
					Files.deleteIfExists(file);
				} catch (Exception cleanup) {
					classLogger.error("Unable to remove incomplete enterprise usage export", cleanup);
				}
			}
			classLogger.error("Unable to export enterprise usage", e);
			if (e instanceof IllegalArgumentException invalid) {
				throw invalid;
			}
			throw new IllegalArgumentException("Unable To Generate Enterprise Usage Export");
		} finally {
			audit(started, succeeded, rowCount);
		}
	}

	/**
	 * Reads server-owned views with fixed export bounds; the client supplies only
	 * filters.
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> query(Source source, View view, boolean previous, int limit) {
		String start = keyValue.get("startDate");
		String end = keyValue.get("endDate");
		if (previous) {
			start = keyValue.get("comparisonStartDate");
			end = keyValue.get("comparisonEndDate");
		}
		return (List<Map<String, Object>>) EnterpriseUsageUtils.report(source, view, start, end, keyValue.get("user"),
				keyValue.get("app"), keyValue.get("engine"), keyValue.get("dimension"), limit, 0).get("rows");
	}

	/** Exports the selected dashboard source with its explicit benchmark. */
	private void overview(Source source, Map<String, List<Map<String, Object>>> sections, boolean includeTrends) {
		String label = source == Source.MODEL ? "Token Consumption" : "Platform Activity";
		Map<String, Object> current = query(source, View.SUMMARY, false, 1).get(0);
		Map<String, Object> previous = query(source, View.SUMMARY, true, 1).get(0);
		long currentDays = ChronoUnit.DAYS.between(LocalDate.parse(keyValue.get("startDate")),
				LocalDate.parse(keyValue.get("endDate"))) + 1;
		long benchmarkDays = ChronoUnit.DAYS.between(LocalDate.parse(keyValue.get("comparisonStartDate")),
				LocalDate.parse(keyValue.get("comparisonEndDate"))) + 1;
		Set<String> additive = Set.of("REQUESTS", "TOKENS", "INPUT_TOKENS", "OUTPUT_TOKENS", "CACHE_READ_TOKENS",
				"CACHE_CREATION_TOKENS", "THINKING_TOKENS", "EVENTS", "FAILED", "SUCCEEDED");
		List<Map<String, Object>> metrics = new ArrayList<>();
		current.forEach((key, value) -> {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("Metric", title(key));
			row.put("Current", value);
			row.put("Benchmark", previous.get(key));
			if (currentDays != benchmarkDays) {
				row.put("Current Days", currentDays);
				row.put("Benchmark Days", benchmarkDays);
				row.put("Current Per Day",
						additive.contains(key) && value instanceof Number n ? n.doubleValue() / currentDays : null);
				row.put("Benchmark Per Day",
						additive.contains(key) && previous.get(key) instanceof Number n
								? n.doubleValue() / benchmarkDays
								: null);
			}
			metrics.add(row);
		});
		if (source == Source.MODEL) {
			metrics.add(metric("P95 Latency (ms)", query(source, View.LATENCY, false, 1).get(0).get("P95_MS")));
			Map<String, Object> feedback = query(source, View.FEEDBACK, false, 1).get(0);
			metrics.add(metric("Rated Responses", feedback.get("RATINGS")));
			metrics.add(metric("Positive Feedback (%)", rate(feedback, "POSITIVE", "RATINGS")));
			metrics.add(metric("Token Coverage (%)", rate(current, "TOKEN_ROWS", "MESSAGE_ROWS")));
		} else {
			metrics.add(metric("Activity Success Rate (%)", rate(current, "SUCCEEDED", "KNOWN_OUTCOMES")));
		}
		sections.put(label, metrics);
		if (includeTrends) {
			List<String> keys = source == Source.MODEL
					? List.of("DAY", "REQUESTS", "USERS", "INPUT_TOKENS", "OUTPUT_TOKENS", "TOKENS", "LATENCY_MS")
					: List.of("DAY", "EVENTS", "USERS", "FAILED", "KNOWN_OUTCOMES");
			for (boolean benchmark : List.of(false, true)) {
				List<Map<String, Object>> daily = new ArrayList<>();
				for (Map<String, Object> row : query(source, View.TREND, benchmark, 366)) {
					Map<String, Object> selected = new LinkedHashMap<>();
					keys.forEach(key -> selected.put(title(key), row.get(key)));
					daily.add(selected);
				}
				sections.put("Daily " + (benchmark ? "Benchmark " : "") + label, daily);
			}
		}
	}

	private static Map<String, Object> metric(String label, Object value) {
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("Metric", label);
		row.put("Current", value);
		row.put("Benchmark", null);
		return row;
	}

	private static Double rate(Map<String, Object> row, String numerator, String denominator) {
		return row.get(numerator) instanceof Number n && row.get(denominator) instanceof Number d && d.doubleValue() > 0
				? n.doubleValue() / d.doubleValue() * 100
				: null;
	}

	private Map<String, Object> scope() {
		Map<String, Object> scope = new LinkedHashMap<>();
		for (String key : keysToGet) {
			if (key.equals("model")) {
				continue;
			}
			scope.put(title(key), keyValue.getOrDefault(key, ""));
		}
		return scope;
	}

	/**
	 * Resolves report branding from the active, server-owned theme. The nested
	 * {@code brand.name} takes precedence over the legacy top-level {@code name}.
	 * Missing or invalid branding uses the platform default without blocking an
	 * otherwise valid export. The returned name remains plain Unicode text and is
	 * escaped at the output boundary, never interpolated into CSS or markup.
	 *
	 * @return the configured display name, or {@code SEMOSS} when none is available
	 */
	private static String activeBrandName() {
		try {
			if (AdminThemeUtils.getActiveAdminTheme() instanceof Map<?, ?> theme) {
				Object value = theme.get("THEME_MAP");
				if (!(value instanceof String)) {
					value = theme.get("ADMIN_THEME__THEME_MAP");
				}
				if (value instanceof String json && !json.isBlank()) {
					JsonObject config = GSON.fromJson(json, JsonObject.class);
					if (config != null) {
						var brand = config.get("brand");
						for (JsonObject candidate : brand != null && brand.isJsonObject()
								? List.of(brand.getAsJsonObject(), config)
								: List.of(config)) {
							var name = candidate.get("name");
							if (name != null && name.isJsonPrimitive() && name.getAsJsonPrimitive().isString()
									&& !name.getAsString().isBlank()) {
								return name.getAsString().strip();
							}
						}
					}
				}
			}
		} catch (RuntimeException e) {
			classLogger.warn("Unable to resolve enterprise usage report branding; using the platform default", e);
		}
		return "SEMOSS";
	}

	/**
	 * Emits metadata to the existing audit appender, preserving the authenticated
	 * actor.
	 */
	private void audit(Instant started, boolean succeeded, int rowCount) {
		try {
			Map<String, Object> event = new LinkedHashMap<>();
			var token = insight.getUser().getPrimaryLoginToken();
			if (token != null) {
				event.put(SemossLogUtils.USER_ID, token.getId());
				event.put(SemossLogUtils.USER_NAME, token.getName());
				event.put(SemossLogUtils.USER_TYPE, String.valueOf(token.getProvider()));
			}
			event.put(SemossLogUtils.INSIGHT_ID, insight.getInsightId());
			event.put(SemossLogUtils.METHOD_NAME, "AdminExportEnterpriseUsage");
			event.put(SemossLogUtils.IS_SUCCESS, succeeded);
			event.put(SemossLogUtils.REQUEST_START_TIME, started.atZone(ZoneOffset.UTC));
			event.put(SemossLogUtils.RESPONSE_END_TIME, Instant.now().atZone(ZoneOffset.UTC));
			event.put(SemossLogUtils.REQUEST, GSON.toJson(scope()));
			event.put(SemossLogUtils.RESPONSE,
					GSON.toJson(Map.of("outcome", succeeded ? "generated" : "failed", "rows", rowCount)));
			SemossLogUtils.getEngineLevelLogger().info(event);
		} catch (Exception e) {
			classLogger.error("Unable to log enterprise usage export attempt", e);
		}
	}

	/**
	 * Writes quoted UTF-8 CSV and neutralizes spreadsheet formulas in textual
	 * cells.
	 */
	private static void writeCsv(Path file, Map<String, List<Map<String, Object>>> sections, Map<String, Object> scope)
			throws Exception {
		Set<String> columns = new LinkedHashSet<>();
		sections.values().forEach(rows -> rows.forEach(row -> columns.addAll(row.keySet())));
		columns.remove("ROW_NUM");
		List<String> headers = new ArrayList<>(List.of("Section"));
		columns.forEach(key -> headers.add(title(key)));
		headers.addAll(scope.keySet());
		headers.add("Notes");
		try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
			writer.write('\ufeff');
			csvRow(writer, headers);
			for (var section : sections.entrySet()) {
				List<Map<String, Object>> rows = section.getValue().isEmpty() ? List.of(Map.of()) : section.getValue();
				for (Map<String, Object> row : rows) {
					List<Object> cells = new ArrayList<>(List.of(section.getKey()));
					columns.forEach(key -> cells.add(row.get(key)));
					cells.addAll(scope.values());
					cells.add(NOTES);
					csvRow(writer, cells);
				}
			}
		}
	}

	private static void csvRow(BufferedWriter writer, List<?> cells) throws Exception {
		for (int i = 0; i < cells.size(); i++) {
			if (i > 0) {
				writer.write(',');
			}
			Object value = cells.get(i);
			String text = value == null ? "" : value.toString();
			if (value instanceof String && text.stripLeading().matches("(?s)^[=+@-].*")) {
				text = "'" + text;
			}
			writer.write('"');
			writer.write(text.replace("\"", "\"\""));
			writer.write('"');
		}
		writer.write("\r\n");
	}

	/**
	 * Renders a branded, paginated report from server-owned data only. All dynamic
	 * text is escaped; chart images are generated in memory with the existing
	 * JFreeChart dependency. No URLs, markup or resource paths come from the
	 * client.
	 */
	private static void writePdf(Path file, Map<String, List<Map<String, Object>>> sections, Map<String, Object> scope)
			throws Exception {
		String brand = xml(scope.get("Brand"));
		boolean model = "model".equals(scope.get("Source"));
		String label = model ? "Token Consumption" : "Platform Activity";
		String reportTitle = label + " Report";
		List<Map<String, Object>> metrics = new ArrayList<>(sections.getOrDefault(label, List.of()));
		List<String> metricOrder = model
				? List.of("Requests", "Tokens", "Input Tokens", "Output Tokens", "Users", "Apps", "Models", "Rooms",
						"Average Latency (ms)", "P95 Latency (ms)", "Rated Responses", "Positive Feedback (%)",
						"Token Coverage (%)", "Message Rows", "Token Rows", "Cache Read Tokens",
						"Cache Creation Tokens", "Thinking Tokens")
				: List.of("Events", "Users", "Succeeded", "Failed", "Known Outcomes", "Activity Success Rate (%)");
		metrics.sort(Comparator.<Map<String, Object>>comparingInt(row -> {
			int index = metricOrder.indexOf(row.get("Metric"));
			return index < 0 ? Integer.MAX_VALUE : index;
		}).thenComparing(row -> String.valueOf(row.get("Metric"))));
		Map<String, Map<String, Object>> byMetric = new LinkedHashMap<>();
		metrics.forEach(row -> byMetric.put(String.valueOf(row.get("Metric")), row));
		long currentDays = ChronoUnit.DAYS.between(LocalDate.parse(scope.get("Start Date").toString()),
				LocalDate.parse(scope.get("End Date").toString())) + 1;
		long benchmarkDays = ChronoUnit.DAYS.between(LocalDate.parse(scope.get("Comparison Start Date").toString()),
				LocalDate.parse(scope.get("Comparison End Date").toString())) + 1;
		StringBuilder html = new StringBuilder("""
				<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8" /><title>
				""").append(xml(reportTitle))
				.append("""
						</title><style>
						@page { size: A4; margin: 22mm 17mm 19mm;
						  @top-left { content: element(reportHeader); }
						  @bottom-left { content: element(reportFooter); }
						  @bottom-right { content: "Page " counter(page) " Of " counter(pages); font: 8pt 'Liberation Sans'; color: #64748b; }
						}
						body { font-family: 'Liberation Sans', Helvetica, Arial, sans-serif; font-size: 9pt; line-height: 1.45; color: #172554; margin: 0; }
						.report-header, .report-footer { font-size: 8pt; color: #64748b; }
						.report-header { position: running(reportHeader); }
						.report-footer { position: running(reportFooter); }
						.wordmark { font-weight: bold; font-size: 23pt; letter-spacing: 2pt; color: #1d4ed8; word-wrap: break-word; }
						.eyebrow { color: #64748b; font-size: 9pt; letter-spacing: 0.5pt; margin: 0 0 4pt; }
						h1 { font-size: 26pt; line-height: 1.15; margin: 10pt 0 7pt; color: #0f172a; }
						h2 { font-size: 15pt; margin: 18pt 0 7pt; line-height: 1.2; color: #0f172a; page-break-after: avoid; }
						h3 { font-size: 10pt; margin: 10pt 0 4pt; color: #334155; page-break-after: avoid; }
						p { margin: 0 0 7pt; }
						.muted { color: #64748b; font-size: 8pt; }
						.rule { border-top: 3pt solid #1d4ed8; margin: 15pt 0; }
						table { width: 100%; border-collapse: collapse; table-layout: fixed; -fs-table-paginate: paginate; }
						tr { page-break-inside: avoid; }
						thead { display: table-header-group; }
						.scope td { width: 50%; vertical-align: top; background: #f1f5f9; padding: 10pt 12pt; }
						.scope strong { display: block; color: #64748b; font-size: 8pt; font-weight: normal; }
						.scope .dates { font-weight: bold; font-size: 11pt; color: #0f172a; }
						.filters { font-size: 8pt; color: #475569; margin: 9pt 0; word-wrap: break-word; }
						.kpis { border-collapse: separate; border-spacing: 5pt; margin: 8pt -5pt 10pt; }
						.kpis td { vertical-align: top; border: 0.6pt solid #dbe3ef; border-top: 2pt solid #2563eb; padding: 10pt; background: #ffffff; }
						.kpis .name { color: #475569; font-size: 8pt; margin-bottom: 6pt; }
						.kpis .value { color: #0f172a; font-size: 21pt; font-weight: bold; line-height: 1.15; margin-bottom: 7pt; }
						.kpis .detail { color: #64748b; font-size: 7pt; line-height: 1.45; }
						.note { padding: 9pt 11pt; border-left: 2pt solid #2563eb; background: #eff6ff; color: #334155; font-size: 8pt; margin: 9pt 0 12pt; }
						.data { font-size: 8pt; margin: 8pt 0 13pt; }
						.data th { padding: 7pt 6pt; background: #eaf0f8; color: #334155; font-size: 7.5pt; font-weight: bold; text-align: right; border-bottom: 1pt solid #94a3b8; }
						.data td { padding: 4pt 6pt; text-align: right; border-bottom: 0.5pt solid #e2e8f0; color: #334155; word-wrap: break-word; }
						.data .first { text-align: left; }
						.data .alternate td { background: #f8fafc; }
						.data .metric { width: 34%; }
						.page { page-break-before: always; }
						.chart { page-break-inside: avoid; border: 0.6pt solid #e2e8f0; padding: 10pt; margin-bottom: 12pt; }
						.chart img { width: 100%; height: 155pt; }
						.chart h3 { margin-top: 0; }
						.reference { font-size: 7pt; color: #94a3b8; margin-top: 10pt; }
						</style></head><body>
						""");
		html.append("<div class='report-header'>").append(brand).append(" / Enterprise Analytics</div>")
				.append("<div class='report-footer'>").append(brand).append(" | Enterprise Analytics</div>")
				.append("<div class='wordmark'>").append(brand)
				.append("</div><div class='eyebrow'>Enterprise Analytics</div><h1>").append(xml(reportTitle))
				.append("</h1><p class='muted'>Generated ")
				.append(xml(DateTimeFormatter.ofPattern("MMM d, uuuu 'At' HH:mm 'UTC'", Locale.US)
						.withZone(ZoneOffset.UTC).format(Instant.parse(scope.get("Generated At").toString()))))
				.append(" | Prepared By ").append(xml(scope.get("Generated By")))
				.append("</p><div class='rule'></div><table class='scope'><tr>");
		for (boolean benchmark : List.of(false, true)) {
			html.append("<td><strong>").append(benchmark ? "Benchmark Period" : "Reporting Period")
					.append("</strong><span class='dates'>")
					.append(xml(scope.get(benchmark ? "Comparison Start Date" : "Start Date"))).append(" - ")
					.append(xml(scope.get(benchmark ? "Comparison End Date" : "End Date"))).append("</span><br />")
					.append(benchmark ? benchmarkDays : currentDays).append(" Calendar Days | Inclusive</td>");
		}
		html.append("</tr></table><div class='filters'>");
		for (String dimension : List.of("User", "App", "Engine")) {
			String value = String.valueOf(scope.getOrDefault(dimension, ""));
			html.append("<div><b>").append(dimension).append(": </b>").append(xml(
					value.isBlank() ? "All " + dimension + "s" : value.startsWith("=") ? value.substring(1) : value))
					.append("</div>");
		}
		html.append("</div><h2>Executive Summary</h2><table class='kpis'>");
		String[][] cards = model
				? new String[][] { { "Requests", "Model Requests" }, { "Tokens", "Recorded Tokens" },
						{ "Users", "Active Users" }, { "Apps", "Active Apps" }, { "Models", "Active Models" },
						{ "Average Latency (ms)", "Average Latency" } }
				: new String[][] { { "Events", "Activity Events" }, { "Users", "Active Platform Users" },
						{ "Failed", "Failed Events" }, { "Activity Success Rate (%)", "Success Rate" } };
		int columns = model ? 3 : 2;
		for (int i = 0; i < cards.length; i++) {
			if (i % columns == 0) {
				html.append("<tr>");
			}
			Map<String, Object> metric = byMetric.getOrDefault(cards[i][0], Map.of());
			html.append("<td style='width:").append(100 / columns).append("%'><div class='name'>")
					.append(xml(cards[i][1])).append("</div><div class='value'>")
					.append(xml(pdfHeadline(metric.get("Current"), cards[i][0]))).append("</div><div class='detail'>");
			if (metric.get("Benchmark") != null) {
				html.append("Benchmark: ").append(xml(pdfValue(metric.get("Benchmark"), cards[i][0])));
				boolean perDay = metric.get("Current Per Day") instanceof Number;
				Object current = perDay ? metric.get("Current Per Day") : metric.get("Current");
				Object prior = perDay ? metric.get("Benchmark Per Day") : metric.get("Benchmark");
				boolean distinct = Set.of("Users", "Apps", "Models", "Rooms").contains(cards[i][0]);
				if (!(distinct && currentDays != benchmarkDays) && current instanceof Number n
						&& prior instanceof Number p && p.doubleValue() > 0) {
					double change = (n.doubleValue() / p.doubleValue() - 1) * 100;
					html.append("<br />").append(change > 0 ? "+" : "").append(xml(pdfValue(change, "")))
							.append("% Change").append(perDay ? " Per Day" : "");
				} else if (distinct && currentDays != benchmarkDays) {
					html.append("<br />Unequal Period Lengths");
				}
			} else {
				html.append("Current Period");
			}
			html.append("</div></td>");
			if (i % columns == columns - 1) {
				html.append("</tr>");
			}
		}
		html.append("</table><div class='note'>").append(currentDays == benchmarkDays
				? "Both Periods Cover The Same Number Of Calendar Days. Comparisons Use Each Full Period."
				: "Periods Have Different Lengths. Volume Changes Compare Daily Averages; Headline Values Remain Period Totals. Distinct Users, Apps And Models Are Not Divided By Duration.")
				.append("</div><h3>Reading This Report</h3><p class='muted'>")
				.append(model
						? "Requests Count Model INPUT Records. Tokens Include Recorded INPUT And RESPONSE Tokens. Latency Measures The Complete Model Response."
						: "Activity Events Include Recorded Operations Across Engine Types. Success Rate Uses Only Events With Known Outcomes. Activity Events And Model Requests Are Separate Populations.")
				.append(" Missing Values Appear As A Dash; Missing Telemetry Is Not Zero.</p>")
				.append("<p class='reference'>Report Reference: ")
				.append(xml(file.getFileName().toString().replace("Enterprise-Usage-", "").replace(".pdf", "")))
				.append("</p>");
		html.append(
				"<div class='page'><div class='eyebrow'>Period Comparison</div><h2>Usage Over Time</h2><p class='muted'>Both Complete Periods Align At Day 1. Blank Segments Mark Unreported Values Or Days Outside A Period. Quiet Days Are Zero For Volume Metrics.</p>");
		List<Map<String, Object>> daily = sections.getOrDefault("Daily " + label, List.of());
		List<Map<String, Object>> benchmark = sections.getOrDefault("Daily Benchmark " + label, List.of());
		for (String metric : model ? List.of("Requests", "Tokens") : List.of("Events", "Failed")) {
			html.append("<div class='chart'><h3>").append(xml(metric.equals("Failed") ? "Failed Events" : metric))
					.append("</h3><img alt='").append(xml(metric)).append(" Comparison' src='")
					.append(pdfChart(daily, benchmark, scope, metric)).append("' /></div>");
		}
		html.append("<h3>Coverage And Interpretation</h3><p class='muted'>").append(xml(NOTES)).append("</p></div>");
		html.append(
				"<div class='page'><div class='eyebrow'>Metric Detail</div><h2>KPI Comparison</h2><p class='muted'>Full-Period Values With Daily Averages For Additive Metrics When Period Lengths Differ. Distinct Counts And Rates Are Not Normalized.</p>");
		List<String> metricColumns = currentDays == benchmarkDays ? List.of("Metric", "Current", "Benchmark")
				: List.of("Metric", "Current", "Benchmark", "Current Per Day", "Benchmark Per Day");
		pdfTable(html, metrics, metricColumns);
		html.append("</div>");
		for (boolean prior : List.of(false, true)) {
			html.append("<div class='page'><div class='eyebrow'>Daily Detail / ")
					.append(prior ? "Benchmark Period" : "Reporting Period").append("</div><h2>Daily ")
					.append(prior ? "Benchmark " : "").append(xml(label))
					.append("</h2><p class='muted'>Recorded Daily Aggregates | Dates Use Log Database Time. Dates Without Records Are Omitted From This Table.</p>");
			pdfTable(html, prior ? benchmark : daily,
					model ? List.of("Day", "Requests", "Input Tokens", "Output Tokens", "Users", "Average Latency (ms)")
							: List.of("Day", "Events", "Users", "Failed", "Known Outcomes"));
			html.append("</div>");
		}
		html.append("</body></html>");
		try (OutputStream output = Files.newOutputStream(file)) {
			ITextRenderer renderer = new ITextRenderer();
			// Reuse OpenPDF's bundled font with Unicode encoding and embed it so
			// supported glyphs do not depend on fonts installed on the report reader.
			renderer.getFontResolver().addFont("font-fallback/LiberationSans-Regular.ttf", BaseFont.IDENTITY_H,
					BaseFont.EMBEDDED);
			// Flying Saucer parses this through StringReader, avoiding any default-
			// charset byte conversion. Keep source literals ASCII (Java uses cp1252)
			// while preserving Unicode names and other runtime values unchanged.
			renderer.setDocumentFromString(html.toString());
			renderer.layout();
			renderer.createPDF(output);
		}
	}

	/** Writes a compact table with repeated column headers and unbroken rows. */
	private static void pdfTable(StringBuilder html, List<Map<String, Object>> rows, List<String> columns) {
		if (rows.isEmpty()) {
			html.append("<p class='muted'>No Recorded Data In This Period.</p>");
			return;
		}
		html.append("<table class='data'><thead><tr>");
		for (int i = 0; i < columns.size(); i++) {
			html.append("<th class='").append(i == 0 ? "first" : "")
					.append(columns.get(0).equals("Metric") && i == 0 ? " metric" : "").append("'>")
					.append(xml(columns.get(i))).append("</th>");
		}
		html.append("</tr></thead><tbody>");
		for (int row = 0; row < rows.size(); row++) {
			html.append("<tr class='").append(row % 2 == 1 ? "alternate" : "").append("'>");
			for (int col = 0; col < columns.size(); col++) {
				Object value = rows.get(row).get(columns.get(col));
				html.append("<td class='").append(col == 0 ? "first" : "").append("'>")
						.append(xml(value instanceof Number ? pdfValue(value, "") : value)).append("</td>");
			}
			html.append("</tr>");
		}
		html.append("</tbody></table>");
	}

	/**
	 * Keeps very large headline counts within their cards; detail tables retain
	 * exact values.
	 */
	private static String pdfHeadline(Object value, String metric) {
		if (value instanceof Number number && Math.abs(number.doubleValue()) >= 1_000_000_000
				&& !metric.endsWith("(ms)")) {
			NumberFormat compact = NumberFormat.getCompactNumberInstance(Locale.US, NumberFormat.Style.SHORT);
			compact.setMaximumFractionDigits(2);
			return compact.format(number);
		}
		return pdfValue(value, metric);
	}

	/** Formats report numbers consistently without hiding unreported telemetry. */
	private static String pdfValue(Object value, String metric) {
		if (!(value instanceof Number number)) {
			return value == null ? "-" : value.toString();
		}
		NumberFormat format = NumberFormat.getNumberInstance(Locale.US);
		format.setMaximumFractionDigits(2);
		return format.format(metric.endsWith("(ms)") ? number.doubleValue() / 1000 : number.doubleValue())
				+ (metric.endsWith("(ms)") ? " s" : metric.endsWith("(%)") ? "%" : "");
	}

	/**
	 * Creates an in-memory comparison chart with complete windows and null gaps.
	 */
	private static String pdfChart(List<Map<String, Object>> current, List<Map<String, Object>> benchmark,
			Map<String, Object> scope, String metric) throws Exception {
		XYSeriesCollection dataset = new XYSeriesCollection();
		int maxDays = 1;
		for (boolean prior : List.of(false, true)) {
			List<Map<String, Object>> rows = prior ? benchmark : current;
			Map<String, Map<String, Object>> byDay = new LinkedHashMap<>();
			rows.forEach(row -> byDay.put(String.valueOf(row.get("Day")), row));
			LocalDate from = LocalDate.parse(scope.get(prior ? "Comparison Start Date" : "Start Date").toString());
			LocalDate to = LocalDate.parse(scope.get(prior ? "Comparison End Date" : "End Date").toString());
			XYSeries series = new XYSeries(prior ? "Benchmark Period" : "Current Period");
			int day = 1;
			for (LocalDate date = from; !date.isAfter(to); date = date.plusDays(1), day++) {
				Map<String, Object> row = byDay.get(date.toString());
				Object value = row == null ? 0 : row.get(metric);
				series.add(day, value instanceof Number number ? number : null);
			}
			maxDays = Math.max(maxDays, day - 1);
			dataset.addSeries(series);
		}
		var chart = ChartFactory.createXYLineChart(null, "Day In Period", metric, dataset, PlotOrientation.VERTICAL,
				true, false, false);
		chart.setBackgroundPaint(Color.WHITE);
		chart.setAntiAlias(true);
		chart.getLegend().setItemFont(new Font(Font.SANS_SERIF, Font.PLAIN, 20));
		chart.getLegend().setFrame(BlockBorder.NONE);
		chart.getLegend().setItemPaint(new Color(0x475569));
		var plot = chart.getXYPlot();
		plot.setBackgroundPaint(Color.WHITE);
		plot.setOutlineVisible(false);
		plot.setDomainGridlinesVisible(false);
		plot.setRangeGridlinePaint(new Color(0xe2e8f0));
		var renderer = new XYLineAndShapeRenderer(true, maxDays <= 31);
		renderer.setSeriesPaint(0, new Color(0x2563eb));
		renderer.setSeriesPaint(1, new Color(0x64748b));
		renderer.setSeriesStroke(0, new BasicStroke(3));
		renderer.setSeriesStroke(1,
				new BasicStroke(2, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND, 0, new float[] { 8, 6 }, 0));
		plot.setRenderer(renderer);
		NumberAxis domain = (NumberAxis) plot.getDomainAxis();
		domain.setRange(0.5, maxDays + 0.5);
		domain.setTickUnit(new NumberTickUnit(Math.max(1, Math.ceil(maxDays / 7d))));
		NumberAxis range = (NumberAxis) plot.getRangeAxis();
		range.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		NumberFormat axisFormat = NumberFormat.getCompactNumberInstance(Locale.US, NumberFormat.Style.SHORT);
		axisFormat.setMaximumFractionDigits(1);
		range.setNumberFormatOverride(axisFormat);
		for (var axis : List.of(domain, range)) {
			axis.setLabelFont(new Font(Font.SANS_SERIF, Font.PLAIN, 20));
			axis.setTickLabelFont(new Font(Font.SANS_SERIF, Font.PLAIN, 18));
			axis.setLabelPaint(new Color(0x475569));
			axis.setTickLabelPaint(new Color(0x64748b));
			axis.setAxisLineVisible(false);
			axis.setTickMarksVisible(false);
		}
		return "data:image/png;base64,"
				+ Base64.getEncoder().encodeToString(ChartUtilities.encodeAsPNG(chart.createBufferedImage(1400, 460)));
	}

	private static String xml(Object value) {
		return StringEscapeUtils.escapeXml10(value == null ? "-" : value.toString());
	}

	private static String title(String value) {
		if (value.equals("LATENCY_MS")) {
			return "Average Latency (ms)";
		}
		return java.util.Arrays.stream(value.replaceAll("([a-z])([A-Z])", "$1 $2").replace('_', ' ').split(" "))
				.map(word -> word.isEmpty() ? ""
						: word.equalsIgnoreCase("id") ? "ID"
								: word.substring(0, 1).toUpperCase(Locale.ROOT)
										+ word.substring(1).toLowerCase(Locale.ROOT))
				.collect(java.util.stream.Collectors.joining(" "));
	}

	@Override
	public String getReactorDescription() {
		return "Exports Admin-Only Enterprise Usage Reports As Audited CSV Or PDF Downloads.";
	}
}
