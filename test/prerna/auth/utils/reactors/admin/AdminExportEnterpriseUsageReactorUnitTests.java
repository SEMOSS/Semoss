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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.text.PDFTextStripper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.EnterpriseUsageUtils;
import prerna.auth.utils.EnterpriseUsageUtils.Source;
import prerna.auth.utils.EnterpriseUsageUtils.View;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.logging.SemossLogUtils;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.sablecc2.om.PixelOperationType;
import prerna.theme.AdminThemeUtils;

class AdminExportEnterpriseUsageReactorUnitTests {

	@TempDir
	Path folder;
	private Insight insight;
	private User user;
	private Logger classLogger;
	private AdminExportEnterpriseUsageReactor reactor;
	private MockedStatic<AdminThemeUtils> theme;

	@BeforeEach
	void setup() {
		theme = mockStatic(AdminThemeUtils.class);
		theme.when(AdminThemeUtils::getActiveAdminTheme).thenReturn(Map.of());
		insight = mock(Insight.class);
		user = mock(User.class);
		classLogger = mock(Logger.class);
		var token = mock(AccessToken.class);
		when(user.getPrimaryLoginToken()).thenReturn(token);
		when(token.getId()).thenReturn("admin-actor");
		when(token.getName()).thenReturn("Administrator");
		when(insight.getUser()).thenReturn(user);
		when(insight.getInsightId()).thenReturn("test-insight");
		when(insight.getInsightFolder()).thenReturn(folder.toString());
		reactor = new AdminExportEnterpriseUsageReactor();
		reactor.setInsight(insight);
		reactor.keyValue.putAll(Map.of("source", "model", "view", "ranking", "format", "csv", "dimension", "model",
				"startDate", "2024-03-01", "endDate", "2024-03-31", "user", "=target-user", "app", "", "model", ""));
	}

	@AfterEach
	void closeTheme() {
		theme.close();
	}

	@Test
	void deniesNonAdminsAndAuditsWithoutQueryingOrCreatingFiles() throws Exception {
		try (var security = mockStatic(SecurityAdminUtils.class);
				var usage = mockStatic(EnterpriseUsageUtils.class);
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			assertThrows(IllegalArgumentException.class, reactor::execute);
			usage.verifyNoInteractions();
			theme.verifyNoInteractions();
			verify(insight, never()).addExportFile(anyString(), any());
			var event = auditEvent();
			assertEquals(false, event.get(SemossLogUtils.IS_SUCCESS));
			assertEquals("admin-actor", event.get(SemossLogUtils.USER_ID));
			try (var files = Files.list(folder)) {
				assertEquals(0, files.count());
			}
		}
	}

	@Test
	void honorsExporterPolicyBeforeQueries() {
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var exporter = mockStatic(SecurityQueryUtils.class);
				var usage = mockStatic(EnterpriseUsageUtils.class);
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			policy.when(AbstractSecurityUtils::adminSetExporter).thenReturn(true);
			exporter.when(() -> SecurityQueryUtils.userIsExporter(user)).thenReturn(false);
			assertThrows(IllegalArgumentException.class, reactor::execute);
			usage.verifyNoInteractions();
			verify(insight, never()).addExportFile(anyString(), any());
			assertEquals(false, auditEvent().get(SemossLogUtils.IS_SUCCESS));
		}
	}

	@Test
	void writesServerQueriedCsvWithFormulaProtectionAndAuditedScope() throws Exception {
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var usage = mockUsage();
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			var row = new LinkedHashMap<String, Object>();
			row.put("ENTITY_ID", "model-1");
			row.put("ENTITY_NAME", "=SUM(1,2)");
			String unicodeName = "Zo\u00eb Dvo\u0159\u00e1k \u2013 \u6771\u4eac \ud83d\ude80";
			row.put("USER_NAME", unicodeName);
			row.put("TOKENS", 42);
			row.put("LATENCY_MS", null);
			usage.when(() -> EnterpriseUsageUtils.report(Source.MODEL, View.RANKING, "2024-03-01", "2024-03-31",
					"=target-user", "", "", "model", 20, 0)).thenReturn(Map.of("rows", List.of(row)));
			var noun = reactor.execute();
			assertTrue(noun.getOpType().contains(PixelOperationType.FILE_DOWNLOAD));
			var file = ArgumentCaptor.forClass(InsightFile.class);
			verify(insight).addExportFile(eq((String) noun.getValue()), file.capture());
			String csv = Files.readString(Path.of(file.getValue().getFilePath()), StandardCharsets.UTF_8);
			assertTrue(csv.startsWith("\ufeff"));
			assertTrue(csv.contains(unicodeName));
			assertTrue(csv.contains("\"'=SUM(1,2)\""));
			assertTrue(csv.contains("\"Entity Name\""));
			assertTrue(csv.contains("\"42\""));
			assertTrue(csv.contains("2024-03-01"));
			assertTrue(csv.contains("target-user"));
			var event = auditEvent();
			assertEquals(true, event.get(SemossLogUtils.IS_SUCCESS));
			assertEquals("admin-actor", event.get(SemossLogUtils.USER_ID));
			assertEquals("=target-user", JsonParser.parseString(event.get(SemossLogUtils.REQUEST).toString())
					.getAsJsonObject().get("User").getAsString());
			assertTrue(event.get(SemossLogUtils.RESPONSE).toString().contains("\"rows\":1"));
		}
	}

	@Test
	void logExportUsesServerCapAndRejectsUnsupportedFormats() {
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var usage = mockUsage();
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			reactor.keyValue.put("view", "logs");
			reactor.keyValue.put("limit", "999999");
			usage.when(() -> EnterpriseUsageUtils.report(Source.MODEL, View.LOGS, "2024-03-01", "2024-03-31",
					"=target-user", "", "", "model", 5000, 0)).thenReturn(Map.of("rows", List.of()));
			reactor.execute();
			usage.verify(() -> EnterpriseUsageUtils.report(Source.MODEL, View.LOGS, "2024-03-01", "2024-03-31",
					"=target-user", "", "", "model", 5000, 0));
			reactor.keyValue.put("format", "html");
			assertThrows(IllegalArgumentException.class, reactor::execute);
			verify(insight, times(1)).addExportFile(anyString(), any());
		}
	}

	@Test
	void generatesPdfForOnlyTheSelectedSourceWithExplicitBenchmark() throws Exception {
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var usage = mockUsage();
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			String brand = "Acm\u00e9 <Research> & \"Partners\"";
			theme.when(AdminThemeUtils::getActiveAdminTheme).thenReturn(Map.of("THEME_MAP",
					"{\"brand\":{\"name\":\"Acm\u00e9 <Research> & \\\"Partners\\\"\"},\"name\":\"Legacy Name\"}"));
			String author = "Zo\u00eb Dvo\u0159\u00e1k \u2013 \u0394\u03ad\u03bb\u03c4\u03b1";
			when(user.getPrimaryLoginToken().getName()).thenReturn(author);
			reactor.keyValue.putAll(Map.of("view", "overview", "format", "pdf", "comparisonStartDate", "2023-03-01",
					"comparisonEndDate", "2023-04-30"));
			usage.when(() -> EnterpriseUsageUtils.report(any(), any(), anyString(), anyString(), anyString(),
					anyString(), anyString(), anyString(), anyInt(), eq(0))).thenAnswer(call -> {
						if (call.getArgument(0) == Source.ACTIVITY) {
							throw new IllegalArgumentException("Unavailable");
						}
						View view = call.getArgument(1);
						if (view == View.TREND) {
							var start = java.time.LocalDate.parse(call.getArgument(2, String.class));
							var end = java.time.LocalDate.parse(call.getArgument(3, String.class));
							var rows = start.datesUntil(end.plusDays(1)).map(
									day -> Map.<String, Object>of("DAY", day.toString(), "REQUESTS", 2, "TOKENS", 40))
									.toList();
							return Map.of("rows", rows);
						}
						Map<String, Object> row = switch (view) {
						case LATENCY -> Map.of("P95_MS", 1000);
						case FEEDBACK -> Map.of("RATINGS", 2, "POSITIVE", 1);
						case TREND -> Map.of("DAY", "2024-03-01", "REQUESTS", 2);
						default -> Map.of("REQUESTS", 2, "TOKENS", 40, "TOKEN_ROWS", 4, "MESSAGE_ROWS", 4);
						};
						return Map.of("rows", List.of(row));
					});
			var noun = reactor.execute();
			var file = ArgumentCaptor.forClass(InsightFile.class);
			verify(insight).addExportFile(eq((String) noun.getValue()), file.capture());
			try (var document = Loader.loadPDF(Path.of(file.getValue().getFilePath()).toFile())) {
				assertTrue(document.getNumberOfPages() >= 6);
				String text = new PDFTextStripper().getText(document).replaceAll("\\s+", " ");
				int imageCount = 0;
				for (var page : document.getPages()) {
					for (var name : page.getResources().getXObjectNames()) {
						if (page.getResources().isImageXObject(name)) {
							imageCount++;
						}
					}
				}
				assertEquals(2, imageCount);
				assertTrue(text.contains("Executive Summary"));
				assertTrue(text.contains("Usage Over Time"));
				assertTrue(text.contains("KPI Comparison"));
				assertTrue(text.contains("Daily Benchmark Token Consumption"));
				assertTrue(text.contains("Page 1 Of"));
				assertTrue(text.contains(brand));
				assertTrue(text.contains("Prepared By " + author));
				assertTrue(text.contains("2024-03-01 - 2024-03-31"));
				assertTrue(text.contains("31 Calendar Days | Inclusive"));
				assertFalse(text.contains("SEMOSS"));
				assertFalse(text.contains("Legacy Name"));
				for (int page = 1; page <= document.getNumberOfPages(); page++) {
					var extract = new PDFTextStripper();
					extract.setStartPage(page);
					extract.setEndPage(page);
					String pageText = extract.getText(document);
					assertTrue(pageText.contains(brand + " / Enterprise Analytics"));
					assertTrue(pageText.contains(brand + " | Enterprise Analytics"));
					assertTrue(pageText.contains("Page " + page + " Of " + document.getNumberOfPages()));
				}
				assertTrue(text.contains("2023-03-01"));
				assertFalse(text.contains("Platform Activity"));
				assertTrue(text.contains("Token Consumption"));
				assertTrue(text.contains("Daily Token Consumption"));
				assertTrue(text.contains("Current Per Day"));
				assertTrue(text.contains("Benchmark Per Day"));
			}
			usage.verify(() -> EnterpriseUsageUtils.report(Source.MODEL, View.SUMMARY, "2023-03-01", "2023-04-30",
					"=target-user", "", "", "model", 1, 0));
			assertEquals(true, auditEvent().get(SemossLogUtils.IS_SUCCESS));
		}
	}

	@ParameterizedTest
	@MethodSource("brandingThemes")
	void resolvesBrandFromServerThemeWithSafeDefaults(Object activeTheme, String expected) throws Exception {
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var usage = mockUsage();
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			theme.when(AdminThemeUtils::getActiveAdminTheme).thenReturn(activeTheme);
			usage.when(() -> EnterpriseUsageUtils.report(Source.MODEL, View.RANKING, "2024-03-01", "2024-03-31",
					"=target-user", "", "", "model", 20, 0)).thenReturn(Map.of("rows", List.of()));
			var noun = reactor.execute();
			var file = ArgumentCaptor.forClass(InsightFile.class);
			verify(insight).addExportFile(eq((String) noun.getValue()), file.capture());
			String csv = Files.readString(Path.of(file.getValue().getFilePath()), StandardCharsets.UTF_8);
			assertTrue(csv.contains("\"Brand\""));
			assertTrue(csv.contains("\"" + expected + "\""));
			theme.verify(AdminThemeUtils::getActiveAdminTheme, times(1));
		}
	}

	private static Stream<Arguments> brandingThemes() {
		return Stream.of(
				Arguments.of(Map.of("THEME_MAP", "{\"brand\":{\"name\":\"  Acme Analytics  \"},\"name\":\"Legacy\"}"),
						"Acme Analytics"),
				Arguments.of(Map.of("ADMIN_THEME__THEME_MAP", "{\"brand\":{\"name\":\"Acm\u00e9\"}}"), "Acm\u00e9"),
				Arguments.of(Map.of("THEME_MAP", "{\"name\":\"Legacy Analytics\"}"), "Legacy Analytics"),
				Arguments.of(Map.of("THEME_MAP", "{\"brand\":{\"name\":\" \"},\"name\":\"Legacy Analytics\"}"),
						"Legacy Analytics"),
				Arguments.of(Map.of("THEME_MAP", "{\"brand\":{\"name\":123},\"name\":false}"), "SEMOSS"),
				Arguments.of(Map.of("THEME_MAP", "invalid json"), "SEMOSS"),
				Arguments.of(Map.of("THEME_MAP", "null"), "SEMOSS"), Arguments.of(Map.of(), "SEMOSS"));
	}

	@Test
	void failedFileCreationDoesNotRegisterADownloadAndIsAudited() throws Exception {
		Path notAFolder = Files.writeString(folder.resolve("occupied"), "test");
		when(insight.getInsightFolder()).thenReturn(notAFolder.toString());
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var usage = mockUsage();
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			usage.when(() -> EnterpriseUsageUtils.report(Source.MODEL, View.RANKING, "2024-03-01", "2024-03-31",
					"=target-user", "", "", "model", 20, 0)).thenReturn(Map.of("rows", List.of()));
			assertThrows(IllegalArgumentException.class, reactor::execute);
			verify(insight, never()).addExportFile(anyString(), any());
			assertEquals(false, auditEvent().get(SemossLogUtils.IS_SUCCESS));
		}
	}

	@Test
	void activityExportUsesCanonicalEngineAndAuditsOnlyThatFilter() throws Exception {
		try (var security = admin();
				var policy = mockStatic(AbstractSecurityUtils.class);
				var usage = mockUsage();
				var audit = mockStatic(SemossLogUtils.class)) {
			audit.when(SemossLogUtils::getEngineLevelLogger).thenReturn(classLogger);
			reactor.keyValue.putAll(
					Map.of("source", "activity", "view", "overview", "engine", "=function-1", "model", "=old-model"));
			usage.when(() -> EnterpriseUsageUtils.option(Source.class, "activity")).thenReturn(Source.ACTIVITY);
			usage.when(() -> EnterpriseUsageUtils.report(eq(Source.ACTIVITY), eq(View.SUMMARY), anyString(),
					anyString(), eq("=target-user"), eq(""), eq("=function-1"), eq("model"), eq(1), eq(0)))
					.thenReturn(Map.of("rows", List.of(Map.of("EVENTS", 5, "SUCCEEDED", 4, "KNOWN_OUTCOMES", 5))));
			var noun = reactor.execute();
			var file = ArgumentCaptor.forClass(InsightFile.class);
			verify(insight).addExportFile(eq((String) noun.getValue()), file.capture());
			String csv = Files.readString(Path.of(file.getValue().getFilePath()));
			assertTrue(csv.contains("Platform Activity"));
			assertFalse(csv.contains("Token Consumption"));
			assertFalse(csv.contains("old-model"));
			var scope = JsonParser.parseString(auditEvent().get(SemossLogUtils.REQUEST).toString()).getAsJsonObject();
			assertEquals("=function-1", scope.get("Engine").getAsString());
			assertFalse(scope.has("Model"));
		}
	}

	private MockedStatic<EnterpriseUsageUtils> mockUsage() {
		var usage = mockStatic(EnterpriseUsageUtils.class);
		usage.when(() -> EnterpriseUsageUtils.option(Source.class, "model")).thenReturn(Source.MODEL);
		usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.Dimension.class, "model"))
				.thenReturn(EnterpriseUsageUtils.Dimension.MODEL);
		return usage;
	}

	private MockedStatic<SecurityAdminUtils> admin() {
		var security = mockStatic(SecurityAdminUtils.class);
		security.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
		return security;
	}

	private Map<?, ?> auditEvent() {
		var event = ArgumentCaptor.forClass(Object.class);
		verify(classLogger).info(event.capture());
		return assertInstanceOf(Map.class, event.getValue());
	}
}
