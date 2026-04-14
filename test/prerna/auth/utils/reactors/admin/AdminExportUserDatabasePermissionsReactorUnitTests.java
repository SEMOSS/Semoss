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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class AdminExportUserDatabasePermissionsReactorUnitTests {

	private AdminExportUserDatabasePermissionsReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	private GenRowStruct fnGrs;
	private GenRowStruct fpGrs;
	private GenRowStruct pwGrs;
	private GenRowStruct dbGrs;
	private GenRowStruct panelGrs;

	@BeforeEach
	void setup() throws IOException {
		reactor = new AdminExportUserDatabasePermissionsReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);

		ns = mock(NounStore.class);

		fnGrs = mock(GenRowStruct.class);
		fpGrs = mock(GenRowStruct.class);
		pwGrs = mock(GenRowStruct.class);
		dbGrs = mock(GenRowStruct.class);
		panelGrs = mock(GenRowStruct.class);

		reactor.setNounStore(ns);
	}

	@Test
	void testAdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
		// when(sau.apply(user)).thenReturn(null);
	}

	@Test
	void testPasswordIsNull() {
		when(ns.getGenRowStruct(ReactorKeysEnum.FILE_NAME.getKey())).thenReturn(null);
		when(ns.getGenRowStruct(ReactorKeysEnum.FILE_PATH.getKey())).thenReturn(null);
		when(ns.getGenRowStruct(ReactorKeysEnum.PASSWORD.getKey())).thenReturn(null);
		when(ns.getGenRowStruct(ReactorKeysEnum.DATABASE.getKey())).thenReturn(null);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testPasswordIsEmpty() {
		when(ns.size()).thenReturn(2);

		when(ns.getGenRowStruct(ReactorKeysEnum.PASSWORD.getKey())).thenReturn(pwGrs);

		when(pwGrs.isEmpty()).thenReturn(false);
		when(pwGrs.get(0)).thenReturn("");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a password to encrypt the file", e.getMessage());
		}
	}

	@Test
	void testFileEmpty() throws Exception {
		when(ns.size()).thenReturn(2);

		when(ns.getGenRowStruct(ReactorKeysEnum.PASSWORD.getKey())).thenReturn(pwGrs);

		when(ns.getGenRowStruct(ReactorKeysEnum.PANEL.getKey())).thenReturn(panelGrs);
		when(panelGrs.isEmpty()).thenReturn(false);

		InsightPanel ip = mock(InsightPanel.class);
		NounMetadata nm = new NounMetadata(ip, PixelDataType.PANEL);
		Map<String, Map<String, String>> panelFormatting = new HashMap<>();
		when(ip.getPanelFormatValues()).thenReturn(panelFormatting);
		when(ip.getSheetId()).thenReturn("0");

		InsightSheet is = mock(InsightSheet.class);
		when(insight.getInsightSheet("0")).thenReturn(is);
		when(is.getSheetLabel()).thenReturn("sheetLabel");

		when(panelGrs.getNoun(0)).thenReturn(nm);

		when(pwGrs.isEmpty()).thenReturn(false);
		when(pwGrs.get(0)).thenReturn("encrypt");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<ExcelUtility> eu = Mockito.mockStatic(ExcelUtility.class);
				MockedStatic<Paths> mockPaths = Mockito.mockStatic(Paths.class);
				MockedStatic<Files> mockFiles = Mockito.mockStatic(Files.class);
				MockedConstruction<SXSSFWorkbook> workbook = Mockito.mockConstruction(SXSSFWorkbook.class,
						(mock, context) -> {
							SXSSFSheet sheet = mock(SXSSFSheet.class);
							when(mock.createSheet("sheetLabel")).thenReturn(sheet);

							CreationHelper creationHelper = mock(CreationHelper.class);
							when(mock.getCreationHelper()).thenReturn(creationHelper);

							DataFormat df = mock(DataFormat.class);
							when(creationHelper.createDataFormat()).thenReturn(df);

							CellStyle cs = mock(CellStyle.class);
							when(mock.createCellStyle()).thenReturn(cs);
						});
				MockedStatic<SystemEngineRegistry> ser = Mockito.mockStatic(SystemEngineRegistry.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			RDBMSNativeEngine db = mock(RDBMSNativeEngine.class);
			ser.when(() -> SystemEngineRegistry.getSecurityDb()).thenReturn(db);

			WrapperManager wmmock = mock(WrapperManager.class);
			wm.when(WrapperManager::getInstance).thenReturn(wmmock);

			IRawSelectWrapper irswMock = mock(IRawSelectWrapper.class);
			when(wmmock.getRawWrapper(eq(db), any(SelectQueryStruct.class))).thenReturn(irswMock);

			ZonedDateTime zdt = ZonedDateTime.of(LocalDateTime.of(2025, 2, 14, 1, 1), ZoneId.of("UTC"));
			utility.when(() -> Utility.getCurrentZonedDateTimeForUser(user)).thenReturn(zdt);

			utility.when(() -> Utility.normalizePath("/work/insight1")).thenReturn("/work/insight1");
			utility.when(() -> Utility.normalizePath("/work/insight1/null.xlsx"))
					.thenReturn("/work/insight1/null.xlsx");

			String path = "/work/insight1";
			when(insight.getInsightFolder()).thenReturn(path);

			Path p = mock(Path.class);
			mockPaths.when(() -> Paths.get("/work/insight1")).thenReturn(p);

			mockFiles.when(() -> Files.exists(p)).thenReturn(false);

			when(irswMock.hasNext()).thenReturn(false);

			NounMetadata result = reactor.execute();
			assertNotNull(result.getValue());
			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			assertEquals(PixelOperationType.FILE_DOWNLOAD, result.getOpType().get(0));
			NounMetadata additionalResult = result.getAdditionalReturn().get(0);
			assertEquals("Successfully generated the excel file", additionalResult.getValue().toString());
			assertEquals(PixelDataType.CONST_STRING, additionalResult.getNounType());
			assertEquals(PixelOperationType.SUCCESS, additionalResult.getOpType().get(0));

			eu.verify(() -> ExcelUtility.encrypt(any(Workbook.class), eq("/work/insight1/null.xlsx"), eq("encrypt")),
					times(1));

			mockFiles.verify(() -> Files.createDirectories(p), times(1));
		}
	}
}
