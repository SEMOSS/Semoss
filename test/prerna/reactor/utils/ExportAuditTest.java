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
package prerna.reactor.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.app.ExportAppReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.AgentProjectArchiveUtils;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

/**
 * Exporting a project or an engine records one export event once its archive is
 * built, named for the reactor that ran it, and none for an export that fails.
 */
class ExportAuditTest {

	@TempDir
	Path directory;

	@Test
	void appExportsRecordTheReactorThatRanThem() throws Exception {
		exportApp(new ExportProjectAppReactor(), "ExportProjectApp");
		exportApp(new ExportAppReactor(), "ExportApp");
	}

	@Test
	void engineExportRecordsTheEngineTypeAndVersionHistory() throws Exception {
		exportEngine(false);
	}

	@Test
	void engineExportThatFailsRecordsNoExport() throws Exception {
		exportEngine(true);
	}

	private void exportApp(ExportProjectAppReactor reactor, String pixelName) throws Exception {
		User user = mock(User.class);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		when(insight.getInsightFolder()).thenReturn(this.directory.toString());
		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("Planner");
		when(project.getProjectType()).thenReturn(IProject.PROJECT_TYPE.CODE);
		DIHelper helper = mock(DIHelper.class);
		when(helper.getProperty(Constants.BASE_FOLDER)).thenReturn(this.directory.toString());
		reactor.setInsight(insight);
		reactor.setNounStore(store(ReactorKeysEnum.PROJECT.getKey(), "project-1"));

		try (var security = mockStatic(SecurityProjectUtils.class);
				var admins = mockStatic(SecurityAdminUtils.class);
				var dihelper = mockStatic(DIHelper.class);
				var utility = mockStatic(Utility.class);
				var assets = mockStatic(AssetUtility.class);
				var zips = mockStatic(ZipUtils.class);
				var agents = mockStatic(AgentProjectArchiveUtils.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, "project-1"))
					.thenReturn("project-1");
			security.when(() -> SecurityProjectUtils.userIsOwner(user, "project-1")).thenReturn(true);
			dihelper.when(DIHelper::getInstance).thenReturn(helper);
			utility.when(() -> Utility.getProject("project-1")).thenReturn(project);
			utility.when(() -> Utility.normalizePath(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
			assets.when(() -> AssetUtility.getProjectAssetsFolder("Planner", "project-1"))
					.thenReturn(this.directory.toString());
			zips.when(() -> ZipUtils.zipFolder(anyString(), anyString(), any(), any())).thenAnswer(
					invocation -> new ZipOutputStream(new FileOutputStream(invocation.getArgument(1, String.class))));

			reactor.execute();

			// an app's files never carry the version history, so it is not recorded
			audit.verify(() -> UserAuditTrailUtils.recordProjectExport(user, "project-1", "Planner", "CODE", pixelName,
					null), times(1));
			audit.verifyNoMoreInteractions();
		}
	}

	private void exportEngine(boolean fail) throws Exception {
		User user = mock(User.class);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		when(insight.getInsightFolder()).thenReturn(this.directory.toString());
		IEngine engine = mock(IEngine.class);
		when(engine.getEngineName()).thenReturn("Sales");
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
		when(engine.getSmssFilePath()).thenReturn(this.directory.resolve("Sales.smss").toString());
		ExportEngineReactor reactor = new ExportEngineReactor();
		reactor.setInsight(insight);
		NounStore store = store(ReactorKeysEnum.ENGINE.getKey(), "engine-1");
		store.makeGenRowStruct("keepGit").add("true", PixelDataType.CONST_STRING);
		reactor.setNounStore(store);

		try (var queries = mockStatic(SecurityQueryUtils.class);
				var engines = mockStatic(SecurityEngineUtils.class);
				var admins = mockStatic(SecurityAdminUtils.class);
				var utility = mockStatic(Utility.class);
				var folders = mockStatic(EngineUtility.class);
				var zips = mockStatic(ZipUtils.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			queries.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "engine-1")).thenReturn("engine-1");
			engines.when(() -> SecurityEngineUtils.userIsOwner(user, "engine-1")).thenReturn(true);
			utility.when(() -> Utility.getEngine("engine-1")).thenReturn(engine);
			utility.when(() -> Utility.normalizePath(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
			// no engine folder, so only the metadata and the smss file go in the archive
			folders.when(
					() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, "engine-1", "Sales"))
					.thenReturn(this.directory.resolve("no-engine-folder").toString());

			if (fail) {
				zips.when(() -> ZipUtils.zipObjectToFile(any(), any(), anyString(), any()))
						.thenThrow(new IllegalStateException("Disk full"));
				assertThrows(SemossPixelException.class, reactor::execute);
				audit.verifyNoInteractions();
			} else {
				reactor.execute();
				audit.verify(() -> UserAuditTrailUtils.recordEngineExport(user, AuditTargetType.DATABASE, "engine-1",
						"Sales", "ExportEngine", true), times(1));
				audit.verifyNoMoreInteractions();
			}
		}
	}

	/** A noun store holding one string key, as a pixel passes it. */
	private static NounStore store(String key, String value) {
		NounStore store = new NounStore("export-audit-test");
		store.makeGenRowStruct(key).add(value, PixelDataType.CONST_STRING);
		return store;
	}
}
