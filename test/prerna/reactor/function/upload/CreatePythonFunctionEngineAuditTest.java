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
package prerna.reactor.function.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.om.Insight;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

class CreatePythonFunctionEngineAuditTest {

	@TempDir
	Path directory;

	@Test
	void successfulCreationAuditsExactlyOnceWithoutSourceOrConfiguration() throws Exception {
		runCreation(false);
	}

	@Test
	void failedCreationDoesNotEmitSuccess() throws Exception {
		runCreation(true);
	}

	private void runCreation(boolean fail) throws Exception {
		User user = mock(User.class);
		when(user.getLogins()).thenReturn(List.of());
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		File tempSmss = Files.createFile(directory.resolve("function.temp")).toFile();
		Files.createFile(directory.resolve("main.py"));
		var reactor = new CreatePythonFunctionEngineReactor() {
			@Override
			protected void validateUserAndEngineAccess(User ignored) {
			}

			@Override
			protected void organizeKeys() {
			}

			@Override
			protected String getStringFromKeyOrCurRowStringValue(String key) {
				return "Test function";
			}

			@Override
			protected Map<String, Object> getMapFromKeyOrCurRow(String key) {
				return Map.of(IFunctionEngine.FUNCTION_TYPE, "LOCAL_PYTHON", IFunctionEngine.PYTHON_FILE_NAME,
						"main.py", "password", "private-secret");
			}
		};
		reactor.setInsight(insight);
		try (var security = mockStatic(AbstractSecurityUtils.class);
				var engines = mockStatic(SecurityEngineUtils.class);
				var utility = mockStatic(Utility.class);
				var uploads = mockStatic(UploadUtilities.class);
				var files = mockStatic(FileUtils.class);
				var cluster = mockStatic(ClusterUtil.class);
				var audit = mockStatic(UserAuditTrailUtils.class);
				var functions = mockConstruction(LocalPythonFunctionEngine.class)) {
			utility.when(() -> Utility.validateName(anyString())).thenReturn(true);
			uploads.when(() -> UploadUtilities.generateSpecificEngineAssetsFolder(eq(IEngine.CATALOG_TYPE.FUNCTION),
					anyString(), anyString())).thenReturn(directory.toFile());
			uploads.when(
					() -> UploadUtilities.createTemporaryFunctionSmss(anyString(), anyString(), anyString(), anyMap()))
					.thenReturn(tempSmss);
			uploads.when(() -> UploadUtilities.getEngineReturnData(eq(user), anyString())).thenReturn(Map.of());
			if (fail) {
				cluster.when(() -> ClusterUtil.pushEngine(anyString()))
						.thenThrow(new IllegalStateException("Cluster unavailable"));
			}
			reactor.execute();
			if (fail) {
				audit.verifyNoInteractions();
			} else {
				audit.verify(() -> UserAuditTrailUtils.recordEngineLifecycle(eq(user),
						eq(AuditEventType.FUNCTION_CREATE), eq(AuditTargetType.FUNCTION), anyString(),
						eq("TestFunction"), eq(Map.of("functionType", "LOCAL_PYTHON"))), times(1));
				audit.verifyNoMoreInteractions();
				assertEquals(1, functions.constructed().size());
			}
		}
	}
}
