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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssEditCopy;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetProjectSMSSReactorUnitTests {

	private AdminGetProjectSMSSReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;
	private GenRowStruct projectGrs;

	@TempDir
	Path folder;

	@BeforeEach
	void setup() {
		reactor = new AdminGetProjectSMSSReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);

		ns = mock(NounStore.class);
		projectGrs = mock(GenRowStruct.class);
		reactor.setNounStore(ns);
	}

	@Test
	void test_AdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	void test_ProjectIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Need to define the project", e.getMessage());
		}
	}

	@Test
	void aMissingSmssFileIsReported() {
		askFor("id");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
			IProject project = projectAt(this.folder.resolve("missing.smss").toString());
			utility.when(() -> Utility.getProject("id")).thenReturn(project);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find smss file for project id. Please reach out to an administrator for assistance",
					e.getMessage());
		}
	}

	@Test
	void returnsTheMaskedSmssAndTheHashASaveHandsBack() throws IOException {
		askFor("id");
		Path smss = Files.writeString(this.folder.resolve("project.smss"), "ENGINE\tid\nPASSWORD\tsecret\n");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<SecretsFactory> secrets = Mockito.mockStatic(SecretsFactory.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
			IProject project = projectAt(smss.toString());
			utility.when(() -> Utility.getProject("id")).thenReturn(project);

			NounMetadata nm = reactor.execute();

			assertEquals(PixelDataType.MAP, nm.getNounType());
			Map<?, ?> copy = (Map<?, ?>) nm.getValue();
			assertEquals(SmssUtilities.concealSmssSensitiveInfo("ENGINE\tid\nPASSWORD\tsecret\n"),
					copy.get(SmssEditCopy.SMSS));
			assertFalse(copy.get(SmssEditCopy.SMSS).toString().contains("secret"));
			assertEquals(SmssEditCopy.read(project).getSmssHash(), copy.get(SmssEditCopy.SMSS_HASH));
		}
	}

	private void askFor(String id) {
		when(ns.size()).thenReturn(2);
		when(ns.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey())).thenReturn(projectGrs);
		when(projectGrs.isEmpty()).thenReturn(false);
		when(projectGrs.get(0)).thenReturn(id);
	}

	private static IProject projectAt(String smssFilePath) {
		IProject project = mock(IProject.class);
		when(project.getSmssFilePath()).thenReturn(smssFilePath);
		when(project.getEngineId()).thenReturn("id");
		when(project.getEngineName()).thenReturn("name");
		when(project.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.PROJECT);
		return project;
	}

}
