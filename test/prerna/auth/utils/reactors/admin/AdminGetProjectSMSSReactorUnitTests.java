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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.SmssUtilities;
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

	private FileSystem fs;

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
		fs = Jimfs.newFileSystem(Configuration.unix());
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
	void test_SmssFileNotExist() {
		when(ns.size()).thenReturn(2);
		when(ns.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey())).thenReturn(projectGrs);
		when(projectGrs.isEmpty()).thenReturn(false);
		when(projectGrs.get(0)).thenReturn("id");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IProject project = mock(IProject.class);
			utility.when(() -> Utility.getProject("id")).thenReturn(project);
			when(project.getSmssFilePath()).thenReturn("Semoss.txt");

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find smss file for project id. Please reach out to an administrator for assistance",
					e.getMessage());
		}
	}

	@Test
	void test_SmssFileReturnDirectory() throws IOException {
		when(ns.size()).thenReturn(2);
		when(ns.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey())).thenReturn(projectGrs);
		when(projectGrs.isEmpty()).thenReturn(false);
		when(projectGrs.get(0)).thenReturn("id");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<FileSystems> fss = Mockito.mockStatic(FileSystems.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IProject project = mock(IProject.class);
			utility.when(() -> Utility.getProject("id")).thenReturn(project);
			when(project.getSmssFilePath()).thenReturn("dir");

			fss.when(FileSystems::getDefault).thenReturn(fs);
			Files.createDirectory(fs.getPath("dir"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find smss file for project id. Please reach out to an administrator for assistance",
					e.getMessage());
		}
	}

	@Test
	void test_SmssFileException() throws IOException {
		when(ns.size()).thenReturn(2);
		when(ns.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey())).thenReturn(projectGrs);
		when(projectGrs.isEmpty()).thenReturn(false);
		when(projectGrs.get(0)).thenReturn("id");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<Files> files = Mockito.mockStatic(Files.class);
				MockedStatic<Paths> paths = Mockito.mockStatic(Paths.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IProject project = mock(IProject.class);
			utility.when(() -> Utility.getProject("id")).thenReturn(project);
			when(project.getSmssFilePath()).thenReturn("Semoss.txt");

			Path p = mock(Path.class);
			paths.when(() -> Paths.get("Semoss.txt")).thenReturn(p);
			files.when(() -> Files.exists(p)).thenReturn(true);
			files.when(() -> Files.isRegularFile(p)).thenReturn(true);

			URI mockUri = mock(URI.class);
			when(p.toUri()).thenReturn(mockUri);
			paths.when(() -> Paths.get(mockUri)).thenReturn(p);

			files.when(() -> Files.readAllBytes(p)).thenThrow(new IOException("error"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("An error occurred reading the current project smss details. Detailed message = error",
					e.getMessage());
		}
	}

	@Test
	void test_SmssFileNoException() throws IOException {
		when(ns.size()).thenReturn(2);
		when(ns.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey())).thenReturn(projectGrs);
		when(projectGrs.isEmpty()).thenReturn(false);
		when(projectGrs.get(0)).thenReturn("id");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<Files> files = Mockito.mockStatic(Files.class);
				MockedStatic<Paths> paths = Mockito.mockStatic(Paths.class);
				MockedStatic<SmssUtilities> smssUtil = Mockito.mockStatic(SmssUtilities.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IProject project = mock(IProject.class);
			utility.when(() -> Utility.getProject("id")).thenReturn(project);
			when(project.getSmssFilePath()).thenReturn("Semoss.txt");

			Path p = mock(Path.class);
			paths.when(() -> Paths.get("Semoss.txt")).thenReturn(p);
			files.when(() -> Files.exists(p)).thenReturn(true);
			files.when(() -> Files.isRegularFile(p)).thenReturn(true);

			URI mockUri = mock(URI.class);
			when(p.toUri()).thenReturn(mockUri);
			paths.when(() -> Paths.get(mockUri)).thenReturn(p);

			String test = "test";
			files.when(() -> Files.readAllBytes(p)).thenReturn(test.getBytes());
			smssUtil.when(() -> SmssUtilities.concealSmssSensitiveInfo(test)).thenReturn("test2");

			NounMetadata nm = reactor.execute();
			assertEquals("test2", nm.getValue().toString());
			assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		}
	}

}
