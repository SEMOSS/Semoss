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
package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import prerna.reactor.project.CreateProjectReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiSemossTestProjectUtils {
	
	private static Path PROJECT_CONFIG_FILE = Paths.get(ApiTestsSemossConstants.TEST_CONFIG_DIRECTORY.toString(), "projects.txt");
	private static List<String> CURRENT_PROJECTS = new ArrayList<>();
	private static List<String> CORE_PROJECTS = null;
	
	@SuppressWarnings("unchecked")
	public static String createProject(String projectName) {
		assertFalse(CURRENT_PROJECTS.contains(projectName));
		assertNotNull(projectName);
		String pixel = ApiSemossTestUtils.buildPixelCall(CreateProjectReactor.class, "project", projectName);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		CURRENT_PROJECTS.add(projectName);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String projectId = ret.get("project_id").toString();
		return projectId;
	}

	public static void clearNonCoreProjects() throws IOException {
		List<String> projectsToAvoid = getProjectsToAvoid();
		Path p = Paths.get(ApiTestsSemossConstants.TEST_PROJECT_DIRECTORY);
		if(Files.notExists(p)) {
			Files.createDirectory(p);
			}
		File f = p.toFile();
		List<String> toDelete = new ArrayList<>();
		for (String s : f.list()) {
			boolean found = false;
			for (String c : projectsToAvoid) {
				if (s.toLowerCase().startsWith(c.toLowerCase())) {
					found = true;
					break;
				}
			}
			if (!found) {
				toDelete.add(s);
			}
		}
		
		doClearProject(toDelete);
	}

	private static void doClearProject(List<String> toDelete) throws IOException {
		for (String delete : toDelete) {
			Path p = Paths.get(ApiTestsSemossConstants.TEST_PROJECT_DIRECTORY.toString(), delete);
			if (Files.isDirectory(p)) {
				FileUtils.cleanDirectory(p.toFile());
				Files.delete(p);
			} else {
				Files.delete(p);
			}
		}
	}

	private static List<String> getProjectsToAvoid() throws IOException {
		if (CORE_PROJECTS != null) {
			return CORE_PROJECTS;
		}
		
		CORE_PROJECTS = Files.readAllLines(PROJECT_CONFIG_FILE).stream().map(s -> s.trim()).filter(s -> !s.isEmpty())
				.collect(Collectors.toList());
		return CORE_PROJECTS;
	}

}
