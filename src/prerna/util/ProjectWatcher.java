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
package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.impl.ProjectHelper;

public class ProjectWatcher extends AbstractFileWatcher {

	private static final Logger classLogger = LogManager.getLogger(ProjectWatcher.class);

	private static List<String> INIT_LIST = new ArrayList<>();

	@Override
	public void process(String fileName) {
		catalogProject(fileName, folderToWatch);
	}

	@Override
	public void init() {
		// we will load the platform skills
		List<String> defaultPlatforms = SystemDefaultEngines.getSystemSkills();
		for (String engineId : defaultPlatforms) {
			// find the local master
			String fileName = engineId + this.extension;
			if (new File(folderToWatch + "/platform__" + fileName).exists()) {
				try {
					// set all as global
					catalogProject("platform__" + fileName, folderToWatch, true);
					INIT_LIST.add("platform__" + fileName);
					ensureSkillTag(engineId);
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the {}", engineId, e);
					continue;
				}
			}
		}

		// loading platform mcps (headless system apps, no UI)
		List<String> defaultMCPs = SystemDefaultEngines.getSystemMCPs();
		for (String engineId : defaultMCPs) {
			String fileName = engineId + this.extension;
			if (new File(folderToWatch + "/platform__" + fileName).exists()) {
				try {
					catalogProject("platform__" + fileName, folderToWatch, true);
					INIT_LIST.add("platform__" + fileName);
					SecurityProjectUtils.setProjectCompletelyGlobal(engineId);
					ensureMCPTag(engineId);
				} catch (Exception e) {
					classLogger.error("Failed to load and initialize the {}", engineId, e);
					continue;
				}
			}
		}
	}

	/**
	 * Makes sure a platform skill project carries the PROJECTMETA tag marking it as
	 * a skill. addProject early-returns when the project already exists in the
	 * security db, so this runs on every boot; it is idempotent and preserves any
	 * other tag values already on the project. Never blocks project load.
	 */
	private static void ensureSkillTag(String projectId) {
		try {
			Map<String, Object> meta = SecurityProjectUtils.getAggregateProjectMetadata(projectId, Arrays.asList("tag"),
					false);
			List<Object> tags = new ArrayList<>();
			Object existing = meta.get("tag");
			if (existing instanceof List) {
				tags.addAll((List<?>) existing);
			} else if (existing != null) {
				tags.add(existing);
			}
			for (Object t : tags) {
				if (ProjectHelper.SKILL_PROJECT_TAG.equals(t)) {
					return;
				}
			}
			tags.add(ProjectHelper.SKILL_PROJECT_TAG);
			Map<String, Object> update = new HashMap<>();
			update.put("tag", tags);
			SecurityProjectUtils.updateProjectMetadata(projectId, update);
		} catch (Exception e) {
			classLogger.warn("Failed to ensure skill tag on platform skill project '{}': {}", projectId,
					e.getMessage());
		}
	}

	/**
	 * Makes sure a platform mcp project carries the PROJECTMETA tag marking it as an
	 * mcp. Mirrors {@link #ensureSkillTag(String)}: addProject early-returns when the
	 * project already exists in the security db, so this runs on every boot; it is
	 * idempotent and preserves any other tag values already on the project. Never
	 * blocks project load. The literal "MCP" tag matches MCPUtility.addMCPTag.
	 */
	private static void ensureMCPTag(String projectId) {
		try {
			Map<String, Object> meta = SecurityProjectUtils.getAggregateProjectMetadata(projectId, Arrays.asList("tag"),
					false);
			List<Object> tags = new ArrayList<>();
			Object existing = meta.get("tag");
			if (existing instanceof List) {
				tags.addAll((List<?>) existing);
			} else if (existing != null) {
				tags.add(existing);
			}
			for (Object t : tags) {
				if ("MCP".equals(t)) {
					return;
				}
			}
			tags.add("MCP");
			Map<String, Object> update = new HashMap<>();
			update.put("tag", tags);
			SecurityProjectUtils.updateProjectMetadata(projectId, update);
		} catch (Exception e) {
			classLogger.warn("Failed to ensure mcp tag on platform mcp project '{}': {}", projectId, e.getMessage());
		}
	}

	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst() {
		File dir = new File(folderToWatch);
		String[] fileNames = dir.list(this);
		if (fileNames == null || fileNames.length == 0) {
			return;
		}

		Set<String> projectIds = new HashSet<>(fileNames.length);
		// loop through and load all the projects
		for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
			try {
				String fileName = fileNames[fileIdx];
				if (INIT_LIST.contains(fileName)) {
					// ignore - we have already loaded these
					continue;
				}

				// we need to add projects to security db
				String loadedProject = catalogProject(fileName, folderToWatch);
				projectIds.add(loadedProject);
			} catch (RuntimeException ex) {
				classLogger.error("Project failed to load: {}/{}", folderToWatch, fileNames[fileIdx], ex);
			}
		}

		if (!ClusterUtil.IS_CLUSTER) {
			// reserved system apps (platform skills + platform mcps) reload from disk
			// every boot and must never be pruned during file-system reconciliation
			Set<String> reservedProjects = new HashSet<>(SystemDefaultEngines.getSystemSkills());
			reservedProjects.addAll(SystemDefaultEngines.getSystemMCPs());
			// if projects are removed from the file system
			// remove them
			List<String> projects = SecurityProjectUtils.getAllProjectIds();
			for (String project : projects) {
				if (!projectIds.contains(project) && !reservedProjects.contains(project)) {
					SecurityProjectUtils.deleteProject(project);
				}
			}
		}
	}

	/**
	 * Loads a new project by setting a specific engine with associated properties.
	 * 
	 * @param Specifies properties to load
	 */
	public static String catalogProject(String newFile, String folderToWatch) {
		return catalogProject(newFile, folderToWatch, false);
	}

	/**
	 * Loads a new project by setting a specific engine with associated properties.
	 * 
	 * @param Specifies properties to load
	 */
	public static String catalogProject(String newFile, String folderToWatch, boolean global) {
		String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";
		FileInputStream fileIn = null;
		String projectId = null;
		try {
			Properties prop = new Properties();
			fileIn = new FileInputStream(Utility.normalizePath(folderToWatch) + "/" + Utility.normalizePath(newFile));
			prop.load(fileIn);

			projectId = prop.getProperty(Constants.PROJECT);

			if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId)) {
				classLogger.debug("Project {}<>{} is already loaded...", folderToWatch, newFile);
			} else {
				String fileName = folderToWatch + "/" + newFile;
				DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, fileName);

				String projectNames = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
				if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
						|| projects.endsWith(";" + projectId))) {
					projectNames = projectNames + ";" + projectId;
					DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectNames);
				}

				SecurityProjectUtils.addProject(projectId, global, null);
			}
		} catch (Exception e) {
			classLogger.error("Failed to catalog project from smss file {}/{}", folderToWatch, newFile, e);
		} finally {
			try {
				if (fileIn != null) {
					fileIn.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close input stream for smss file {}/{}", folderToWatch, newFile, e);
			}
		}

		return projectId;
	}

}
