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

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRDBMSEngine;

/**
 * Reconciles a platform project's security-database row with the smss file on
 * disk, at boot.
 *
 * <p>
 * {@code SecurityProjectUtils.addProject} early-returns once a project row
 * exists, so anything it writes is fixed at first insert: a display name
 * corrected in the smss, a tag added to a later build, or a template flag
 * introduced after the row was created never reaches an installation that has
 * already booted once. Each method here re-applies one of those values from the
 * file, writes only when it differs, and never throws - a failed repair must
 * not stop the project loading.
 *
 * <p>
 * These operations bypass the ownership checks in {@link SecurityProjectUtils}
 * because a platform project has no owner to check against; they are correct
 * only for the {@code platform__} projects the boot thread owns. The class is
 * deliberately package private so nothing outside {@code prerna.util} - in
 * particular no reactor - can reach it. {@link ProjectWatcher} is the intended
 * and only caller.
 */
class SystemProjectSeeder {

	private static final Logger classLogger = LogManager.getLogger(SystemProjectSeeder.class);

	private SystemProjectSeeder() {
	}

	/**
	 * Re-applies every value this class owns for one platform project.
	 *
	 * @param projectId    platform project id
	 * @param smssPath     absolute path to the project's smss file
	 * @param requiredTags PROJECTMETA tags the project must carry
	 */
	static void seed(String projectId, String smssPath, String... requiredTags) {
		Properties smss = loadSmss(projectId, smssPath);
		ensureGlobal(projectId);
		ensureTags(projectId, requiredTags);
		if (smss != null) {
			ensureDisplayName(projectId, smss);
			ensureTemplateFlag(projectId, smss);
		}
	}

	/**
	 * Forces the project completely global.
	 *
	 * <p>
	 * The global flag passed to {@code addProject} only lands on the initial
	 * insert. A platform project whose row was created by any other path first -
	 * the generic folder scan, or a boot before it was registered as a system
	 * project - stays non-global and drops out of MyProjects, so this is re-applied
	 * every boot.
	 *
	 * @param projectId platform project id
	 */
	static void ensureGlobal(String projectId) {
		try {
			SecurityProjectUtils.setProjectCompletelyGlobal(projectId);
		} catch (Exception e) {
			classLogger.warn("Failed to set platform project '{}' global: {}", projectId, e.getMessage());
		}
	}

	/**
	 * Adds any missing PROJECTMETA tag (e.g. "SKILL", "MCP", "SYSTEM"), preserving
	 * tags already present. The literal "MCP" tag matches
	 * {@code MCPUtility.addMCPTag}.
	 *
	 * @param projectId    platform project id
	 * @param requiredTags tags the project must carry
	 */
	static void ensureTags(String projectId, String... requiredTags) {
		if (requiredTags == null || requiredTags.length == 0) {
			return;
		}
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
			boolean changed = false;
			for (String required : requiredTags) {
				if (!tags.contains(required)) {
					tags.add(required);
					changed = true;
				}
			}
			if (changed) {
				Map<String, Object> update = new HashMap<>();
				update.put("tag", tags);
				SecurityProjectUtils.updateProjectMetadata(projectId, update);
			}
		} catch (Exception e) {
			classLogger.warn("Failed to ensure tags {} on platform project '{}': {}", Arrays.toString(requiredTags),
					projectId, e.getMessage());
		}
	}

	/**
	 * Re-applies PROJECT_DISPLAY_NAME from the smss.
	 *
	 * <p>
	 * Platform projects are renamed by editing their smss, and the security db row
	 * is the value every catalog screen reads, so without this a rename in the file
	 * is invisible on an installation that has already booted. A platform project
	 * has no owner, so this writes directly rather than going through
	 * {@code SecurityProjectUtils.setProjectDisplayName}, which requires one.
	 *
	 * @param projectId platform project id
	 * @param smss      loaded smss properties
	 */
	static void ensureDisplayName(String projectId, Properties smss) {
		try {
			String displayName = smss.getProperty(Constants.PROJECT_DISPLAY_NAME);
			if (displayName == null || displayName.trim().isEmpty()) {
				return;
			}
			displayName = displayName.trim();
			if (displayName.equals(SecurityProjectUtils.getProjectDisplayNameForId(projectId))) {
				return;
			}
			IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement("UPDATE PROJECT SET PROJECTDISPLAYNAME=? WHERE PROJECTID=?");
				ps.setString(1, displayName);
				ps.setString(2, projectId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
			classLogger.info("Updated platform project '{}' display name to '{}' from its smss.", projectId,
					displayName);
		} catch (Exception e) {
			classLogger.warn("Failed to ensure display name on platform project '{}': {}", projectId, e.getMessage());
		}
	}

	/**
	 * Re-applies IS_TEMPLATE from the smss.
	 *
	 * <p>
	 * One-directional on purpose: an smss without {@code IS_TEMPLATE=true} leaves
	 * the db value alone, so a flag enabled at runtime through the REST endpoint is
	 * never clobbered.
	 *
	 * @param projectId platform project id
	 * @param smss      loaded smss properties
	 */
	static void ensureTemplateFlag(String projectId, Properties smss) {
		try {
			if (Boolean.parseBoolean(smss.getProperty(Constants.IS_TEMPLATE, "false"))) {
				SecurityProjectUtils.setProjectTemplate(projectId, true);
			}
		} catch (Exception e) {
			classLogger.warn("Failed to ensure template flag on platform project '{}': {}", projectId, e.getMessage());
		}
	}

	private static Properties loadSmss(String projectId, String smssPath) {
		try {
			return Utility.loadProperties(smssPath);
		} catch (Exception e) {
			classLogger.warn("Failed to read smss for platform project '{}' at {}: {}", projectId, smssPath,
					e.getMessage());
			return null;
		}
	}
}
