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
package prerna.reactor.agent.skill;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

/**
 * Lists the files that make up one skill project's content, so a caller can see
 * what the skill ships before pulling any of it into context.
 *
 * <p>
 * This is the skill-shaped view over the existing asset reactors rather than
 * new file handling: discovery is {@link FileSystemUtil#search} (the same
 * recursive walk {@code SearchAppAssets} uses, so hidden assets are skipped the
 * same way) rooted at the skill's content folder, and each header is read with
 * {@link FileSystemUtil#getAssetAsString} and parsed by
 * {@link Skill#parseFrontmatter}. What it adds over
 * {@code BrowseAppAssets}/{@code SearchAppAssets} is the skill contract: paths
 * are relative to the skill folder rather than to {@code version/assets}, so a
 * caller never has to know the project layout, and every row carries the
 * {@code name}/{@code description} declared at the top of the file.
 *
 * <p>
 * Returns one row per file: {@code filePath}, {@code name}, and
 * {@code description}. {@code name} and {@code description} come from the YAML
 * frontmatter at the start of the file; a file without frontmatter (a helper
 * script, a data file) reports its own file name and an empty description. Feed
 * a returned {@code filePath} to {@link ReadSkillFileReactor} for that file's
 * full contents.
 *
 * <p>
 * Note this is not {@link ListSkillsReactor}, which scans a working directory
 * for {@code .claude/skills/<name>/SKILL.md} style folders and so does not see
 * a skill project's own content folder.
 *
 * <p>
 * {@code project} is optional and defaults to the insight's context project,
 * which is what a project-scoped MCP tool call already carries.
 */
public class ListSkillFilesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListSkillFilesReactor.class);

	/** Matches every entry name - we want the whole skill folder, not a search. */
	private static final Pattern ALL_ENTRIES = Pattern.compile(".");

	/** Files larger than this are listed without reading a header off the front. */
	private static final long MAX_HEADER_BYTES = 256 * 1024;

	public ListSkillFilesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String projectId = resolveContextEngineId(this.keyValue.get(this.keysToGet[0]));
		// editors/owners can read everything; view-only users are confined to the
		// public folder - which is where skill content lives
		boolean canEdit = SecurityProjectUtils.userCanEditProject(user, projectId);
		if (!canEdit && !SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Skill " + projectId + " does not exist or user does not have access to view it.");
		}
		IProject project = Utility.getProject(projectId);

		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getEngineId(), project.getEngineName());
		// confine view-only users to the public folder (throws if outside it)
		String skillFolder = FileSystemUtil.resolveReadableAssetPath(canEdit,
				SkillProjects.contentSubfolder(projectId));

		File skillDir = new File(assetFolder + skillFolder);
		if (!skillDir.isDirectory()) {
			throw new IllegalArgumentException("Skill " + projectId + " has no '" + skillFolder
					+ "' content folder - it may not be a skill project.");
		}

		List<Map<String, Object>> entries = FileSystemUtil.search(user, skillDir, ALL_ENTRIES,
				skillDir.getAbsolutePath().length() + 1);

		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map<String, Object> entry : entries) {
			if ("directory".equals(entry.get("type"))) {
				continue;
			}
			String relativePath = (String) entry.get("path");
			Skill.Frontmatter fm = readHeader(assetFolder, skillFolder + "/" + relativePath);

			Map<String, Object> row = new LinkedHashMap<>();
			row.put("filePath", relativePath);
			row.put("name", isEmpty(fm.name) ? entry.get("name") : fm.name);
			row.put("description", isEmpty(fm.description) ? "" : fm.description);
			rows.add(row);
		}

		classLogger.info("ListSkillFilesReactor: found {} file(s) for skill '{}' under '{}'", rows.size(), projectId,
				skillFolder);
		return new NounMetadata(rows, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	/**
	 * Reads the {@code name}/{@code description} declared at the top of one file.
	 * Never throws: a file that is unreadable, oversized, or simply has no
	 * frontmatter still belongs in the listing, it just has no header of its own.
	 */
	private static Skill.Frontmatter readHeader(String assetFolder, String assetRelativePath) {
		try {
			if (new File(assetFolder + assetRelativePath).length() > MAX_HEADER_BYTES) {
				return new Skill.Frontmatter();
			}
			return Skill.parseFrontmatter(FileSystemUtil.getAssetAsString(assetFolder, assetRelativePath));
		} catch (Exception e) {
			classLogger.warn("ListSkillFilesReactor: could not read the header of '{}': {}", assetRelativePath,
					e.getMessage());
			return new Skill.Frontmatter();
		}
	}

	private static boolean isEmpty(String s) {
		return s == null || s.trim().isEmpty();
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Lists the files that make up a skill's content, one row per file with filePath (relative to the "
				+ "skill folder), name, and description as declared in the YAML frontmatter at the top of the file "
				+ "(files without frontmatter report their file name and an empty description). Pass a returned "
				+ "filePath to ReadSkillFile to get that file's full contents. Defaults to the current project "
				+ "when 'project' is not supplied.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Optional. Id of the skill project to list. Defaults to the current project context. "
					+ "Requires view access to the project.";
		}
		return super.getDescriptionForKey(key);
	}
}
