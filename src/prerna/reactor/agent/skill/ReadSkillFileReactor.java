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

import java.util.HashMap;
import java.util.Map;

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
 * Returns the entire contents of one file inside a skill's content folder. The
 * companion to {@link ListSkillFilesReactor}: list first, then read only the
 * file you need.
 *
 * <p>
 * The read itself is {@code GetAppAssets} logic reused verbatim -
 * {@link Utility#normalizePath}, then
 * {@link FileSystemUtil#resolveReadableAssetPath} for the view-only guard, then
 * {@link FileSystemUtil#getAssetAsString}. What this adds is the skill
 * contract: {@code filePath} is relative to the skill's content folder (exactly
 * what {@code ListSkillFiles} reports, so {@code SKILL.md} and
 * {@code references/palette.md} both work) rather than to
 * {@code version/assets}, so a caller never has to know the project layout.
 *
 * <p>
 * {@code project} is optional and defaults to the insight's context project,
 * which is what a project-scoped MCP tool call already carries.
 */
public class ReadSkillFileReactor extends AbstractReactor {

	public ReadSkillFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 0, 1 };
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

		String filePath = this.keyValue.get(this.keysToGet[1]);
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass the filePath of the skill file to read");
		}
		// the caller's path is relative to the skill folder, not to version/assets
		filePath = SkillProjects.contentSubfolder(projectId) + "/" + filePath.replace("\\", "/");
		filePath = Utility.normalizePath(filePath);
		// confine view-only users to the public folder (throws if outside it)
		filePath = FileSystemUtil.resolveReadableAssetPath(canEdit, filePath);

		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getEngineId(), project.getEngineName());

		String output = FileSystemUtil.getAssetAsString(assetFolder, filePath);

		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
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
		return "Returns the entire contents of one file in a skill's content folder. Pass a filePath exactly as "
				+ "ListSkillFiles reports it, for example 'SKILL.md' or 'references/palette.md'. Defaults to the "
				+ "current project when 'project' is not supplied.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.FILE_PATH.getKey().equals(key)) {
			return "Path of the file to read, relative to the skill's content folder - the same value "
					+ "ListSkillFiles returns (for example 'SKILL.md' or 'references/palette.md').";
		}
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Optional. Id of the skill project the file belongs to. Defaults to the current project "
					+ "context. Requires view access to the project.";
		}
		return super.getDescriptionForKey(key);
	}
}
