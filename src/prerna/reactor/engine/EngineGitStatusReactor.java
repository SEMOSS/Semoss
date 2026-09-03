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
package prerna.reactor.engine;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.ProjectGitStatusUtils;

/**
 * Returns the git working-tree status (branch, HEAD, staged/unstaged/
 * untracked/conflicted files) for an engine's repository.
 */
public class EngineGitStatusReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineGitStatusReactor.class);

	public EngineGitStatusReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the engine id");
		}

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new SemossPixelException("Engine does not exist or user does not have access to the engine");
		}

		IEngine engine = Utility.getEngine(engineId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(), engineId,
				engine.getEngineName());

		Map<String, Object> statusMap;
		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) {
			classLogger.info("No git repository found for engine {}", engineId);
			statusMap = EngineGitReactorUtils.buildStatusMap(engineId, ProjectGitStatusUtils.emptyStatus());
		} else {
			try (Git thisGit = Git.open(new File(versionFolder))) {
				statusMap = EngineGitReactorUtils.buildStatusMap(engineId,
						ProjectGitStatusUtils.computeStatus(thisGit.getRepository()));
			} catch (Exception e) {
				classLogger.error("Error occurred getting git status for engine {}", engineId, e);
				throw new SemossPixelException(
						"Error occurred getting the git status. Detailed error = " + e.getMessage(), e);
			}
		}

		return new NounMetadata(statusMap, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the git working tree status for an engine";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		}
		return super.getDescriptionForKey(key);
	}
}
