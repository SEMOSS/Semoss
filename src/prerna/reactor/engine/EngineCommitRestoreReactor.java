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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.lib.ObjectId;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * This reactor reverts an engine's git repository to a previous commit by
 * creating a new commit that restores the file state of the target commit.
 * Uses the reset --hard / reset --soft pattern to avoid JGit path matching
 * issues with checkout.
 */
public class EngineCommitRestoreReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineCommitRestoreReactor.class);
	private static final String COMMIT_ID_KEY = "commitId";

	public EngineCommitRestoreReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), COMMIT_ID_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();

		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			classLogger.error("Unauthorized access: you must be logged in to perform this action");
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String commitId = this.keyValue.get(COMMIT_ID_KEY);

		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the engine id");
		}
		if (commitId == null || (commitId = commitId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the commit id");
		}

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to the engine");
		}

		IEngine engine = Utility.getEngine(engineId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(
				engine.getCatalogType(), engineId, engine.getEngineName());

		try (Git thisGit = Git.open(new File(versionFolder))) {
			ObjectId commitObjectId = thisGit.getRepository().resolve(commitId);
			if (commitObjectId == null) {
				throw new IllegalArgumentException("Commit id " + commitId + " not found");
			}

			// Save the current HEAD so we can soft-reset back to it
			ObjectId originalHead = thisGit.getRepository().resolve("HEAD");

			// Step 1: Hard reset to the target commit
			// This sets HEAD, index, AND working tree to the target commit's state
			thisGit.reset().setMode(ResetType.HARD).setRef(commitObjectId.name()).call();

			// Step 2: Soft reset back to the original HEAD
			// This moves HEAD back but keeps index and working tree at the target state
			// Now the index differs from HEAD = ready to commit
			thisGit.reset().setMode(ResetType.SOFT).setRef(originalHead.name()).call();

			// Step 3: Commit the staged changes (index has target state, HEAD has original)
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			String author = accessToken.getUsername();
			String email = accessToken.getEmail();
			if (author == null || author.isEmpty()) {
				author = "SEMOSS";
			}
			if (email == null || email.isEmpty()) {
				email = "semoss@semoss.org";
			}

			thisGit.commit()
					.setMessage("Reverted to commit: " + commitId)
					.setAuthor(author, email)
					.call();

			classLogger.info("Reverted engine {} to commit {}", engineId, commitId);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error reverting engine {} to commit {}", engineId, commitId, e);
			throw new IllegalArgumentException("Unable to revert to commit id " + commitId, e);
		}

		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushEngineFolder(engine, versionFolder);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor reverts an engine to the requested commit id";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id to revert";
		} else if (key.equals(COMMIT_ID_KEY)) {
			return "The commit id to revert to";
		}
		return super.getDescriptionForKey(key);
	}

}
