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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

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

/**
 * This reactor returns paginated commit details from an engine's git repository.
 * Each commit includes the commit SHA, author information, date, commit message,
 * and any tags pointing to that commit.
 */
public class EngineCommitDetailsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineCommitDetailsReactor.class);

	public EngineCommitDetailsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());

		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the engine id");
		}
		if (limitStr == null || (limitStr = limitStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the limit");
		}
		if (offsetStr == null || (offsetStr = offsetStr.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the offset");
		}

		int limit;
		int offset;

		try {
			limit = Integer.parseInt(limitStr);
			if (limit < 1) {
				throw new SemossPixelException("Limit is a valid integer but must be >= 1");
			}
		} catch (NumberFormatException nfe) {
			throw new SemossPixelException("Limit must be a valid integer");
		}

		try {
			offset = Integer.parseInt(offsetStr);
			if (offset < 0) {
				throw new SemossPixelException("Offset is a valid integer but must be >= 0");
			}
		} catch (NumberFormatException nfe) {
			throw new SemossPixelException("Offset must be a valid integer");
		}

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new SemossPixelException("Engine does not exist or user does not have access to the engine");
		}

		IEngine engine = Utility.getEngine(engineId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(
				engine.getCatalogType(), engineId, engine.getEngineName());

		List<Map<String, Object>> commits = new ArrayList<>();

		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) {
			classLogger.info("No git repository found for engine {}", engineId);
			return new NounMetadata(commits, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
		}

		try (Git thisGit = Git.open(new File(versionFolder))) {
			if (thisGit.getRepository().resolve("HEAD") == null) {
				classLogger.info("Git repository has no commits for engine {}", engineId);
				return new NounMetadata(commits, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
			}

			List<Ref> tagList = thisGit.tagList().call();
			Iterable<RevCommit> gitCommits = thisGit.log().call();

			for (RevCommit commit : gitCommits) {
				Map<String, Object> details = new LinkedHashMap<>();
				details.put("commitId", commit.getName());

				Map<String, String> authorDetails = new LinkedHashMap<>();
				authorDetails.put("userId", commit.getAuthorIdent().getName());
				authorDetails.put("userEmail", commit.getAuthorIdent().getEmailAddress());
				details.put("author", authorDetails);
				details.put("date", commit.getAuthorIdent().getWhen().toString());
				details.put("commitMessage", commit.getFullMessage());

				List<String> tagsForCommit = new ArrayList<>();
				try (RevWalk walk = new RevWalk(thisGit.getRepository())) {
					for (Ref tag : tagList) {
						RevCommit taggedCommit = walk
								.parseCommit(thisGit.getRepository().getRefDatabase().peel(tag).getObjectId());
						if (taggedCommit.equals(commit)) {
							tagsForCommit.add(tag.getName().replace("refs/tags/", ""));
						}
					}
				}
				details.put("tags", tagsForCommit);
				commits.add(details);
			}

		} catch (Exception e) {
			classLogger.error("Error occurred getting commit details for engine {}", engineId, e);
			throw new SemossPixelException(
					"Error occurred getting the commit details. Detailed error = " + e.getMessage(), e);
		}

		int totalCommits = commits.size();
		int toIndex = Math.min(offset + limit, totalCommits);

		return new NounMetadata(commits.subList(offset, toIndex), PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the details of all the commits in an engine";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum number of commits to return";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Number of commits to skip for pagination";
		}
		return super.getDescriptionForKey(key);
	}

}
