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
package prerna.reactor.project;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * This reactor returns paginated commit details from a project's git
 * repository. Each commit includes the commit SHA, author information, date,
 * commit message, parent commit ids, and any refs (local branches, remote
 * branches, tags) pointing to that commit.
 */
public class ProjectCommitDetailsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectCommitDetailsReactor.class);

	public ProjectCommitDetailsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the project id");
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

		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new SemossPixelException("Project does not exist or user does not have access to the project");
		}

		List<Map<String, Object>> commits = new ArrayList<>();

		IProject project = Utility.getProject(projectId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				project.getEngineName());

		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) {
			classLogger.info("No git repository found for project {}", projectId);
			return new NounMetadata(commits, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
		}

		try (Git thisGit = Git.open(new File(versionFolder))) {
			if (thisGit.getRepository().resolve("HEAD") == null) {
				classLogger.info("Git repository has no commits for project {}", projectId);
				return new NounMetadata(commits, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
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

				List<String> parentIds = new ArrayList<>();
				for (int p = 0; p < commit.getParentCount(); p++) {
					parentIds.add(commit.getParent(p).getName());
				}
				details.put("parentCommitIds", parentIds);

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

			int totalCommits = commits.size();
			int toIndex = Math.min(offset + limit, totalCommits);
			List<Map<String, Object>> page = commits.subList(offset, toIndex);

			// resolve refs (local branches, remote branches, tags) only for the
			// paginated page, so this cost scales with the page size rather than
			// the full commit history
			Set<String> pageShas = new HashSet<>();
			for (Map<String, Object> details : page) {
				pageShas.add((String) details.get("commitId"));
			}

			Map<String, List<Map<String, String>>> refsBySha = new HashMap<>();

			for (Ref localBranch : thisGit.branchList().call()) {
				// BranchListCommand includes a synthetic "HEAD" entry when HEAD is
				// detached - not a real branch, skip it.
				if (!localBranch.getName().startsWith(org.eclipse.jgit.lib.Constants.R_HEADS)) {
					continue;
				}
				addRefIfOnPage(refsBySha, pageShas, localBranch.getObjectId(),
						localBranch.getName().replace("refs/heads/", ""), "LOCAL_BRANCH");
			}
			for (Ref remoteBranch : thisGit.branchList().setListMode(ListBranchCommand.ListMode.REMOTE).call()) {
				addRefIfOnPage(refsBySha, pageShas, remoteBranch.getObjectId(),
						remoteBranch.getName().replace("refs/remotes/", ""), "REMOTE_BRANCH");
			}
			try (RevWalk walk = new RevWalk(thisGit.getRepository())) {
				for (Ref tag : tagList) {
					RevCommit taggedCommit = walk
							.parseCommit(thisGit.getRepository().getRefDatabase().peel(tag).getObjectId());
					addRefIfOnPage(refsBySha, pageShas, taggedCommit, tag.getName().replace("refs/tags/", ""), "TAG");
				}
			}

			for (Map<String, Object> details : page) {
				List<Map<String, String>> refs = refsBySha.getOrDefault(details.get("commitId"), new ArrayList<>());
				refs.sort(Comparator.comparing(r -> r.get("name")));
				details.put("refs", refs);
			}

			return new NounMetadata(page, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
		} catch (Exception e) {
			classLogger.error("Error occurred getting commit details for project {}", projectId, e);
			throw new SemossPixelException(
					"Error occurred getting the commit details. Detailed error = " + e.getMessage(), e);
		}
	}

	/**
	 * Records a ref entry keyed by its target commit's SHA, but only when that SHA
	 * is one of the commits in the current paginated page.
	 */
	private void addRefIfOnPage(Map<String, List<Map<String, String>>> refsBySha, Set<String> pageShas,
			org.eclipse.jgit.lib.AnyObjectId target, String name, String type) {
		if (target == null) {
			return;
		}
		String sha = target.getName();
		if (!pageShas.contains(sha)) {
			return;
		}
		Map<String, String> refInfo = new LinkedHashMap<>();
		refInfo.put("name", name);
		refInfo.put("type", type);
		refsBySha.computeIfAbsent(sha, k -> new ArrayList<>()).add(refInfo);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the details of all the commits in a project";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The project id";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum number of commits to return";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Number of commits to skip for pagination";
		}
		return super.getDescriptionForKey(key);
	}

}