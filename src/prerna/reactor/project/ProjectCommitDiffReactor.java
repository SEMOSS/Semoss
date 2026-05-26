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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.RenameDetector;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;

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
 * This reactor returns the list of changed files for a specific commit in a
 * project's git repository, with optional unified diff text for a single file.
 * 
 * <p>When called without a filePath, it returns a summary of all changed files
 * and their change types (ADD, MODIFY, DELETE, RENAME, COPY). When called with
 * a filePath, it additionally returns the unified diff text for that specific
 * file, along with flags indicating if the file is binary or if the diff was
 * truncated due to size.</p>
 * 
 * <p>For merge commits, the diff is computed against the first parent. For the
 * initial commit (no parent), the diff is computed against an empty tree.</p>
 */
public class ProjectCommitDiffReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectCommitDiffReactor.class);
	private static final String COMMIT_ID_KEY = "commitId";
	private static final String FILE_PATH_KEY = "filePath";
	private static final int MAX_DIFF_LINES = 10000;

	public ProjectCommitDiffReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), COMMIT_ID_KEY, FILE_PATH_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String commitId = this.keyValue.get(COMMIT_ID_KEY);
		String filePath = this.keyValue.get(FILE_PATH_KEY);
		if (filePath != null) {
			filePath = filePath.trim();
			if (filePath.isEmpty()) {
				filePath = null;
			}
		}

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the project id");
		}
		if (commitId == null || (commitId = commitId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the commit id");
		}

		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new SemossPixelException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(
				IEngine.CATALOG_TYPE.PROJECT, projectId, project.getEngineName());

		List<Map<String, Object>> changedFiles = new ArrayList<>();

		try (Git thisGit = Git.open(new File(versionFolder))) {
			Repository repo = thisGit.getRepository();

			ObjectId commitObjectId = repo.resolve(commitId);
			if (commitObjectId == null) {
				classLogger.error("Commit not found for project {} with commitId {}", projectId, commitId);
				throw new SemossPixelException("Commit not found: " + commitId);
			}

			try (RevWalk revWalk = new RevWalk(repo)) {
				RevCommit commit = revWalk.parseCommit(commitObjectId);
				classLogger.info("Retrieving diff for commit {} in project {}", commitId, projectId);

				AbstractTreeIterator parentTreeIter = getParentTreeIterator(revWalk, repo, commit);
				AbstractTreeIterator commitTreeIter = prepareTreeParser(repo, commit);

				changedFiles = computeDiffs(repo, parentTreeIter, commitTreeIter, filePath);
			}
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error getting commit diff for project {} and commit {}", projectId, commitId, e);
			throw new SemossPixelException("Error getting commit diff. Detailed error = " + e.getMessage(), e);
		}

		return new NounMetadata(changedFiles, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
	}

	/**
	 * This method returns the parent tree iterator for a given commit. For the
	 * initial commit (no parents), an empty tree iterator is returned. For merge
	 * commits, the first parent is used.
	 * 
	 * @param revWalk
	 * @param repo
	 * @param commit
	 * @return
	 * @throws Exception
	 */
	private AbstractTreeIterator getParentTreeIterator(RevWalk revWalk, Repository repo, RevCommit commit)
			throws Exception {
		if (commit.getParentCount() > 0) {
			RevCommit parentCommit = revWalk.parseCommit(commit.getParent(0).getId());
			return prepareTreeParser(repo, parentCommit);
		}
		return new EmptyTreeIterator();
	}

	/**
	 * This method computes the diff entries between two tree iterators and builds
	 * the list of changed file metadata. When a filePath is provided, the unified
	 * diff text is included for that specific file.
	 * 
	 * @param repo
	 * @param parentTreeIter
	 * @param commitTreeIter
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	private List<Map<String, Object>> computeDiffs(Repository repo, AbstractTreeIterator parentTreeIter,
			AbstractTreeIterator commitTreeIter, String filePath) throws IOException {
		List<Map<String, Object>> changedFiles = new ArrayList<>();

		try (ByteArrayOutputStream diffOutputStream = new ByteArrayOutputStream();
				DiffFormatter diffFormatter = new DiffFormatter(diffOutputStream)) {

			diffFormatter.setRepository(repo);
			diffFormatter.setDetectRenames(true);

			List<DiffEntry> diffs = diffFormatter.scan(parentTreeIter, commitTreeIter);

			RenameDetector renameDetector = new RenameDetector(repo);
			renameDetector.addAll(diffs);
			diffs = renameDetector.compute();

			for (DiffEntry entry : diffs) {
				String entryNewPath = entry.getNewPath();
				String entryOldPath = entry.getOldPath();
				String changeType = entry.getChangeType().name();

				String displayPath = DiffEntry.DEV_NULL.equals(entryNewPath) ? entryOldPath : entryNewPath;

				if (filePath != null && !displayPath.equals(filePath)) {
					continue;
				}

				Map<String, Object> fileInfo = new LinkedHashMap<>();
				fileInfo.put("fileName", displayPath);
				fileInfo.put("changeType", changeType);
				fileInfo.put("oldPath", DiffEntry.DEV_NULL.equals(entryOldPath) ? null : entryOldPath);
				fileInfo.put("newPath", DiffEntry.DEV_NULL.equals(entryNewPath) ? null : entryNewPath);

				if (filePath != null) {
					addDiffText(diffFormatter, diffOutputStream, entry, fileInfo, displayPath);
				}

				changedFiles.add(fileInfo);
			}
		}

		return changedFiles;
	}

	/**
	 * This method formats and adds the unified diff text for a single file entry.
	 * It handles binary detection and truncation for large diffs.
	 * 
	 * @param diffFormatter
	 * @param diffOutputStream
	 * @param entry
	 * @param fileInfo
	 * @param displayPath
	 */
	private void addDiffText(DiffFormatter diffFormatter, ByteArrayOutputStream diffOutputStream,
			DiffEntry entry, Map<String, Object> fileInfo, String displayPath) {
		boolean isBinary = false;
		boolean isTruncated = false;
		String diffText = "";

		try {
			diffOutputStream.reset();
			diffFormatter.format(entry);
			diffText = diffOutputStream.toString("UTF-8");

			if (diffText.contains("Binary files differ")) {
				isBinary = true;
				diffText = "";
			}

			long lineCount = diffText.lines().count();
			if (lineCount > MAX_DIFF_LINES) {
				classLogger.info("Truncating diff for file {} ({} lines exceeds max {})",
						displayPath, lineCount, MAX_DIFF_LINES);
				isTruncated = true;
				diffText = diffText.lines()
						.limit(MAX_DIFF_LINES)
						.collect(Collectors.joining("\n"));
			}
		} catch (Exception e) {
			classLogger.warn("Could not format diff for file {}", displayPath, e);
			isBinary = true;
		}

		fileInfo.put("diff", diffText);
		fileInfo.put("isBinary", isBinary);
		fileInfo.put("isTruncated", isTruncated);
	}

	/**
	 * This method prepares a tree parser for a given commit so its tree can be
	 * compared in a diff operation.
	 * 
	 * @param repo
	 * @param commit
	 * @return
	 * @throws IOException
	 */
	private static AbstractTreeIterator prepareTreeParser(Repository repo, RevCommit commit) throws IOException {
		try (ObjectReader reader = repo.newObjectReader()) {
			CanonicalTreeParser treeParser = new CanonicalTreeParser();
			treeParser.reset(reader, commit.getTree().getId());
			return treeParser;
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the list of changed files for a specific commit, "
				+ "with optional unified diff text for a single file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The project id";
		} else if (key.equals(COMMIT_ID_KEY)) {
			return "The commit SHA to inspect";
		} else if (key.equals(FILE_PATH_KEY)) {
			return "Optional file path to get diff text for a specific file";
		}
		return super.getDescriptionForKey(key);
	}
}
