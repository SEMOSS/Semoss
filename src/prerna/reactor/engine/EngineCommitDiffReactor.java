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
 * This reactor returns the list of changed files for a specific commit in an
 * engine's git repository, with optional unified diff text for a single file.
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
public class EngineCommitDiffReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EngineCommitDiffReactor.class);
	private static final String COMMIT_ID_KEY = "commitId";
	private static final String FILE_PATH_KEY = "filePath";
	private static final int MAX_DIFF_LINES = 10000;

	public EngineCommitDiffReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), COMMIT_ID_KEY, FILE_PATH_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String commitId = this.keyValue.get(COMMIT_ID_KEY);
		String filePath = this.keyValue.get(FILE_PATH_KEY);
		if (filePath != null) {
			filePath = filePath.trim();
			if (filePath.isEmpty()) {
				filePath = null;
			}
		}

		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the engine id");
		}
		if (commitId == null || (commitId = commitId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the commit id");
		}

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new SemossPixelException("Engine does not exist or user does not have access to the engine");
		}

		IEngine engine = Utility.getEngine(engineId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(
				engine.getCatalogType(), engineId, engine.getEngineName());

		List<Map<String, Object>> result = new ArrayList<>();

		try (Git thisGit = Git.open(new File(versionFolder))) {
			Repository repo = thisGit.getRepository();

			ObjectId commitObjectId = repo.resolve(commitId);
			if (commitObjectId == null) {
				classLogger.error("Commit not found for engine {} with commitId {}", engineId, commitId);
				throw new SemossPixelException("Commit not found: " + commitId);
			}

			try (RevWalk revWalk = new RevWalk(repo)) {
				RevCommit commit = revWalk.parseCommit(commitObjectId);
				classLogger.info("Retrieving diff for commit {} in engine {}", commitId, engineId);

				AbstractTreeIterator parentIterator;
				if (commit.getParentCount() > 0) {
					RevCommit parent = revWalk.parseCommit(commit.getParent(0).getId());
					parentIterator = prepareTreeParser(repo, parent);
				} else {
					parentIterator = new EmptyTreeIterator();
				}

				AbstractTreeIterator commitIterator = prepareTreeParser(repo, commit);

				try (DiffFormatter formatter = new DiffFormatter(new ByteArrayOutputStream())) {
					formatter.setRepository(repo);
					formatter.setDetectRenames(true);

					List<DiffEntry> diffs = formatter.scan(parentIterator, commitIterator);

					RenameDetector renameDetector = new RenameDetector(repo);
					renameDetector.addAll(diffs);
					diffs = renameDetector.compute();

					if (filePath != null) {
						final String targetPath = filePath;
						diffs = diffs.stream()
								.filter(d -> d.getNewPath().equals(targetPath) || d.getOldPath().equals(targetPath))
								.collect(Collectors.toList());
					}

					for (DiffEntry diff : diffs) {
						Map<String, Object> fileChange = new LinkedHashMap<>();
						fileChange.put("fileName",
								diff.getChangeType() == DiffEntry.ChangeType.DELETE ? diff.getOldPath()
										: diff.getNewPath());
						fileChange.put("changeType", diff.getChangeType().name());

						if (diff.getChangeType() == DiffEntry.ChangeType.RENAME
								|| diff.getChangeType() == DiffEntry.ChangeType.COPY) {
							fileChange.put("oldPath", diff.getOldPath());
							fileChange.put("newPath", diff.getNewPath());
						}

						if (filePath != null) {
							try (ByteArrayOutputStream diffStream = new ByteArrayOutputStream()) {
								DiffFormatter fullFormatter = new DiffFormatter(diffStream);
								fullFormatter.setRepository(repo);
								fullFormatter.setDetectRenames(true);
								fullFormatter.format(diff);
								fullFormatter.flush();

								String rawDiff = diffStream.toString("UTF-8");
								String cleanDiff = stripDiffHeaders(rawDiff);

								boolean isBinary = rawDiff.contains("Binary files differ");
								boolean isTruncated = false;

								if (!isBinary) {
									String[] lines = cleanDiff.split("\n");
									if (lines.length > MAX_DIFF_LINES) {
										StringBuilder sb = new StringBuilder();
										for (int i = 0; i < MAX_DIFF_LINES; i++) {
											sb.append(lines[i]).append("\n");
										}
										cleanDiff = sb.toString();
										isTruncated = true;
									}
								}

								fileChange.put("diff", cleanDiff);
								fileChange.put("isBinary", isBinary);
								fileChange.put("isTruncated", isTruncated);
								fullFormatter.close();
							}
						}

						result.add(fileChange);
					}
				}
			}

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error getting commit diff for engine {} and commit {}", engineId, commitId, e);
			throw new SemossPixelException(
					"Error occurred getting the commit diff. Detailed error = " + e.getMessage(), e);
		}

		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.ENGINE_INFO);
	}

	/**
	 * Strips the git plumbing headers from a unified diff, returning only the
	 * hunk content lines (starting from the first @@ marker).
	 *
	 * @param rawDiff the full diff output including headers
	 * @return the diff content without plumbing headers
	 */
	private String stripDiffHeaders(String rawDiff) {
		String[] lines = rawDiff.split("\n");
		StringBuilder sb = new StringBuilder();
		boolean foundHunk = false;
		for (String line : lines) {
			if (!foundHunk && line.startsWith("@@")) {
				foundHunk = true;
			}
			if (foundHunk) {
				sb.append(line).append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Prepares a tree parser for a given commit, used to compute diffs between
	 * two commits.
	 *
	 * @param repo   the git repository
	 * @param commit the commit to parse the tree for
	 * @return a tree iterator positioned at the commit's tree
	 * @throws IOException if the tree cannot be read
	 */
	private AbstractTreeIterator prepareTreeParser(Repository repo, RevCommit commit) throws IOException {
		try (ObjectReader reader = repo.newObjectReader()) {
			CanonicalTreeParser treeParser = new CanonicalTreeParser();
			treeParser.reset(reader, commit.getTree().getId());
			return treeParser;
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the changed files and optional diff for a commit in an engine";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		} else if (key.equals(COMMIT_ID_KEY)) {
			return "The commit SHA to get the diff for";
		} else if (key.equals(FILE_PATH_KEY)) {
			return "Optional file path to get a specific file diff";
		}
		return super.getDescriptionForKey(key);
	}

}
