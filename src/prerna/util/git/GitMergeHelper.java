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
package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.MergeCommand;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.errors.AbortedByHookException;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;

import prerna.util.Utility;

/**
 * Static helper utilities for merging and aborting merges on local jgit
 * repositories. Provides conflict-aware merging with an optional
 * delete-conflicting-files retry strategy as well as a hard reset to abort an
 * in-progress merge.
 */
public class GitMergeHelper {

	private static final Logger classLogger = LogManager.getLogger(GitMergeHelper.class);

	private GitMergeHelper() {

	}

	/**
	 * Opens the git repository at {@code localRepository}, checks out
	 * {@code startPoint}, and merges {@code branchName} into it. The repository
	 * (and the underlying {@link Git} instance) is opened and closed within this
	 * call via try-with-resources.
	 * <p>
	 * If either {@code startPoint} or {@code branchName} cannot be resolved to a
	 * {@link Ref}, the merge is skipped. When the merge results in a conflict and
	 * {@code delete} is {@code true} and {@code numAttempts} is still below
	 * {@code maxAttempts}, the conflicting files are deleted (see
	 * {@link #wipeFiles(List)}), all files are re-added and committed, and this
	 * method recurses with an incremented attempt count to retry the merge. The
	 * same delete-and-retry recovery is applied if a
	 * {@link CheckoutConflictException} is thrown during checkout. All other git
	 * failures are logged and swallowed.
	 *
	 * @param localRepository the file system path to the local git repository to
	 *                        merge in
	 * @param startPoint      the branch/ref to check out and merge the changes into
	 * @param branchName      the branch/ref whose changes are merged into
	 *                        {@code startPoint}
	 * @param numAttempts     the current merge attempt count, incremented on each
	 *                        conflict-driven retry
	 * @param maxAttempts     the maximum number of merge attempts before giving up
	 *                        on conflict resolution
	 * @param delete          if {@code true}, conflicting files are deleted and the
	 *                        merge is retried; if {@code false}, conflicts are left
	 *                        for the user to resolve manually
	 */
	public static void merge(String localRepository, String startPoint, String branchName, int numAttempts,
			int maxAttempts, boolean delete) {
		try (Git thisGit = Git.open(new File(localRepository)); Repository thisRepo = thisGit.getRepository()) {
			Ref startRef = thisRepo.findRef(startPoint);

			CheckoutCommand cc = thisGit.checkout();
			cc.setName(startPoint);
			cc.setCreateBranch(false); // probably not needed, just to make sure
			if (startRef != null) {
				cc.call();
			}
			MergeCommand mc = thisGit.merge();
			Ref ref = thisGit.getRepository().findRef(branchName);
			if (ref != null && startRef != null) {
				mc.include(ref);
				MergeResult res = mc.call();
				boolean retBoolean = true;
				if (res.getMergeStatus().equals(MergeResult.MergeStatus.CONFLICTING)) {
					classLogger.debug(res.getConflicts().toString());
					retBoolean = false;
					Iterator<String> files = res.getConflicts().keySet().iterator();
					Vector<String> delFiles = new Vector<>();
					while (files.hasNext()) {
						String thisFile = files.next();
						classLogger.debug("File is{}", thisFile);
						if (!delFiles.contains(thisFile)) {
							delFiles.add(thisFile);
						}
					}
					// inform the user he has to handle the conflicts
					if (numAttempts < maxAttempts && delete) {
						wipeFiles(delFiles);
						GitRepoUtils.addAllFiles(localRepository, false);
						GitRepoUtils.commitAddedFiles(localRepository);
						numAttempts++;
						// I will attempt this just one more time to merge
						merge(localRepository, startPoint, branchName, numAttempts, maxAttempts, delete);
					}
				}
			}
		} catch (CheckoutConflictException e) {
			if (delete) {
				List<String> delFiles = e.getConflictingPaths();
				wipeFiles(delFiles);
				GitRepoUtils.addAllFiles(localRepository, false);
				GitRepoUtils.commitAddedFiles(localRepository);
				numAttempts++;
				// I will attempt this just one more time to merge
				merge(localRepository, startPoint, branchName, numAttempts, maxAttempts, delete);
			}
		} catch (NoFilepatternException nfpe) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, nfpe);
		} catch (NoHeadException nhe) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, nhe);
		} catch (NoMessageException nme) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, nme);
		} catch (UnmergedPathsException upe) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, upe);
		} catch (ConcurrentRefUpdateException crue) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, crue);
		} catch (WrongRepositoryStateException wrse) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, wrse);
		} catch (AbortedByHookException abhe) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, abhe);
		} catch (IOException ioe) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, ioe);
		} catch (GitAPIException gae) {
			classLogger.error("Failed to merge branch {} into {} for repository {}", branchName, startPoint,
					localRepository, gae);
		}
	}

	/**
	 * Deletes the conflicting files from disk. Each path is normalized via
	 * {@link Utility#normalizePath(String)} and only deleted if it currently
	 * exists; non-existent paths are skipped.
	 *
	 * @param filesToWipe the list of file paths to delete from the file system
	 */
	private static void wipeFiles(List<String> filesToWipe) {
		for (int fileIndex = 0; fileIndex < filesToWipe.size(); fileIndex++) {
			File file = new File(Utility.normalizePath(filesToWipe.get(fileIndex)));
			if (file.exists()) {
				file.delete();
			}
		}
	}

	/**
	 * Aborts an in-progress merge by performing a hard reset of the repository back
	 * to {@code HEAD}, discarding any working-tree and index changes. The
	 * repository is opened from the given path and closed in a {@code finally}
	 * block. Failures to open the repository or to perform the reset are logged and
	 * swallowed.
	 *
	 * @param repository the file system path to the local git repository to reset
	 */
	public static void abortMerge(String repository) {
		File dirFile = new File(repository);
		Git thisGit = null;
		try {
			thisGit = Git.open(dirFile);
		} catch (IOException e) {
			classLogger.error("Failed to open git repository at {} to abort merge", repository, e);
		}
		try {
			if (thisGit != null) {
				thisGit.reset().setMode(ResetType.HARD).setRef("HEAD").call();
			}
		} catch (GitAPIException e) {
			classLogger.error("Failed to abort merge with hard reset for repository {}", repository, e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
		}
	}

}
