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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.GitAPIException;

/**
 * Removes files from a local Git repository - either un-tracking them (leaving
 * the working-tree copy on disk) or deleting them outright - using jgit's
 * {@code rm} command.
 */
public class GitDestroyer {

	private static final Logger classLogger = LogManager.getLogger(GitDestroyer.class);

	private GitDestroyer() {

	}

	/**
	 * Removes the given files from a local Git repository.
	 * <p>
	 * When {@code deleteFile} is {@code true} the files are deleted from disk and
	 * the index; when {@code false} they are only un-tracked (removed from the
	 * index via {@code rm --cached}) and the working-tree copies are left in place.
	 * Each path is normalized before being staged: anything under a {@code version}
	 * folder is made relative to that folder, and backslashes are converted to
	 * forward slashes. A {@code null} or empty {@code files} list is a no-op.
	 *
	 * @param localRepository the local Git working directory to operate on
	 * @param deleteFile      {@code true} to delete the files from disk as well as
	 *                        un-tracking them; {@code false} to only remove them
	 *                        from Git tracking and keep them on disk
	 * @param files           the file paths to remove (absolute or
	 *                        version-relative); {@code null} or empty is a no-op
	 * @throws IllegalArgumentException if the repository cannot be opened or the
	 *                                  remove operation fails
	 */
	public static void removeSpecificFiles(String localRepository, boolean deleteFile, List<String> files) {
		if (files == null || files.size() == 0) {
			return;
		}
		try (Git thisGit = Git.open(new File(localRepository))) {
			// if we want to delete the file, setCached needs to be set to false
			RmCommand rm = thisGit.rm().setCached(!deleteFile);
			for (String daFile : files) {
				if (daFile.contains("/version")) {
					daFile = daFile.substring(daFile.indexOf("/version") + 9);
				} else if (daFile.contains("\\version")) {
					daFile = daFile.substring(daFile.indexOf("\\version") + 9);
				}
				daFile = daFile.replace("\\", "/");
				rm.addFilepattern(daFile);
			}

			try {
				rm.call();
			} catch (GitAPIException e) {
				classLogger.error("Failed to remove specified files from git repository at {}", localRepository, e);
				throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
			}
		} catch (IOException e) {
			classLogger.error("Failed to open git repository at {}", localRepository, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}
	}

}
