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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;

/**
 * Static utility for fetching remote git repositories. Currently provides a
 * destructive clone that removes any existing local copy before cloning the
 * remote repository fresh.
 */
public class GitFetchUtils {

	protected static final Logger classLogger = LogManager.getLogger(GitFetchUtils.class);

	private GitFetchUtils() {

	}

	/**
	 * Clones the remote repository into the given local folder, replacing any
	 * existing copy. The SSL certificate for the remote's domain is first installed
	 * via {@link GitRepoUtils#addCertForDomain(String)}. If {@code localFolder}
	 * already exists as a directory it is deleted, and then the remote repository
	 * is cloned fresh into that location. The cloned {@link Git} instance is opened
	 * and closed within this call. Invalid remote, transport, IO, and other git API
	 * failures are logged and swallowed rather than propagated.
	 *
	 * @param remoteRepo  the URI of the remote git repository to clone
	 * @param localFolder the local file system path where the repository is cloned
	 *                    (deleted first if it already exists)
	 */
	// wipes it and puts a new clone
	public static void cloneApp(String remoteRepo, String localFolder) {
		GitRepoUtils.addCertForDomain(remoteRepo);
		// tries to find if the local folder is available
		// deletes it and then clones it back
		try {
			File file = new File(localFolder);
			if (file.exists() && file.isDirectory()) {
				FileUtils.deleteDirectory(file);
			}

			CloneCommand clone = Git.cloneRepository().setURI(remoteRepo).setDirectory(file);
			try (Git gclone = clone.call()) {
			}
		} catch (InvalidRemoteException e) {
			classLogger.error("Invalid remote repository {} while cloning to {}", remoteRepo, localFolder, e);
		} catch (org.eclipse.jgit.api.errors.TransportException e) {
			classLogger.error("Transport error while cloning remote repository {} to {}", remoteRepo, localFolder, e);
		} catch (IOException e) {
			classLogger.error("Failed to delete existing directory or clone remote repository {} to {}", remoteRepo,
					localFolder, e);
		} catch (GitAPIException e) {
			classLogger.error("Failed to clone remote repository {} to {}", remoteRepo, localFolder, e);
		}
	}

}
