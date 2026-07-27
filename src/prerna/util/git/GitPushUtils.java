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
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.CreateBranchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import prerna.auth.AuthProvider;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Static utility methods for interacting with a local jgit {@link Git}
 * repository: pushing, pulling, checking out branches, listing branches and
 * cloning. Each operation opens the repository on demand, builds a
 * {@link CredentialsProvider} appropriate to the supplied {@link AuthProvider}
 * (GitLab uses an {@code oauth2}/token pair while all other providers use a
 * token/empty-secret pair) and logs failures rather than propagating most
 * exceptions.
 */
public class GitPushUtils {

	private static final Logger classLogger = LogManager.getLogger(GitPushUtils.class);

	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private GitPushUtils() {

	}

	/**
	 * Pushes the given branch of a local repository to a remote, selecting the
	 * {@link AuthProvider} from the {@link Constants#GIT_PROVIDER} configuration
	 * property. If that property equals {@code gitlab} (case-insensitive) the push
	 * uses {@link AuthProvider#GITLAB}, otherwise it defaults to
	 * {@link AuthProvider#GITHUB}. Delegates to
	 * {@link #push(String, String, String, String, AuthProvider, int)} with the
	 * attempt counter initialized to {@code 1}.
	 *
	 * @param repository   path to the local Git repository to push from
	 * @param remoteToPush name of the remote to push to
	 * @param branch       branch to push; may be {@code null} or empty to use the
	 *                     command default
	 * @param token        credential token used to authenticate with the remote
	 */
	public static void push(String repository, String remoteToPush, String branch, String token) {
		int attempt = 1;

		String gitProvider = Utility.getDIHelperProperty(Constants.GIT_PROVIDER);
		if (gitProvider != null && !(gitProvider.isEmpty())
				&& gitProvider.toLowerCase().equals(AuthProvider.GITLAB.toString().toLowerCase())) {
			push(repository, remoteToPush, branch, token, AuthProvider.GITLAB, attempt);
		} else {
			push(repository, remoteToPush, branch, token, AuthProvider.GITHUB, attempt);
		}
	}

	/**
	 * Opens the local repository and pushes the given branch to the named remote
	 * using credentials derived from the supplied provider. For
	 * {@link AuthProvider#GITLAB} an {@code oauth2}/token credential pair is used;
	 * any other provider uses a token/empty-secret pair. The push is only performed
	 * while {@code attempt} is less than {@code 3}, so the method is a no-op once
	 * the attempt counter reaches that limit. Failures opening the repository or
	 * calling the push are logged; no exception is propagated. The opened
	 * {@link Git} handle is closed automatically via try-with-resources.
	 *
	 * @param repository   path to the local Git repository to push from
	 * @param remoteToPush name of the remote to push to
	 * @param branch       branch to push; added to the push command only when
	 *                     non-{@code null} and non-empty
	 * @param token        credential token used to authenticate with the remote
	 * @param prov         authentication provider that determines how credentials
	 *                     are constructed
	 * @param attempt      current attempt number; the push runs only when this is
	 *                     less than {@code 3}
	 */
	public static void push(String repository, String remoteToPush, String branch, String token, AuthProvider prov,
			int attempt) {
		if (attempt < 3) {
			Boolean isGitlab = (prov == AuthProvider.GITLAB);

			File dirFile = new File(Utility.normalizePath(repository));
			try (Git thisGit = Git.open(dirFile)) {

				CredentialsProvider cp = null;
				if (isGitlab) {
					cp = new UsernamePasswordCredentialsProvider("oauth2", token);
				} else {
					cp = new UsernamePasswordCredentialsProvider(token, "");
				}

				PushCommand pc = thisGit.push();
				pc.setRemote(remoteToPush);
				if (branch != null && !branch.isEmpty()) {
					pc.add(branch);
				}
				pc.setCredentialsProvider(cp);
				try {
					pc.call();
				} catch (GitAPIException e) {
					classLogger.error("Failed to push to remote {} for repo {}", remoteToPush, repository, e);
				}
			} catch (IOException e) {
				classLogger.error("Failed to open Git directory at {}", repository, e);
			}
		}
	}

	/**
	 * Opens the local repository and performs a {@code git pull} using credentials
	 * derived from the supplied provider ({@code oauth2}/token for
	 * {@link AuthProvider#GITLAB}, otherwise token/empty-secret). Failures opening
	 * the repository are logged and result in the "Git is empty" error response.
	 * The opened {@link Git} handle is closed in a {@code finally} block once the
	 * pull has been attempted.
	 *
	 * @param repository path to the local Git repository to pull into
	 * @param token      credential token used to authenticate with the remote
	 * @param prov       authentication provider that determines how credentials are
	 *                   constructed
	 * @return a {@link NounMetadata} describing the outcome: a success/non-success
	 *         message when the pull completes, an {@link PixelDataType#ERROR} noun
	 *         carrying the exception text if the pull throws, or an
	 *         {@link PixelDataType#ERROR} "Git is empty" noun if the repository
	 *         could not be opened
	 */
	public static NounMetadata pull(String repository, String token, AuthProvider prov) {
		Boolean isGitlab = null;
		if (prov.toString().equals(AuthProvider.GITLAB.toString())) {
			isGitlab = true;
		} else {
			isGitlab = false;
		}

		File dirFile = new File(Utility.normalizePath(repository));
		Git thisGit = null;
		try {
			thisGit = Git.open(dirFile);
		} catch (IOException e) {
			classLogger.error("Failed to open Git directory at {}", repository, e);
		}
		CredentialsProvider cp = null;
		if (isGitlab) {
			cp = new UsernamePasswordCredentialsProvider("oauth2", token);
		} else {
			cp = new UsernamePasswordCredentialsProvider(token, "");
		}

		if (thisGit != null) {
			PullCommand pc = thisGit.pull();
			pc.setCredentialsProvider(cp);
			try {
				PullResult pr = pc.call();
				if (pr.isSuccessful()) {
					return new NounMetadata("Git Pulled: " + pr.isSuccessful(), PixelDataType.CONST_STRING,
							PixelOperationType.HELP);
				} else {
					return new NounMetadata("Git pull error", PixelDataType.CONST_STRING, PixelOperationType.HELP);
				}
			} catch (GitAPIException e) {
				classLogger.error("Failed to pull from remote for repo {}", repository, e);
				return new NounMetadata("Git Pull Error: " + e, PixelDataType.ERROR, PixelOperationType.HELP);
			} finally {
				thisGit.close();
			}

		}
		return new NounMetadata("Git Pull Error - Git is empty ", PixelDataType.ERROR, PixelOperationType.HELP);

	}

	/**
	 * Opens the local repository and checks out the given branch. If the branch
	 * does not already exist locally (determined via
	 * {@link #branchNameExist(Git, String)}) it is created as a new tracking branch
	 * with its start point set to {@code origin/<branch>} and upstream mode
	 * {@link CreateBranchCommand.SetupUpstreamMode#TRACK}; otherwise the existing
	 * branch is simply checked out. Credentials are derived from the supplied
	 * provider but the local checkout itself does not use them. Failures opening
	 * the repository or listing branches are logged; the {@link Git} handle is
	 * closed in a {@code finally} block after the checkout is attempted.
	 *
	 * @param repository path to the local Git repository to operate on
	 * @param branch     name of the branch to check out or create
	 * @param token      credential token associated with the provider
	 * @param prov       authentication provider that determines how credentials are
	 *                   constructed
	 * @return a {@link NounMetadata} describing the outcome: a success message
	 *         naming the branch, an {@link PixelDataType#ERROR} noun carrying the
	 *         exception text if the checkout throws, or an
	 *         {@link PixelDataType#ERROR} "Git is empty" noun if the repository
	 *         could not be opened
	 */
	public static NounMetadata checkout(String repository, String branch, String token, AuthProvider prov) {
		Boolean isGitlab = null;
		if (prov.toString().equals(AuthProvider.GITLAB.toString())) {
			isGitlab = true;
		} else {
			isGitlab = false;
		}

		File dirFile = new File(Utility.normalizePath(repository));
		Git thisGit = null;
		boolean exists = false;
		try {
			thisGit = Git.open(dirFile);
			exists = branchNameExist(thisGit, branch);
		} catch (IOException | GitAPIException e) {
			classLogger.error("Failed to open Git directory or list branches for repo {}", repository, e);
		}
		CredentialsProvider cp = null;
		if (isGitlab) {
			cp = new UsernamePasswordCredentialsProvider("oauth2", token);
		} else {
			cp = new UsernamePasswordCredentialsProvider(token, "");
		}

		if (thisGit != null) {
			CheckoutCommand checkout = thisGit.checkout();

			if (!exists) {

				checkout.setCreateBranch(true);
				checkout.setName(branch);
				checkout.setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.TRACK);
				checkout.setStartPoint("origin/" + branch);
			} else {
				checkout.setName(branch);
			}
			try {
				checkout.call();
				return new NounMetadata("Git checkout: " + branch, PixelDataType.CONST_STRING, PixelOperationType.HELP);
			} catch (GitAPIException e) {
				classLogger.error("Failed to checkout branch {} for repo {}", branch, repository, e);
				return new NounMetadata("Git Checkout Error: " + e, PixelDataType.ERROR, PixelOperationType.HELP);
			} finally {
				thisGit.close();
			}

		}
		return new NounMetadata("Git Checkout Error - Git is empty ", PixelDataType.ERROR, PixelOperationType.HELP);
	}

	/**
	 * Determines whether a local branch matching the given name exists by listing
	 * the repository branches and returning {@code true} as soon as any ref name
	 * contains {@code branchName} as a substring.
	 *
	 * @param git        the open repository whose branches are listed
	 * @param branchName branch name to search for; matched against each ref name
	 *                   via {@link String#contains(CharSequence)}
	 * @return {@code true} if a branch ref name contains {@code branchName},
	 *         {@code false} otherwise
	 * @throws GitAPIException if listing the branches fails
	 * @author wgs
	 * @date July 20, 2019 2:49:46 PM
	 */
	public static boolean branchNameExist(Git git, String branchName) throws GitAPIException {
		List<Ref> refs = git.branchList().call();
		for (Ref ref : refs) {
			if (ref.getName().contains(branchName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Clones a remote repository into {@code workingDir}, appending the derived
	 * repository folder name to the destination. Delegates to
	 * {@link #clone(String, String, String, AuthProvider, boolean)} with
	 * {@code appendFolderName} set to {@code true}.
	 *
	 * @param workingDir base directory into which the repository is cloned
	 * @param repo       URI of the remote repository to clone
	 * @param token      credential token used to authenticate; may be {@code null}
	 *                   to clone without credentials
	 * @param prov       authentication provider that determines how credentials are
	 *                   constructed
	 * @return a {@link NounMetadata} describing the clone outcome (success or
	 *         error)
	 */
	public static NounMetadata clone(String workingDir, String repo, String token, AuthProvider prov) {
		return clone(workingDir, repo, token, prov, true);
	}

	/**
	 * Clones a remote repository into the given working directory. When
	 * {@code appendFolderName} is {@code true} the destination is
	 * {@code workingDir/<instanceName>} where the instance name is derived from
	 * {@code repo} (the portion before the first {@code .}); otherwise the
	 * repository is cloned directly into {@code workingDir}. If the
	 * {@link Constants#GIT_TRUSTED_REPO} property is configured, the clone is
	 * rejected with an error noun unless {@code repo} starts with that trusted
	 * prefix, and the {@link Constants#GIT_DEFAULT_BRANCH} property (when set) is
	 * used as the branch to clone. Credentials are applied only when {@code token}
	 * is non-{@code null} ({@code oauth2}/token for {@link AuthProvider#GITLAB},
	 * otherwise token/empty-secret). Clone failures are logged and returned as an
	 * error noun.
	 *
	 * @param workingDir       base directory into which the repository is cloned
	 * @param repo             URI of the remote repository to clone
	 * @param token            credential token used to authenticate; may be
	 *                         {@code null} to clone without credentials
	 * @param prov             authentication provider that determines how
	 *                         credentials are constructed
	 * @param appendFolderName when {@code true}, append the derived repository
	 *                         folder name to {@code workingDir}; when
	 *                         {@code false}, clone directly into {@code workingDir}
	 * @return a {@link NounMetadata} describing the outcome: a success message
	 *         naming the repo, an {@link PixelDataType#ERROR} noun if cloning from
	 *         an unapproved registry, or an {@link PixelDataType#ERROR} noun
	 *         carrying the exception text if the clone throws
	 */
	public static NounMetadata clone(String workingDir, String repo, String token, AuthProvider prov,
			boolean appendFolderName) {
		Boolean isGitlab = (prov == AuthProvider.GITLAB);
		workingDir = Utility.normalizePath(workingDir);

		File dirFile = null;
		if (appendFolderName) {
			String dirName = Utility.getInstanceName(repo).split(Pattern.quote("."))[0];
			dirFile = new File(workingDir + FILE_SEPARATOR + dirName);
		} else {
			dirFile = new File(workingDir);
		}

		String trustedRepo = Utility.getDIHelperProperty(Constants.GIT_TRUSTED_REPO);
		String defaultBranch = Utility.getDIHelperProperty(Constants.GIT_DEFAULT_BRANCH);

		if (trustedRepo != null && !trustedRepo.isEmpty()) {
			if (!repo.startsWith(trustedRepo)) {
				return new NounMetadata("Git clone Error: Cloning from unapproved git registry", PixelDataType.ERROR,
						PixelOperationType.HELP);
			}
		}

		CredentialsProvider cp = null;
		if (token != null) {
			if (isGitlab) {
				cp = new UsernamePasswordCredentialsProvider("oauth2", token);
			} else {
				cp = new UsernamePasswordCredentialsProvider(token, "");
			}
			classLogger.info("Cloning project {} with {} credentials", repo, prov);
		} else {
			classLogger.info("Cloning project {} without any credentials", repo);
		}

		CloneCommand clone = Git.cloneRepository();
		clone.setURI(repo);
		clone.setDirectory(dirFile);
		if (cp != null) {
			clone.setCredentialsProvider(cp);
		}
		if (trustedRepo != null && !trustedRepo.isEmpty()) {
			if (defaultBranch != null && !defaultBranch.isEmpty()) {
				clone.setBranch(defaultBranch);
			}
		}

		try {
			clone.call();
			return new NounMetadata("Git clone success: " + repo, PixelDataType.CONST_STRING, PixelOperationType.HELP);
		} catch (GitAPIException e) {
			classLogger.error("Failed to clone repo {} to {}", repo, dirFile, e);
			return new NounMetadata("Git clone error: " + e, PixelDataType.ERROR, PixelOperationType.HELP);
		}

	}

}
