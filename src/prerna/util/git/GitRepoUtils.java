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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.security.InstallCertNow;
import prerna.util.Utility;

/**
 * Static utility methods for working with local git repositories and their
 * GitHub remotes via JGit and the GitHub Java APIs. Provides helpers to check
 * remote repository existence, configure/list remotes, fetch/pull and push,
 * inspect commit history, read file content at a given commit, and stage and
 * commit changes.
 */
public class GitRepoUtils {

	private static final Logger classLogger = LogManager.getLogger(GitRepoUtils.class);

	public static final String DUAL = "DUAL";
	public static final String SUBSCRIBE = "SUBSCRIBE";
	public static final String PUBLISH = "PUBLISH";

	private GitRepoUtils() {

	}

	/**
	 * Convenience overload of {@link #checkRemoteRepositoryO(String, String, int)}
	 * that starts the attempt counter at {@code 1}.
	 *
	 * @param repositoryName the GitHub repository, either {@code owner/repo} or
	 *                       just {@code repo} (the owner is resolved from the OAuth
	 *                       user)
	 * @param oauth          OAuth2 token used to authenticate against GitHub; may
	 *                       be {@code null}
	 * @return {@code true} if the repository was found, {@code false} otherwise
	 */
	public static boolean checkRemoteRepositoryO(String repositoryName, String oauth) {
		int attempt = 1;
		return checkRemoteRepositoryO(repositoryName, oauth, attempt);
	}

	/**
	 * Checks whether the given GitHub repository exists and is reachable using the
	 * supplied OAuth token. When {@code oauth} is provided the owner is resolved
	 * from the authenticated user if {@code repositoryName} does not already
	 * contain a {@code /}. On an {@link HttpException} this attempts to install the
	 * github.com certificate and retries by recursively re-invoking itself with an
	 * incremented attempt counter, giving up once {@code attempt} reaches
	 * {@code 3}.
	 *
	 * @param repositoryName the GitHub repository, either {@code owner/repo} or
	 *                       just {@code repo} (the owner is resolved from the OAuth
	 *                       user)
	 * @param oauth          OAuth2 token used to authenticate against GitHub; may
	 *                       be {@code null}
	 * @param attempt        the current attempt number; retries stop once this
	 *                       reaches {@code 3}
	 * @return {@code true} if the repository was found, {@code false} if the
	 *         maximum number of attempts was exhausted
	 * @throws IllegalArgumentException if the repository cannot be found (lookup
	 *                                  fails with a non-HTTP exception)
	 */
	public static boolean checkRemoteRepositoryO(String repositoryName, String oauth, int attempt) {

		boolean returnVal = true;
		String[] repoParts = null;

		if (attempt < 3) {
			try {
				GitHubClient client = GitHubClient.createClient("https://github.com");
				if (oauth != null) {
					client.setOAuth2Token(oauth);
					GitHub gh = GitUtils.login(oauth);
					classLogger.debug(gh.getMyself().getLogin());
					if (!repositoryName.contains("/")) {
						repositoryName = gh.getMyself().getLogin() + "/" + repositoryName;
					}
				}

				repoParts = repositoryName.split("/");

				RepositoryService service = new RepositoryService(client);

				service.getRepository(repoParts[0], repoParts[1]);

			} catch (HttpException ex) {
				classLogger.error("Failed to check remote repository access using OAuth: {}", ex.getMessage(), ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					classLogger.error("Failed to check remote repository access using OAuth: {}", e.getMessage(), e);
				}
				attempt = attempt + 1;
				checkRemoteRepositoryO(repositoryName, oauth, attempt);
			} catch (Exception ex) {
				if (repoParts != null) {
					throw new IllegalArgumentException(
							"Cannot find repo at " + repositoryName + " for username " + repoParts[0]);
				} else {
					throw new IllegalArgumentException("Cannot find repo at " + repositoryName + " for null username ");
				}
			}
			return returnVal;
		}

		return false;
	}

	/**
	 * Adds (or overwrites) a named git remote on the local repository pointing at
	 * the GitHub URL {@code https://github.com/<username>/<repoName>} and saves the
	 * updated stored config. The remote is named after {@code repoName} and its
	 * fetch refspec is configured to track all of its heads. Opens and closes the
	 * repository for the duration of the call.
	 *
	 * @param localRepository path to the local git working directory
	 * @param username        GitHub owner/user used to build the remote URL
	 * @param repoName        repository name, used both as the remote name and in
	 *                        the remote URL
	 * @throws IllegalArgumentException if the repository config cannot be read or
	 *                                  saved
	 */
	public static void addRemote(String localRepository, String username, String repoName) {
		StoredConfig config;
		try (Git thisGit = Git.open(new File(localRepository)); Repository thisRepo = thisGit.getRepository()) {
			config = thisRepo.getConfig();
			config.setString("remote", repoName, "url", "https://github.com/" + username + "/" + repoName);
			config.setString("remote", repoName, "fetch", "+refs/heads/*:refs/remotes/" + repoName + "/*");
			config.save();
		} catch (IOException e) {
			classLogger.error("Failed to add git remote: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Error with adding the remote repository");
		}
	}

	/**
	 * Fetch and merge (pull) the current branch from a remote using the supplied
	 * credentials. The named remote's URL is (re)pointed at {@code remoteUrl} first
	 * so a short-lived token is never embedded/persisted in git config - the
	 * credential is supplied per call via {@code cp}.
	 *
	 * @param localRepository local git working directory
	 * @param remoteName      name of the remote to use/point (e.g. "origin")
	 * @param remoteUrl       https clone url to point the remote at
	 * @param cp              credentials used for the fetch (may be null for public
	 *                        repos)
	 * @return the jgit {@link PullResult} describing the fetch + merge outcome
	 * @throws IOException     if the local repository cannot be opened
	 * @throws GitAPIException if the fetch or merge fails
	 */
	public static PullResult pullFromRemote(String localRepository, String remoteName, String remoteUrl,
			CredentialsProvider cp) throws IOException, GitAPIException {
		File dir = new File(Utility.normalizePath(localRepository));
		try (Git thisGit = Git.open(dir); Repository thisRepo = thisGit.getRepository()) {
			StoredConfig config = thisRepo.getConfig();
			config.setString("remote", remoteName, "url", remoteUrl);
			config.setString("remote", remoteName, "fetch", "+refs/heads/*:refs/remotes/" + remoteName + "/*");
			config.save();

			String branch = thisRepo.getBranch();
			PullCommand pc = thisGit.pull().setRemote(remoteName);
			if (cp != null) {
				pc.setCredentialsProvider(cp);
			}
			if (branch != null && !branch.isEmpty()) {
				pc.setRemoteBranchName(branch);
			}
			return pc.call();
		}
	}

	/**
	 * Push the current branch to a remote using the supplied credentials. The named
	 * remote's URL is (re)pointed at {@code remoteUrl} first so a short-lived token
	 * is never embedded/persisted in git config - the credential is supplied per
	 * call via {@code cp}.
	 *
	 * @param localRepository local git working directory
	 * @param remoteName      name of the remote to use/point (e.g. "origin")
	 * @param remoteUrl       https clone url to point the remote at
	 * @param cp              credentials used for the push (may be null for public
	 *                        repos)
	 * @return the jgit {@link PushResult}s describing the per-ref outcome
	 * @throws IOException     if the local repository cannot be opened
	 * @throws GitAPIException if the push fails
	 */
	public static Iterable<PushResult> pushToRemote(String localRepository, String remoteName, String remoteUrl,
			CredentialsProvider cp) throws IOException, GitAPIException {
		File dir = new File(Utility.normalizePath(localRepository));
		try (Git thisGit = Git.open(dir); Repository thisRepo = thisGit.getRepository()) {
			StoredConfig config = thisRepo.getConfig();
			config.setString("remote", remoteName, "url", remoteUrl);
			config.setString("remote", remoteName, "fetch", "+refs/heads/*:refs/remotes/" + remoteName + "/*");
			config.save();

			String branch = thisRepo.getBranch();
			PushCommand pc = thisGit.push().setRemote(remoteName);
			if (cp != null) {
				pc.setCredentialsProvider(cp);
			}
			if (branch != null && !branch.isEmpty()) {
				pc.add(branch);
			}
			return pc.call();
		}
	}

	/**
	 * Fetches a remote, checks out the target branch (creating it to track the
	 * remote branch if it does not exist locally), and hard-resets it to match the
	 * remote, replacing local history and working-tree content with the remote
	 * state.
	 * <p>
	 * The named remote's URL is (re)pointed at {@code remoteUrl} first so a
	 * short-lived token is never persisted in git config. This is a destructive,
	 * one-way mirror operation: any uncommitted or divergent local changes are
	 * discarded (hard reset) and untracked files are removed (git clean, respecting
	 * {@code .gitignore}) so the working tree exactly matches the remote branch.
	 *
	 * @param localRepository local git working directory
	 * @param remoteName      name of the remote to use/point (e.g. "origin")
	 * @param remoteUrl       https clone url to point the remote at
	 * @param branch          branch to switch to and reset; when {@code null} or
	 *                        empty the local repository's current branch name is
	 *                        used
	 * @param cp              credentials used for the fetch (may be {@code null}
	 *                        for public repos)
	 * @return the resulting {@code HEAD} commit SHA after the reset, or
	 *         {@code null} if HEAD cannot be resolved
	 * @throws IOException     if the local repository cannot be opened
	 * @throws GitAPIException if the fetch or reset fails
	 */
	public static String resetToRemote(String localRepository, String remoteName, String remoteUrl, String branch,
			CredentialsProvider cp) throws IOException, GitAPIException {
		File dir = new File(Utility.normalizePath(localRepository));
		try (Git thisGit = Git.open(dir); Repository thisRepo = thisGit.getRepository()) {
			StoredConfig config = thisRepo.getConfig();
			config.setString("remote", remoteName, "url", remoteUrl);
			config.setString("remote", remoteName, "fetch", "+refs/heads/*:refs/remotes/" + remoteName + "/*");
			config.save();

			String targetBranch = (branch == null || branch.isEmpty()) ? thisRepo.getBranch() : branch;

			FetchCommand fetch = thisGit.fetch().setRemote(remoteName);
			if (cp != null) {
				fetch.setCredentialsProvider(cp);
			}
			fetch.call();

			// discard local working-tree/index changes so they cannot block the
			// checkout - the remote is the source of truth on a webhook-driven sync
			thisGit.reset().setMode(ResetType.HARD).call();

			// ensure the target branch is checked out, creating it to track the remote
			// branch if it does not exist locally (forced, in case anything still
			// conflicts in the working tree)
			if (!targetBranch.equals(thisRepo.getBranch())) {
				boolean localExists = thisRepo.findRef("refs/heads/" + targetBranch) != null;
				CheckoutCommand checkout = thisGit.checkout().setName(targetBranch).setForced(true);
				if (!localExists) {
					checkout.setCreateBranch(true).setStartPoint(remoteName + "/" + targetBranch)
							.setUpstreamMode(SetupUpstreamMode.TRACK);
				}
				checkout.call();
			}

			thisGit.reset().setMode(ResetType.HARD).setRef(remoteName + "/" + targetBranch).call();

			// remove untracked files/dirs (respecting .gitignore) so the working tree
			// is an exact mirror of the remote branch
			thisGit.clean().setCleanDirectories(true).call();

			ObjectId head = thisRepo.resolve("HEAD");
			return head == null ? null : head.getName();
		}
	}

	/**
	 * Shallow-clones a remote repository into {@code targetDir} at the tip of
	 * {@code branch} and returns the resolved {@code HEAD} SHA. Used by the
	 * monorepo subdir sync path to stage the full repo in a temporary directory
	 * before copying only the relevant subtree into the project's assets folder.
	 * <p>
	 * The caller is responsible for deleting {@code targetDir} afterwards (in a
	 * {@code finally} block) to avoid leaving stale staging directories on disk.
	 *
	 * @param targetDir the directory to clone into (created by the clone)
	 * @param remoteUrl HTTPS URL of the remote repository
	 * @param branch    the branch to check out
	 * @param cp        credentials provider carrying the installation token
	 * @return the {@code HEAD} commit SHA of the cloned repository
	 * @throws GitAPIException if the clone fails
	 * @throws IOException     if {@code HEAD} cannot be resolved after the clone
	 */
	public static String cloneToDir(File targetDir, String remoteUrl, String branch, CredentialsProvider cp)
			throws GitAPIException, IOException {
		try (Git cloned = Git.cloneRepository()
				.setURI(remoteUrl)
				.setDirectory(targetDir)
				.setBranch(branch)
				.setCloneAllBranches(false)
				.setDepth(1)
				.setCredentialsProvider(cp)
				.call()) {
			ObjectId head = cloned.getRepository().resolve("HEAD");
			return head == null ? null : head.getName();
		}
	}

	/**
	 * Returns the name of the branch currently checked out in the local repository.
	 *
	 * @param localRepository local git working directory
	 * @return the current branch name (e.g. "main"), or {@code null} if it cannot
	 *         be determined
	 * @throws IOException if the local repository cannot be opened
	 */
	public static String getCurrentBranch(String localRepository) throws IOException {
		try (Git thisGit = Git.open(new File(Utility.normalizePath(localRepository)));
				Repository thisRepo = thisGit.getRepository()) {
			return thisRepo.getBranch();
		}
	}

	/**
	 * Lists every remote configured on the local repository. For each remote it
	 * collects the remote {@code url}, derives a {@code name} of the form
	 * {@code namespace/appName} from that URL, and classifies the {@code type} as
	 * {@link #SUBSCRIBE} when the remote's {@code upstream} value equals
	 * {@code "DEFUNCT"} or {@link #DUAL} otherwise. Returns an empty list (and
	 * logs) if the repository cannot be opened.
	 *
	 * @param localRepositoryDir path to the local git working directory
	 * @return a list of maps, one per remote, each containing {@code url},
	 *         {@code name} and {@code type} entries; never {@code null}
	 */
	public static List<Map<String, String>> listConfigRemotes(String localRepositoryDir) {
		List<Map<String, String>> returnList = new ArrayList<>();
		try (Git thisGit = Git.open(new File(localRepositoryDir)); Repository thisRepo = thisGit.getRepository()) {
			String[] remNames = thisRepo.getRemoteNames().toArray(new String[] {});
			for (int remIndex = 0; remIndex < remNames.length; remIndex++) {
				String remName = remNames[remIndex] + "";
				String url = thisRepo.getConfig().getString("remote", remName, "url");
				String upstream = thisRepo.getConfig().getString(remName, "upstream", "url");

				Map<String, String> remoteMap = new Hashtable<String, String>();
				remoteMap.put("url", url);
				String appName = Utility.getClassName(url) + "/" + Utility.getInstanceName(url);
				remoteMap.put("name", appName);
				if (upstream != null && upstream.equalsIgnoreCase("DEFUNCT")) {
					remoteMap.put("type", SUBSCRIBE);
				} else {
					remoteMap.put("type", DUAL);
				}
				classLogger.debug("We have remote with details {}", remoteMap);
				returnList.add(remoteMap);
			}
		} catch (IOException e) {
			classLogger.error("Failed to list configured git remotes: {}", e.getMessage(), e);
		}

		return returnList;
	}

	/**
	 * Looks up the configured URL of a specific remote on the local repository. The
	 * remote name match is case-insensitive. Returns {@code null} if no matching
	 * remote is found or if the repository cannot be opened.
	 *
	 * @param localRepositoryName path to the local git working directory
	 * @param remoteName          name of the remote whose URL is requested (matched
	 *                            case-insensitively)
	 * @return the remote URL, or {@code null} if the remote is not configured
	 */
	public static String getConfigRemoteURL(String localRepositoryName, String remoteName) {
		try (Git thisGit = Git.open(new File(Utility.normalizePath(localRepositoryName)));
				Repository thisRepo = thisGit.getRepository()) {
			String[] remNames = thisRepo.getRemoteNames().toArray(new String[] {});
			for (int remIndex = 0; remIndex < remNames.length; remIndex++) {
				String remName = remNames[remIndex] + "";
				if (remName.equalsIgnoreCase(remoteName)) {
					String url = thisRepo.getConfig().getString("remote", remName, "url");
					return url;
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to read configured remote URL: {}", e.getMessage(), e);
		}

		return null;
	}

	/**
	 * Finds a commit in the given repository whose object id contains the supplied
	 * id fragment. Walks the full commit log (all refs) and returns the first
	 * commit whose id string contains {@code id}, or {@code null} if none match.
	 *
	 * @param gitFolder path to the local git working directory
	 * @param id        a full or partial commit id to search for within commit
	 *                  object ids
	 * @return the matching {@link RevCommit}, or {@code null} if no commit matches
	 * @throws Exception if the repository cannot be opened or the log cannot be
	 *                   read
	 */
	// find a particular commit in the folder
	public static RevCommit findCommit(String gitFolder, String id) throws Exception {
		RevCommit comm = null;
		try (Git thisGit = Git.open(new File(gitFolder))) {
			LogCommand lg = thisGit.log().all();
			Iterator<RevCommit> commits = lg.call().iterator();
			while (commits.hasNext()) {
				comm = commits.next();
				if ((comm.getId() + "").contains(id)) {
					break;
				}
				comm = null;
			}
		}
		return comm;
	}

	/**
	 * Installs the SSL certificate for the host of the given repository URL into
	 * the local trust store. The host is parsed from {@code repoName} as a URI and
	 * any leading {@code www.} prefix is stripped before installing the
	 * certificate.
	 *
	 * @param repoName the repository URL from which the host/domain is extracted
	 * @return {@code true} if the certificate was installed, {@code false} if the
	 *         URL was malformed or the install failed
	 */
	// install the certificate
	public static boolean addCertForDomain(String repoName) {
		try {
			URI uri = new URI(repoName);
			String domain = uri.getHost();
			domain = domain.startsWith("www.") ? domain.substring(4) : domain;

			InstallCertNow.please(domain, null, null);
			return true;
		} catch (URISyntaxException use) {
			classLogger.error("Failed to install certificate for repository domain: {}", use.getMessage(), use);
			return false;
		} catch (Exception e) {
			classLogger.error("Failed to install certificate for repository domain: {}", e.getMessage(), e);
			return false;
		}
	}

	/**
	 * Lists the commit history of the repository as rows of raw values. The first
	 * row is a header ({@code date}, {@code user}, {@code message}, {@code id}) and
	 * each subsequent row holds the commit time, author name, full commit message,
	 * and the first 6 characters of the commit id. When {@code fileName} is
	 * non-null the log is restricted to commits touching that path. Returns
	 * whatever has been collected (logging) if the repository or log cannot be
	 * read.
	 *
	 * @param gitFolder path to the local git working directory
	 * @param fileName  optional repo-relative path to limit history to a single
	 *                  file; pass {@code null} to list all commits
	 * @return a list of rows; the first row is the header and the rest are commit
	 *         records; never {@code null}
	 */
	public static List<List<Object>> listCommits(String gitFolder, String fileName) {
		// list of lists
		List<List<Object>> builder = new ArrayList<>();
		try (Git thisGit = Git.open(new File(gitFolder))) {
			// add header row
			List<Object> row = new ArrayList<>();
			row.add("date");
			row.add("user");
			row.add("message");
			row.add("id");
			builder.add(row);
			LogCommand lg = null;
			if (fileName != null) {
				lg = thisGit.log().addPath(fileName).all();
			} else {
				lg = thisGit.log().all();
			}

			Iterator<RevCommit> commits = lg.call().iterator();
			while (commits.hasNext()) {
				RevCommit comm = commits.next();
				row = new ArrayList<>();
				row.add(comm.getCommitTime());
				row.add(comm.getAuthorIdent().getName());
				row.add(comm.getFullMessage());
				row.add(comm.toObjectId().toString().replace("commit ", "").substring(0, 6));
				builder.add(row);
			}
		} catch (NoHeadException nhe) {
			classLogger.error("Failed to list commit history: {}", nhe.getMessage(), nhe);
		} catch (IOException ioe) {
			classLogger.error("Failed to list commit history: {}", ioe.getMessage(), ioe);
		} catch (GitAPIException e) {
			classLogger.error("Failed to list commit history: {}", e.getMessage(), e);
		}

		return builder;
	}

	/**
	 * Returns the commit history of the repository as a list of maps with commit
	 * metadata. Each map contains a formatted {@code date} (via
	 * {@link GitAssetUtils#getDate(int)}), the author {@code user}, the full commit
	 * {@code message}, and the first 6 characters of the commit {@code id}. When
	 * {@code fileName} is non-null and non-empty the log is restricted to commits
	 * touching that path. Returns whatever has been collected (logging) if the
	 * repository or log cannot be read.
	 *
	 * @param gitFolder path to the local git working directory
	 * @param fileName  optional repo-relative path to limit history to a single
	 *                  file; pass {@code null} or empty to list all commits
	 * @return a list of per-commit metadata maps; never {@code null}
	 */
	public static List<Map<String, Object>> getCommits(String gitFolder, String fileName) {
		// list of lists
		List<Map<String, Object>> commitList = new ArrayList<>();
		try (Git thisGit = Git.open(new File(gitFolder))) {

			LogCommand lg = null;
			if (fileName != null && !fileName.isEmpty()) {
				lg = thisGit.log().addPath(fileName).all();
			} else {
				lg = thisGit.log().all();
			}
			Iterator<RevCommit> commits = lg.call().iterator();

			while (commits.hasNext()) {
				RevCommit comm = commits.next();
				Map<String, Object> commitMap = new HashMap();
				commitMap.put("date", GitAssetUtils.getDate(comm.getCommitTime()));
				commitMap.put("user", comm.getAuthorIdent().getName());
				commitMap.put("message", comm.getFullMessage());
				commitMap.put("id", comm.toObjectId().toString().replace("commit ", "").substring(0, 6));
				commitList.add(commitMap);
			}
		} catch (NoHeadException nhe) {
			classLogger.error("Failed to read commit details: {}", nhe.getMessage(), nhe);
		} catch (IOException ioe) {
			classLogger.error("Failed to read commit details: {}", ioe.getMessage(), ioe);
		} catch (GitAPIException e) {
			classLogger.error("Failed to read commit details: {}", e.getMessage(), e);
		}

		return commitList;
	}

	/**
	 * Reads the text content of a file from the repository, either from the working
	 * tree or from a specific commit. When {@code commId} is {@code null} the
	 * current file is read from disk under {@code gitFolder} (falling back to the
	 * legacy {@code version/assets/} location if the direct path is missing). When
	 * {@code commId} is provided the matching commit is resolved via
	 * {@link #findCommit(String, String)} and the blob is read from that commit's
	 * tree. All opened resources (readers, repository, object reader) are closed in
	 * a finally block. Returns {@code null} if the file does not exist or reading
	 * fails (errors are logged).
	 *
	 * @param commId    full or partial commit id to read the file from;
	 *                  {@code null} reads the current working-tree copy
	 * @param fileName  repo-relative path of the file to read
	 * @param gitFolder path to the local git working directory
	 * @return the file content as a string, or {@code null} if unavailable
	 */
	// gets a particular file
	// showing file content for a particular ID
	// this will be utilized where the user goes
	// ok what did the user abcd check in for this file without the needing to
	// revert / reset
	// frankly we should have a way for the user to go back and forth
	public static String getFile(String commId, String fileName, String gitFolder) {
		String output = null;
		FileReader fis = null;
		BufferedReader br = null;
		Git thisGit = null;
		ObjectReader objectReader = null;
		try {

			RevCommit comm = null;
			if (commId == null) {
				// there is a good possibility the user has not saved this !?
				File file = new File(gitFolder + "/" + fileName);

				if (!file.exists()) {
					// this could be an old insight try with the /version/assets
					file = new File(gitFolder + "/version/assets/" + fileName);
				}

				if (file.exists()) {
					fis = new FileReader(file);
					br = new BufferedReader(fis);
					StringBuffer buff = new StringBuffer();
					String temp = null;
					while ((temp = br.readLine()) != null) {
						buff.append(temp).append("\n");
					}

					output = buff.toString();
				}
			} else {
				thisGit = Git.open(new File(gitFolder));

				comm = findCommit(gitFolder, commId);

				TreeWalk treeWalk = TreeWalk.forPath(thisGit.getRepository(), fileName, comm.getTree());
				ObjectId blobId = treeWalk.getObjectId(0);

				objectReader = thisGit.getRepository().newObjectReader();
				ObjectLoader objectLoader = objectReader.open(blobId);
				byte[] bytes = objectLoader.getBytes();
				output = new String(bytes);
			}
		} catch (MissingObjectException moe) {
			classLogger.error("Failed to read file content from commit: {}", moe.getMessage(), moe);
		} catch (IncorrectObjectTypeException iote) {
			classLogger.error("Failed to read file content from commit: {}", iote.getMessage(), iote);
		} catch (CorruptObjectException coe) {
			classLogger.error("Failed to read file content from commit: {}", coe.getMessage(), coe);
		} catch (LargeObjectException loe) {
			classLogger.error("Failed to read file content from commit: {}", loe.getMessage(), loe);
		} catch (IOException ioe) {
			classLogger.error("Failed to read file content from commit: {}", ioe.getMessage(), ioe);
		} catch (Exception e) {
			classLogger.error("Failed to read file content from commit: {}", e.getMessage(), e);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					classLogger.error("Failed to read file content from commit: {}", e.getMessage(), e);
				}
			}
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					classLogger.error("Failed to read file content from commit: {}", e.getMessage(), e);
				}
			}
			if (thisGit != null) {
				thisGit.close();
			}
			if (objectReader != null) {
				objectReader.close();
			}
		}

		return output;

	}

	/**
	 * Reads the raw bytes of a file from the repository, either from the working
	 * tree or from a specific commit. When {@code commId} is {@code null} the
	 * current file is read from disk under {@code gitFolder}. When {@code commId}
	 * is provided the matching commit is resolved via
	 * {@link #findCommit(String, String)} and the blob is read from that commit's
	 * tree. The repository and object reader are closed in a finally block. Returns
	 * {@code null} if the file does not exist or reading fails (errors are logged).
	 *
	 * @param commId    full or partial commit id to read the file from;
	 *                  {@code null} reads the current working-tree copy
	 * @param fileName  repo-relative path of the file to read
	 * @param gitFolder path to the local git working directory
	 * @return the file content as a byte array, or {@code null} if unavailable
	 */
	// gets a particular file
	// showing file content for a particular ID
	// this will be utilized where the user goes
	// ok what did the user abcd check in for this file without the needing to
	// revert / reset
	// frankly we should have a way for the user to go back and forth
	public static byte[] getBinary(String commId, String fileName, String gitFolder) {
		byte[] bytes = null;
		Git thisGit = null;
		ObjectReader objectReader = null;
		try {

			RevCommit comm = null;
			if (commId == null) {
				// there is a good possibility the user has not saved this !?
				File file = new File(gitFolder + "/" + fileName);
				if (file.exists()) {
					bytes = FileUtils.readFileToByteArray(file);
				}
			} else {
				thisGit = Git.open(new File(gitFolder));

				comm = findCommit(gitFolder, commId);

				TreeWalk treeWalk = TreeWalk.forPath(thisGit.getRepository(), fileName, comm.getTree());
				ObjectId blobId = treeWalk.getObjectId(0);

				objectReader = thisGit.getRepository().newObjectReader();
				ObjectLoader objectLoader = objectReader.open(blobId);
				bytes = objectLoader.getBytes();
			}
		} catch (MissingObjectException moe) {
			classLogger.error("Failed to read binary file content from commit: {}", moe.getMessage(), moe);
		} catch (IncorrectObjectTypeException iote) {
			classLogger.error("Failed to read binary file content from commit: {}", iote.getMessage(), iote);
		} catch (CorruptObjectException coe) {
			classLogger.error("Failed to read binary file content from commit: {}", coe.getMessage(), coe);
		} catch (LargeObjectException loe) {
			classLogger.error("Failed to read binary file content from commit: {}", loe.getMessage(), loe);
		} catch (IOException ioe) {
			classLogger.error("Failed to read binary file content from commit: {}", ioe.getMessage(), ioe);
		} catch (Exception e) {
			classLogger.error("Failed to read binary file content from commit: {}", e.getMessage(), e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
			if (objectReader != null) {
				objectReader.close();
			}
		}

		return bytes;

	}

	/**
	 * Stages all untracked and modified files in the repository. Each candidate
	 * file is added unless {@code ignoreTheIgnoreFiles} is {@code false} and
	 * {@link GitUtils#isIgnore(String)} reports it as ignored. The add command is
	 * only invoked when at least one file was matched. Opens the repository and
	 * closes it before returning.
	 *
	 * @param gitFolder            path to the local git working directory
	 * @param ignoreTheIgnoreFiles when {@code true}, ignore filtering is bypassed
	 *                             and every untracked/modified file is staged; when
	 *                             {@code false}, files matching the ignore rules
	 *                             are skipped
	 * @throws IllegalArgumentException if the repository cannot be opened or files
	 *                                  cannot be staged
	 */
	public static void addAllFiles(String gitFolder, boolean ignoreTheIgnoreFiles) {
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(gitFolder));
			status = thisGit.status().call();
		} catch (IOException | NoWorkTreeException | GitAPIException e) {
			classLogger.error("Failed to stage files in git repository: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + gitFolder);
		}

		AddCommand ac = thisGit.add();
		boolean added = false;

		// get new files
		Iterator<String> upFiles = status.getUntracked().iterator();
		while (upFiles.hasNext()) {
			String daFile = upFiles.next();
			if (ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
				added = true;
				ac.addFilepattern(daFile);
			}
		}

		// get the modified files
		Iterator<String> modFiles = status.getModified().iterator();
		while (modFiles.hasNext()) {
			String daFile = modFiles.next();
			if (ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
				added = true;
				ac.addFilepattern(daFile);
			}
		}

		if (added) {
			try {
				ac.call();
			} catch (GitAPIException e) {
				classLogger.error("Failed to stage files in git repository: {}", e.getMessage(), e);
				throw new IllegalArgumentException("Unable to add files to Git directory at " + gitFolder);
			}
		}

		thisGit.close();
	}

	/**
	 * Stages a specific set of files in the given repository. Each pattern is
	 * trimmed to the portion after a {@code version} segment (when present) and
	 * then normalized via {@link #normalizeGitFilePattern(String)} before being
	 * added. No-op when {@code files} is {@code null} or empty. Opens the
	 * repository and closes it before returning; add failures are logged rather
	 * than thrown.
	 *
	 * @param localRepository path to the local git working directory
	 * @param files           list of file paths/patterns to stage
	 * @throws IllegalArgumentException if the repository cannot be opened
	 */
	public static void addSpecificFiles(String localRepository, List<String> files) {
		if (files == null || files.isEmpty()) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(Utility.normalizePath(localRepository)));
		} catch (IOException e) {
			classLogger.error("Unable to connect to Git directory at {}", localRepository, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}
		AddCommand ac = thisGit.add();
		List<String> normalizedPatterns = new ArrayList<>();
		for (String daFile : files) {
			if (daFile.contains("version")) {
				daFile = daFile.substring(daFile.indexOf("version") + 8);
			}
			daFile = normalizeGitFilePattern(daFile);
			ac.addFilepattern(daFile);
			normalizedPatterns.add(daFile);
		}
		classLogger.debug("Git add file patterns {} in repo {}", normalizedPatterns, localRepository);
		try {
			ac.call();
		} catch (GitAPIException e) {
			classLogger.error("Failed to stage files {} in repo {}", normalizedPatterns, localRepository, e);
		}
		thisGit.close();
	}

	/**
	 * Stages a specific set of files in the given repository. Each file's absolute
	 * path is trimmed to the portion after a {@code version} segment (when present)
	 * and then normalized via {@link #normalizeGitFilePattern(String)} before being
	 * added. No-op when {@code files} is {@code null} or empty. Opens the
	 * repository and closes it before returning; add failures are logged rather
	 * than thrown.
	 *
	 * @param localRepository path to the local git working directory
	 * @param files           array of files to stage
	 * @throws IllegalArgumentException if the repository cannot be opened
	 */
	public static void addSpecificFiles(String localRepository, File[] files) {
		if (files == null || files.length == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			classLogger.error("Unable to connect to Git directory at {}", localRepository, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}
		AddCommand ac = thisGit.add();
		List<String> normalizedPatterns = new ArrayList<>();
		for (File f : files) {
			String daFile = f.getAbsolutePath();
			if (daFile.contains("version")) {
				daFile = daFile.substring(daFile.indexOf("version") + 8);
			}
			daFile = normalizeGitFilePattern(daFile);
			ac.addFilepattern(daFile);
			normalizedPatterns.add(daFile);
		}
		classLogger.debug("Git add file patterns {} in repo {}", normalizedPatterns, localRepository);
		try {
			ac.call();
		} catch (GitAPIException e) {
			classLogger.error("Failed to stage files {} in repo {}", normalizedPatterns, localRepository, e);
		}
		thisGit.close();
	}

	/**
	 * Convenience overload of
	 * {@link #commitAddedFiles(String, String, String, String)} that commits with a
	 * {@code null} (auto-generated) message and the default SEMOSS author/email.
	 *
	 * @param gitFolder path to the local git working directory
	 */
	public static void commitAddedFiles(String gitFolder) {
		commitAddedFiles(gitFolder, null);
	}

	/**
	 * Convenience overload of
	 * {@link #commitAddedFiles(String, String, String, String)} that commits with
	 * the given message and the default SEMOSS author/email.
	 *
	 * @param gitFolder path to the local git working directory
	 * @param message   the commit message; {@code null} or empty triggers an
	 *                  auto-generated dated message
	 */
	public static void commitAddedFiles(String gitFolder, String message) {
		commitAddedFiles(gitFolder, message, null, null);
	}

	/**
	 * Convenience overload of
	 * {@link #commitAddedFiles(String, String, String, String)} that derives the
	 * author name and email from the user's primary-login {@link AccessToken}.
	 *
	 * @param gitFolder path to the local git working directory
	 * @param message   the commit message; {@code null} or empty triggers an
	 *                  auto-generated dated message
	 * @param user      the user whose primary-login access token supplies the
	 *                  commit author name and email
	 */
	public static void commitAddedFiles(String gitFolder, String message, User user) {
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getResolvedUsername();
		commitAddedFiles(gitFolder, message, author, email);
	}

	/**
	 * Commits the already-staged changes in the repository. First checks the status
	 * and skips the commit (logging a warning) when there are no staged
	 * added/changed/removed entries. Falls back to an auto-generated dated message,
	 * the {@code SEMOSS} author, and {@code semoss@semoss.org} email when the
	 * respective arguments are {@code null} or empty. Opens the repository and
	 * closes it before returning; commit failures are logged rather than thrown.
	 *
	 * @param gitFolder path to the local git working directory
	 * @param message   the commit message; {@code null} or empty triggers an
	 *                  auto-generated dated message
	 * @param author    the commit author name; {@code null} or empty defaults to
	 *                  {@code SEMOSS}
	 * @param email     the commit author email; {@code null} or empty defaults to
	 *                  {@code semoss@semoss.org}
	 * @throws IllegalArgumentException if the repository cannot be opened
	 */
	public static void commitAddedFiles(String gitFolder, String message, String author, String email) {
		try (Git thisGit = Git.open(new File(gitFolder))) {
			try {
				// Check if there are actually staged changes before committing
				Status status = thisGit.status().call();
				boolean hasStagedChanges = !status.getAdded().isEmpty() || !status.getChanged().isEmpty()
						|| !status.getRemoved().isEmpty();

				if (!hasStagedChanges) {
					classLogger.warn("Skipping commit in {} - no staged changes to commit", gitFolder);
					return;
				}

				if (message == null || message.isEmpty()) {
					message = GitUtils.getDateMessage("Commited on.. ");
				}
				if (author == null || author.isEmpty()) {
					author = "SEMOSS";
				}
				if (email == null || email.isEmpty()) {
					email = "semoss@semoss.org";
				}

				CommitCommand cc = thisGit.commit();
				cc.setMessage(message).setAuthor(author, email).call();
				classLogger.debug("Committed to {} with message '{}'", gitFolder, message);
			} catch (GitAPIException e) {
				classLogger.error("Failed to commit in {}", gitFolder, e);
			}
		} catch (IOException e) {
			classLogger.error("Unable to connect to Git directory at {}", gitFolder, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + gitFolder);
		}

	}

	/**
	 * Convenience overload of
	 * {@link #addAllChangesAndCommit(String, boolean, String, String, String)} that
	 * uses a {@code null} (auto-generated) message and the default SEMOSS
	 * author/email.
	 *
	 * @param gitFolder            path to the local git working directory
	 * @param ignoreTheIgnoreFiles when {@code true}, ignore filtering is bypassed
	 *                             when staging changes
	 */
	public static void addAllChangesAndCommit(String gitFolder, boolean ignoreTheIgnoreFiles) {
		addAllChangesAndCommit(gitFolder, ignoreTheIgnoreFiles, null, null, null);
	}

	/**
	 * Convenience overload of
	 * {@link #addAllChangesAndCommit(String, boolean, String, String, String)} that
	 * uses the given message and the default SEMOSS author/email.
	 *
	 * @param gitFolder            path to the local git working directory
	 * @param ignoreTheIgnoreFiles when {@code true}, ignore filtering is bypassed
	 *                             when staging changes
	 * @param message              the commit message; {@code null} or empty
	 *                             triggers an auto-generated dated message
	 */
	public static void addAllChangesAndCommit(String gitFolder, boolean ignoreTheIgnoreFiles, String message) {
		addAllChangesAndCommit(gitFolder, ignoreTheIgnoreFiles, message, null, null);
	}

	/**
	 * Convenience overload of
	 * {@link #addAllChangesAndCommit(String, boolean, String, String, String)} that
	 * derives the author name and email from the user's primary-login
	 * {@link AccessToken}.
	 *
	 * @param gitFolder            path to the local git working directory
	 * @param ignoreTheIgnoreFiles when {@code true}, ignore filtering is bypassed
	 *                             when staging changes
	 * @param message              the commit message; {@code null} or empty
	 *                             triggers an auto-generated dated message
	 * @param user                 the user whose primary-login access token
	 *                             supplies the commit author name and email
	 */
	public static void addAllChangesAndCommit(String gitFolder, boolean ignoreTheIgnoreFiles, String message,
			User user) {
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		addAllChangesAndCommit(gitFolder, ignoreTheIgnoreFiles, message, accessToken.getResolvedUsername(),
				accessToken.getEmail());
	}

	/**
	 * Stages all changes in the repository and commits them in a single call. Adds
	 * untracked and modified files, and removes (cached) missing/deleted files,
	 * applying ignore filtering unless {@code ignoreTheIgnoreFiles} is
	 * {@code true}. After staging it re-checks status and skips the commit (logging
	 * a warning) when nothing is staged. Falls back to an auto-generated dated
	 * message, the {@code SEMOSS} author, and {@code semoss@semoss.org} email when
	 * the respective arguments are {@code null} or empty. Opens the repository and
	 * closes it before returning.
	 *
	 * @param gitFolder            path to the local git working directory
	 * @param ignoreTheIgnoreFiles when {@code true}, ignore filtering is bypassed
	 *                             so all changed files are staged; when
	 *                             {@code false}, files matching the ignore rules
	 *                             are skipped
	 * @param message              the commit message; {@code null} or empty
	 *                             triggers an auto-generated dated message
	 * @param author               the commit author name; {@code null} or empty
	 *                             defaults to {@code SEMOSS}
	 * @param email                the commit author email; {@code null} or empty
	 *                             defaults to {@code semoss@semoss.org}
	 * @throws IllegalArgumentException if the repository cannot be opened or the
	 *                                  add/commit fails
	 */
	public static void addAllChangesAndCommit(String gitFolder, boolean ignoreTheIgnoreFiles, String message,
			String author, String email) {
		try (Git thisGit = Git.open(new File(gitFolder))) {
			Status status = thisGit.status().call();

			AddCommand ac = thisGit.add();
			boolean stagedAnyAdd = false;
			Iterator<String> upFiles = status.getUntracked().iterator();
			while (upFiles.hasNext()) {
				String daFile = upFiles.next();
				if (ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
					ac.addFilepattern(daFile);
					stagedAnyAdd = true;
				}
			}
			Iterator<String> modFiles = status.getModified().iterator();
			while (modFiles.hasNext()) {
				String daFile = modFiles.next();
				if (ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
					ac.addFilepattern(daFile);
					stagedAnyAdd = true;
				}
			}
			if (stagedAnyAdd) {
				ac.call();
			}

			RmCommand rc = thisGit.rm().setCached(true);
			boolean stagedAnyRm = false;
			Iterator<String> delFiles = status.getMissing().iterator();
			while (delFiles.hasNext()) {
				String daFile = delFiles.next();
				if (ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
					rc.addFilepattern(daFile);
					stagedAnyRm = true;
				}
			}
			if (stagedAnyRm) {
				rc.call();
			}

			Status post = thisGit.status().call();
			boolean hasStagedChanges = !post.getAdded().isEmpty() || !post.getChanged().isEmpty()
					|| !post.getRemoved().isEmpty();
			if (!hasStagedChanges) {
				classLogger.warn("Skipping commit in {} no staged changes to commit", gitFolder);
				return;
			}

			if (message == null || message.isEmpty()) {
				message = GitUtils.getDateMessage("Commited on.. ");
			}
			if (author == null || author.isEmpty()) {
				author = "SEMOSS";
			}
			if (email == null || email.isEmpty()) {
				email = "semoss@semoss.org";
			}

			thisGit.commit().setMessage(message).setAuthor(author, email).call();
			classLogger.debug("Committed all changes to {} with message '{}'", gitFolder, message);
		} catch (IOException | GitAPIException e) {
			classLogger.error("Failed to add+commit all changes in {}", gitFolder, e);
			throw new IllegalArgumentException("Unable to add+commit all changes in Git directory at " + gitFolder);
		}
	}

	/**
	 * Initializes a new git repository in the given folder. First writes a default
	 * {@code .gitignore} via {@link #addGitIgnore(String)}, then runs
	 * {@code git init} on the folder and immediately opens and closes it to
	 * finalize creation. When running in cluster mode the folder is validated via
	 * {@link ClusterUtil#validateFolder(String)}. Failures are logged rather than
	 * thrown.
	 *
	 * @param folder path to the directory to initialize as a git repository
	 */
	public static void init(String folder) {
		try {
			addGitIgnore(folder);
			Git.init().setDirectory(new File(folder)).call();
			Git.open(new File(folder)).close();
			if (ClusterUtil.IS_CLUSTER) {
				ClusterUtil.validateFolder(folder);
			}
		} catch (IllegalStateException ise) {
			classLogger.error("Failed to initialize git repository: {}", ise.getMessage(), ise);
		} catch (GitAPIException gae) {
			classLogger.error("Failed to initialize git repository: {}", gae.getMessage(), gae);
		} catch (IOException e) {
			classLogger.error("Failed to initialize git repository: {}", e.getMessage(), e);
		}
	}

	/**
	 * Creates a {@code .gitignore} file in the given folder pre-populated with a
	 * standard set of ignore patterns (OS metadata, log/cache/temp/pid files,
	 * compiled python, npm/yarn debug logs, and {@code node_modules}). Failures are
	 * logged rather than thrown.
	 *
	 * @param folder path to the directory in which to create the {@code .gitignore}
	 */
	public static void addGitIgnore(String folder) {
		// @formatter:off
		String[] ignoreList = new String[] {
				".DS_Store",
				".AppleDouble",
				".LSOverride",
				"*.log",
				"*.cache",
				"*.tmp",
				"*.pid",
				"*.pyc",
				"npm-debug.log*",
				"yarn-debug.log*",
				"*/Temp/*",
				"**/node_modules/"
		};
		// @formatter:on

		File f = new File(folder, ".gitignore");
		try (FileWriter fw = new FileWriter(f); BufferedWriter bw = new BufferedWriter(fw);) {
			f.createNewFile();
			for (String ignore : ignoreList) {
				bw.write(ignore);
				bw.newLine();
			}
		} catch (Exception ex) {
			classLogger.error("Failed to create gitignore file: {}", ex.getMessage(), ex);
		}
	}

	/**
	 * Normalizes a git file pattern to be repo-relative with forward slashes, no
	 * leading slash, and no repeated slashes.
	 * 
	 * @param pattern the raw file pattern
	 * @return the normalized pattern suitable for JGit AddCommand/RmCommand
	 */
	private static String normalizeGitFilePattern(String pattern) {
		pattern = pattern.replace("\\", "/");
		while (pattern.contains("//")) {
			pattern = pattern.replace("//", "/");
		}
		if (pattern.startsWith("/")) {
			pattern = pattern.substring(1);
		}
		return pattern;
	}

}
