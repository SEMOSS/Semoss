package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RemoteRemoveCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.kohsuke.github.GHCreateRepositoryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;

import prerna.util.Utility;

public class GitRepoUtils {

	public static final String DUAL = "DUAL";
	public static final String SUBSCRIBE = "SUBSCRIBE";
	public static final String PUBLISH = "PUBLISH";

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitRepoUtils() {

	}

	/**
	 * Generate a version folder within an app and init a local Git repo
	 * @param appFolder
	 */
	public static void makeLocalAppGitVersionFolder(String appFolder) {
		File appDir = new File(appFolder + "/version");
		if(!appDir.exists()) {
			appDir.mkdirs();
		}
		try {
			Git.init().setDirectory(appDir).call();
			Git.open(appDir).close();
		} catch (IllegalStateException | GitAPIException | IOException e) {
			throw new IllegalArgumentException("Error in initializing local Git repository");
		}
	}

	/**
	 * 
	 * @param gh
	 * @param username
	 * @param repoName
	 */
	public static void makeRemoteRepository(GitHub gh, String username, String repoName) {
		GHCreateRepositoryBuilder ghr = gh.createRepository(repoName)
				.description(GitUtils.getDateMessage("Repository created on ") + " By user " + username);
		try {
			ghr.autoInit(false).create();
		} catch (IOException e) {
			throw new IllegalArgumentException("Error with creating remote repository at " + username + "/" + repoName);
		}
	}

	/**
	 * 
	 * @param localRepository
	 * @param repositoryName
	 */
	public static void removeRemote(String localRepository, String repositoryName) {
		try {
			StoredConfig config = Git.open(new File(localRepository)).getRepository().getConfig();
			config.unsetSection("remote", repositoryName);
			config.save();
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to drop remote");
		}
	}

	/**
	 * Delete a repository
	 * @param repositoryName
	 * @param username
	 * @param password
	 * @throws IOException
	 */
	public static void deleteRemoteRepository(String repositoryName, String username, String password) {
		String repoName = repositoryName.split("/")[1];
		if(checkRemoteRepository(repoName, username, password)) {
			GitHub gh = GitUtils.login(username, password);
			GHRepository ghr = null;
			try {
				ghr = gh.getRepository(repositoryName);
				ghr.delete();
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Unalbe to delete remote repository at " + repositoryName);
			}
		}
	}
	
	/**
	 * 
	 * @param localRepository
	 * @param repositoryName
	 */
	public static void deleteRemoteRepositorySettings(String localRepository, String repositoryName) {
		try {
			File file = new File(localRepository);
			Git gFile = Git.open(file);
			RemoteRemoveCommand remover = gFile.remoteRemove();
			remover.setName(repositoryName.split("/")[1]);
			remover.call();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GitAPIException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Check if a repo exists
	 * @param repositoryName
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public static boolean checkRemoteRepository(String repositoryName, String username, String password) {
		GitHubClient client = GitHubClient.createClient("https://github.com");
		if(password != null) {
			client.setCredentials(username, password);
		}
		RepositoryService service = new RepositoryService(client);
		boolean returnVal = true;
		try {
			service.getRepository(username, repositoryName);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Cannot find repo at " + repositoryName + " for username " + username);
		}
		return returnVal;
	}

	/**
	 * 
	 * @param localRepository
	 * @param username
	 * @param repoName
	 */
	public static void addRemote(String localRepository, String username, String repoName) {
		StoredConfig config;
		try {
			config = Git.open(new File(localRepository)).getRepository().getConfig();
			config.setString("remote", repoName , "url", "https://github.com/" + username + "/" + repoName);
			config.setString("remote", repoName , "fetch", "+refs/heads/*:refs/remotes/" + repoName + "/*");
			config.save();
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with adding the remote repository");
		}
	}

	/**
	 * Switch to a specific git remote
	 * @param localRepo
	 * @param remoteRepo
	 * @param userName
	 * @param password
	 */
	public static void fetchRemote(String localRepo, String remoteRepo, String userName, String password) {
		File file = new File(localRepo);
		RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/" + remoteRepo +"/master");
		List <RefSpec> refList = new ArrayList<RefSpec>();
		refList.add(spec);
		CredentialsProvider cp = null;
		if(userName != null && password != null && !userName.isEmpty() && !password.isEmpty()) {
			cp = new UsernamePasswordCredentialsProvider(userName, password);
		}
		Git thisGit;
		try {
			thisGit = Git.open(file);
			if(cp != null) {
				thisGit.fetch().setCredentialsProvider(cp).setRemote(remoteRepo).call();
			} else {
				thisGit.fetch().setRemote(remoteRepo).call();
			}
			thisGit.close();
		} catch (IOException | GitAPIException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with fetching the remote respository at " + remoteRepo);
		}
	}


	/**
	 * 
	 * Configuration based remote methods
	 * 
	 */

	/**
	 * Get the list of remote configurations associated with an app directory
	 * Get the url
	 * Get the namespace/appName
	 * Get the type -> dual or subscript
	 * @param localRepositoryName
	 * @return
	 */
	public static List<Map<String, String>> listConfigRemotes(String localRepositoryName) {
		List<Map<String, String>> returnList = new Vector<Map<String, String>>();
		try {
			File file = new File(localRepositoryName);
			Repository thisRepo = Git.open(file).getRepository();

			String[] remNames = thisRepo.getRemoteNames().toArray(new String[]{});
			for(int remIndex = 0; remIndex < remNames.length; remIndex++) {
				String remName = remNames[remIndex] +"";
				String url = thisRepo.getConfig().getString("remote", remName, "url");
				String upstream = thisRepo.getConfig().getString(remName, "upstream",  "url");

				Map<String, String> remoteMap = new Hashtable<String, String>();
				remoteMap.put("url", url);
				String appName = Utility.getClassName(url) + "/" + Utility.getInstanceName(url);
				remoteMap.put("name", appName);
				if(upstream != null && upstream.equalsIgnoreCase("DEFUNCT")) {
					remoteMap.put("type", SUBSCRIBE);
				} else {
					remoteMap.put("type", DUAL);
				}
				System.out.println("We have remote with details " + remoteMap);
				returnList.add(remoteMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return returnList;
	}

	/**
	 * Get the list of repos for a given user
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<String> listRemotesForUser(String username, String password) {
		List<String> remoteRepos = new Vector<String>();
		GitHubClient client = GitHubClient.createClient("https://github.com");
		client.setCredentials(username, password);
		RepositoryService service = new RepositoryService(client);
		try {
			List<org.eclipse.egit.github.core.Repository> repList = service.getRepositories();
			for(int repIndex = 0;repIndex < repList.size();repIndex++) {
				remoteRepos.add(repList.get(repIndex).getName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return remoteRepos;
	}
}
