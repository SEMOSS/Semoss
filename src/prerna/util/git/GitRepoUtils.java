package prerna.util.git;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.net.ssl.SSLHandshakeException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.RemoteRemoveCommand;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.kohsuke.github.GHCreateRepositoryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.security.InstallCertNow;
import prerna.util.Constants;
import prerna.util.Utility;

public class GitRepoUtils {

	private static final Logger logger = LogManager.getLogger(GitRepoUtils.class);

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
	public static void makeLocalDatabaseGitVersionFolder(String appFolder) {
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
	
	public static void makeRemoteRepository(GitHub gh, String username, String repoName) {
		int attempt = 1;
		makeRemoteRepository(gh, username, repoName, attempt);
	}


	/**
	 * 
	 * @param gh
	 * @param username
	 * @param repoName
	 */
	public static void makeRemoteRepository(GitHub gh, String username, String repoName, int attempt) {
		if(attempt < 3) {
			try {
				GHCreateRepositoryBuilder ghr = gh.createRepository(repoName)
						.description(GitUtils.getDateMessage("Repository created on ") + " By user " + username)
						.autoInit(false);
				ghr.create();
			} catch(SSLHandshakeException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				makeRemoteRepository(gh, username, repoName, attempt);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error with creating remote repository at " + username + "/" + repoName);
			}
		}
	}
	
	public static void removeRemote(String localRepository, String repositoryName) {
		int attempt = 1;
		removeRemote(localRepository, repositoryName, attempt);
	}


	/**
	 * 
	 * @param localRepository
	 * @param repositoryName
	 */
	public static void removeRemote(String localRepository, String repositoryName, int attempt) {
		if(attempt < 3)
		{
			Git thisGit = null;
			Repository thisRepo = null;
			try {
				thisGit = Git.open(new File(localRepository));
				thisRepo = thisGit.getRepository();
				StoredConfig config = thisRepo.getConfig();
				config.unsetSection("remote", repositoryName);
				config.save();
			} catch(HttpException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				removeRemote(localRepository, repositoryName, attempt);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to drop remote");
			} finally {
				if(thisRepo != null) {
					thisRepo.close();
				}
				if(thisGit != null) {
					thisGit.close();
				}
			}
		}
	}
	
	public static void deleteRemoteRepository(String repositoryName, String username, String password) {
		int attempt = 1;
		deleteRemoteRepository(repositoryName, username, password, attempt);
	}


	/**
	 * Delete a repository
	 * @param repositoryName
	 * @param username
	 * @param password
	 * @throws IOException
	 */
	public static void deleteRemoteRepository(String repositoryName, String username, String password, int attempt) {
		if(attempt < 3)
		{
			String repoName = repositoryName.split("/")[1];
			if(checkRemoteRepository(repoName, username, password)) {
				GitHub gh = GitUtils.login(username, password);
				GHRepository ghr = null;
				try {
					ghr = gh.getRepository(repositoryName);
					ghr.delete();
				} catch(HttpException ex) {
					logger.error(Constants.STACKTRACE, ex);
					try {
						InstallCertNow.please("github.com", null, null);
					} catch (Exception e) {
						logger.error(Constants.STACKTRACE, e);
					}
					attempt = attempt + 1;
					deleteRemoteRepository(repositoryName, username, password, attempt);
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to delete remote repository at " + repositoryName);
				}
			}
		}
	}
	
	/**
	 * 
	 * @param localRepository
	 * @param repositoryName
	 */
	public static void deleteRemoteRepositorySettings(String localRepository, String repositoryName) {
		File file = new File(localRepository);

		try (Git gFile = Git.open(file)){
			
			RemoteRemoveCommand remover = gFile.remoteRemove();
			remover.setName(repositoryName.split("/")[1]);
			remover.call();
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	public static boolean checkRemoteRepository(String repositoryName, String username, String password) {
		int attempt = 1;
		return checkRemoteRepository(repositoryName, username, password, attempt);
	}

	/**
	 * Check if a repo exists
	 * @param repositoryName
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public static boolean checkRemoteRepository(String repositoryName, String username, String password, int attempt) {
		
		if(attempt < 3)
		{
			GitHubClient client = GitHubClient.createClient("https://github.com");
			if(password != null) {
				client.setCredentials(username, password);
			}
			RepositoryService service = new RepositoryService(client);
			boolean returnVal = true;
			try {
				service.getRepository(username, repositoryName);
			}catch(HttpException ex)
			{
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				checkRemoteRepository(repositoryName, username, password, attempt);
			} catch (Exception ex) {
				throw new IllegalArgumentException("Cannot find repo at " + repositoryName + " for username " + username);
			}
			return returnVal;
		}
		else
			return false;
	}

	public static boolean checkRemoteRepositoryO(String repositoryName, String oauth) {
		int attempt = 1;
		return checkRemoteRepositoryO(repositoryName, oauth, attempt);
	}

	/**
	 * Check if a repo exists
	 * @param repositoryName
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public static boolean checkRemoteRepositoryO(String repositoryName, String oauth, int attempt) {

		boolean returnVal = true;
		String [] repoParts = null;

		if(attempt < 3) {
			try {
				GitHubClient client = GitHubClient.createClient("https://github.com");
				if(oauth != null) {
					client.setOAuth2Token(oauth);
					GitHub gh = GitUtils.login(oauth);
					logger.debug(gh.getMyself().getLogin());
					if(!repositoryName.contains("/"))
						repositoryName = gh.getMyself().getLogin() + "/" + repositoryName ;
				}

				repoParts = repositoryName.split("/");

				RepositoryService service = new RepositoryService(client);

				service.getRepository(repoParts[0], repoParts[1]);

			} catch(HttpException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				checkRemoteRepositoryO(repositoryName, oauth, attempt);
			} catch (Exception ex) {
				if (repoParts != null) {
					throw new IllegalArgumentException("Cannot find repo at " + repositoryName + " for username " + repoParts[0]);
				} else {
					throw new IllegalArgumentException("Cannot find repo at " + repositoryName + " for null username ");
				}
			}
			return returnVal;
		}

		return false;
	}

	
	/**
	 * 
	 * @param localRepository
	 * @param username
	 * @param repoName
	 */
	public static void addRemote(String localRepository, String username, String repoName) {
		Git thisGit = null;
		Repository thisRepo = null;
		StoredConfig config;
		try {
			thisGit = Git.open(new File(localRepository));
			thisRepo = thisGit.getRepository();
			config = thisRepo.getConfig();
			config.setString("remote", repoName , "url", "https://github.com/" + username + "/" + repoName);
			config.setString("remote", repoName , "fetch", "+refs/heads/*:refs/remotes/" + repoName + "/*");
			config.save();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error with adding the remote repository");
		} finally {
			if(thisRepo != null) {
				thisRepo.close();
			}
			if(thisGit != null) {
				thisGit.close();
			}
		}
	}

	public static ProgressMonitor fetchRemote(String localRepo, String remoteRepo, String userName, String password) {
		int attempt = 1;
		return fetchRemote(localRepo, remoteRepo, userName, password, attempt);
	}

	/**
	 * Switch to a specific git remote
	 * @param localRepo
	 * @param remoteRepo
	 * @param userName
	 * @param password
	 */
	public static ProgressMonitor fetchRemote(String localRepo, String remoteRepo, String userName, String password, int attempt) {

		ProgressMonitor mon = new GitProgressMonitor();
		try {
			InstallCertNow.please("github.com", null, null);
		} catch (Exception e1) {
			logger.error(Constants.STACKTRACE, e1);
		}

		if(attempt < 3)
		{
			File file = new File(localRepo);
			RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/" + remoteRepo +"/master");
			List <RefSpec> refList = new ArrayList<>();
			refList.add(spec);
			CredentialsProvider cp = null;
			if(userName != null && password != null && !userName.isEmpty() && !password.isEmpty()) {
				cp = new UsernamePasswordCredentialsProvider(userName, password);
			}
			Git thisGit = null;
			try {
				thisGit = Git.open(file);
				if(cp != null) {
					thisGit.fetch().setCredentialsProvider(cp).setRemote(remoteRepo).setProgressMonitor(mon).call();
				} else {
					thisGit.fetch().setRemote(remoteRepo).setProgressMonitor(mon).call();
				}
				
			} catch(TransportException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				return fetchRemote(localRepo, remoteRepo, userName, password, attempt);
			} catch (IOException | GitAPIException e) {
				logger.error(Constants.STACKTRACE, e);
				mon.endTask();
				throw new IllegalArgumentException("Error with fetching the remote respository at " + remoteRepo);
			} finally {
				if(thisGit != null) {
					thisGit.close();
				}
			}
		}
		return mon;
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
	 * @param localRepositoryDir
	 * @return
	 */
	public static List<Map<String, String>> listConfigRemotes(String localRepositoryDir) {
		List<Map<String, String>> returnList = new Vector<Map<String, String>>();
		Git thisGit = null;
		Repository thisRepo = null;
		try {
			File file = new File(localRepositoryDir);
			thisGit = Git.open(file);
			thisRepo = thisGit.getRepository();
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
				logger.debug("We have remote with details " + remoteMap);
				returnList.add(remoteMap);
			}
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(thisRepo != null) {
				thisRepo.close();
			}
			if(thisGit != null) {
				thisGit.close();
			}
		}

		return returnList;
	}

	/**
	 * Get the list of remote configurations associated with an app directory
	 * Get the url
	 * Get the namespace/appName
	 * Get the type -> dual or subscript
	 * @param localRepositoryName
	 * @return
	 */
	public static String getConfigRemoteURL(String localRepositoryName, String remoteName) {
		List<Map<String, String>> returnList = new Vector<Map<String, String>>();
		Git thisGit = null;
		Repository thisRepo = null;
		try {
			File file = new File(Utility.normalizePath(localRepositoryName));
			thisGit = Git.open(file);
			thisRepo = thisGit.getRepository();
			String[] remNames = thisRepo.getRemoteNames().toArray(new String[]{});
			for(int remIndex = 0; remIndex < remNames.length; remIndex++) {
				String remName = remNames[remIndex] +"";
				if(remName.equalsIgnoreCase(remoteName))
				{
					String url = thisRepo.getConfig().getString("remote", remName, "url");
					String upstream = thisRepo.getConfig().getString(remName, "upstream",  "url");
					return url;
				}
			}
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(thisRepo != null) {
				thisRepo.close();
			}
			if(thisGit != null) {
				thisGit.close();
			}
		}

		return null;
	}

	public static List<String> listRemotesForUser(String username, String password) {
		int attempt = 1;
		return listRemotesForUser(username, password, attempt);
	}

		
	/**
	 * Get the list of repos for a given user
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<String> listRemotesForUser(String username, String password, int attempt) {
		if(attempt < 3)
		{
			
			List<String> remoteRepos = new Vector<String>();
			GitHubClient client = GitHubClient.createClient("https://github.com");
			client.setCredentials(username, password);
			RepositoryService service = new RepositoryService(client);
			try {
				List<org.eclipse.egit.github.core.Repository> repList = service.getRepositories();
				for(int repIndex = 0;repIndex < repList.size();repIndex++) {
					remoteRepos.add(repList.get(repIndex).getName());
				}
			}catch(HttpException ex)
			{
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				listRemotesForUser(username, password, attempt);
				
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
	
			return remoteRepos;
		}
		return null;
	}

	
	
	/*************** OAUTH Overloads Go Here ***********************/
	/***************************************************************/

	public static List<String> listRemotesForUser(String token) {
		int attempt = 1;
		return listRemotesForUser(token, attempt);
	}

	/**
	 * Get the list of repos for a given user
	 * @param username
	 * @param password
	 * @return
	 */
	public static List<String> listRemotesForUser(String token, int attempt) {
		if(attempt < 3) {
			List<String> remoteRepos = new Vector<>();
			GitHubClient client = GitHubClient.createClient("https://github.com");
			client.setOAuth2Token(token);
			RepositoryService service = new RepositoryService(client);
			try {
				List<org.eclipse.egit.github.core.Repository> repList = service.getRepositories();
				for(int repIndex = 0;repIndex < repList.size();repIndex++) {
					remoteRepos.add(repList.get(repIndex).getName());
				}
			} catch(HttpException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				listRemotesForUser(token, attempt);
				
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
	
			return remoteRepos;
		}
		return null;
	}

	public static void deleteRemoteRepository(String repositoryName, String token) {
		int attempt = 1;
		deleteRemoteRepository(repositoryName, token, attempt);
	}


	/**
	 * Delete a repository
	 * @param repositoryName
	 * @param username
	 * @param password
	 * @throws IOException
	 */
	public static void deleteRemoteRepository(String repositoryName, String token, int attempt) {
		if(attempt < 3) {
			String repoName = repositoryName.split("/")[1];
			if(checkRemoteRepositoryO(repoName, token)) {
				GitHub gh = GitUtils.login(token);
				GHRepository ghr = null;
				try {
					ghr = gh.getRepository(repositoryName);
					ghr.delete();
				} catch(HttpException ex) {
					logger.error(Constants.STACKTRACE, ex);
					try {
						InstallCertNow.please("github.com", null, null);
					} catch (Exception e) {
						logger.error(Constants.STACKTRACE, e);
					}
					attempt = attempt + 1;
					deleteRemoteRepository(repositoryName, token, attempt);
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unalbe to delete remote repository at " + repositoryName);
				}
			}
		}
	}
	


	public static void fetchRemote(String localRepo, String remoteRepo, String token) {
		int attempt = 1;
		fetchRemote(localRepo, remoteRepo, token, attempt);
	}

	/**
	 * Switch to a specific git remote
	 * @param localRepo
	 * @param remoteRepo
	 * @param userName
	 * @param password
	 */
	public static void fetchRemote(String localRepo, String remoteRepo, String token, int attempt) {
		
		if(attempt < 3) {
			File file = new File(localRepo);
			RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/" + remoteRepo +"/master");
			List <RefSpec> refList = new ArrayList<RefSpec>();
			refList.add(spec);
			CredentialsProvider cp = null;
			if(token != null) {
				cp = new UsernamePasswordCredentialsProvider(token, "");
			}
			Git thisGit = null;
			try {
				thisGit = Git.open(file);
				if(cp != null) {
					thisGit.fetch().setCredentialsProvider(cp).setRemote(remoteRepo).call();
				} else {
					thisGit.fetch().setRemote(remoteRepo).call();
				}
			} catch(SSLHandshakeException ex) {
				logger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				fetchRemote(localRepo, remoteRepo, token, attempt);
				
			} catch (IOException | GitAPIException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error with fetching the remote respository at " + remoteRepo);
			} finally {
				if(thisGit != null) {
					thisGit.close();
				}
			}
		}
	}

	// find a particular commit in the folder
	public static RevCommit findCommit(String gitFolder, String id) throws Exception
	{
		RevCommit comm = null;

		try(Git thisGit = Git.open(new File(gitFolder))){
		StringBuilder builder = new StringBuilder();
		LogCommand lg = thisGit
						.log()
						//.addPath(fileName)
						.all();
		
		Iterator <RevCommit> commits = lg.call().iterator();
		
		boolean first = true;
		
		
		while(commits.hasNext())
		{
			comm = commits.next();
			if((comm.getId() + "").contains(id))
			{
				break;
			}
			comm = null;
		}

		}
		return comm;
	}
	
	// install the certificate
	public static boolean addCertForDomain(String repoName) {
		try {
			URI uri = new URI(repoName);
			String domain = uri.getHost();
			domain = domain.startsWith("www.") ? domain.substring(4) : domain;
			
			InstallCertNow.please(domain, null, null);
			return true;
		} catch (URISyntaxException use) {
			logger.error(Constants.STACKTRACE, use);
			return false;
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
	}
	
	public static List listCommits(String gitFolder, String fileName)
	{
		// list of lists
		List builder = null;
		try (			Git thisGit = Git.open(new File(gitFolder))){
			builder = new ArrayList();
			List row = new ArrayList();
			row.add("date");
			row.add("user");
			row.add("message");
			row.add("id");
			builder.add(row);
			LogCommand lg = null;
			if(fileName != null)
				lg = thisGit
					.log()
					.addPath(fileName)
					.all();
			else
				lg = thisGit
				.log()
				.all();

			
			Iterator <RevCommit> commits = lg.call().iterator();
			
			boolean first = true;
			
			while(commits.hasNext())
			{
				RevCommit comm = commits.next();
				row = new ArrayList();

				row.add(comm.getCommitTime());
				row.add(comm.getAuthorIdent().getName());
				row.add(comm.getFullMessage());
				row.add(comm.toObjectId().toString().replace("commit ", "").substring(0,6));
				builder.add(row);
				//RevTree tree = comm.getTree();
				//tree.
				
				//if(first)
				// {
					//System.out.println(comm.getId());
					//thisGit.revert().include(comm).call();
					//first = false;
				// }
				//break;
			}
		} catch (NoHeadException nhe) {
			logger.error(Constants.STACKTRACE, nhe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		return builder;
	}
	
	/**
	 * Get commit message with metadata
	 * @param gitFolder
	 * @param fileName optional if getting specific commit messages for a file
	 * @return
	 */
	public static List<Map<String, Object>> getCommits(String gitFolder, String fileName) {
		// list of lists
		List<Map<String, Object>> commitList = new Vector<>();
		try(Git thisGit = Git.open(new File(gitFolder))) {
			
			LogCommand lg = null;
			if (fileName != null && !fileName.isEmpty())
				lg = thisGit.log().addPath(fileName).all();
			else
				lg = thisGit.log().all();
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
			logger.error(Constants.STACKTRACE, nhe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return commitList;
	}
	
	// gets a particular file
	// showing file content for a particular ID
	// this will be utilized where the user goes
	// ok what did the user abcd check in for this file without the needing to revert / reset
	// frankly we should have a way for the user to go back and forth
	public static String getFile(String commId, String fileName, String gitFolder)
	{
		String output = null;
		FileReader fis = null;
		BufferedReader br = null; 
		Git thisGit = null;
		ObjectReader objectReader = null;
		try {
			
			RevCommit comm = null;
			if(commId == null)
			{
				// there is a good possibility the user has not saved this !?
				File file = new File(gitFolder + "/" + fileName);
				
				if(!file.exists())
				{
					// this could be an old insight try with the /version/assets
					file = new File(gitFolder + "/version/assets/" + fileName);
				}
				
				if(file.exists())
				{
					fis = new FileReader(file);
					br = new BufferedReader(fis);
					StringBuffer buff = new StringBuffer();
					String temp = null;
					while((temp = br.readLine()) != null)
						buff.append(temp).append("\n");
					
					output = buff.toString();
					// not going to process the head for now
					//ObjectId commId2 = thisGit.getRepository().resolve(Constants.HEAD);
					//RevWalk walk = new RevWalk(thisGit.getRepository());
					//comm = walk.lookupCommit(commId2);
				}
			}
			else
			{
				thisGit = Git.open(new File(gitFolder));

				comm = findCommit(gitFolder, commId);
			
				TreeWalk treeWalk = TreeWalk.forPath( thisGit.getRepository(), fileName, comm.getTree());
				ObjectId blobId = treeWalk.getObjectId( 0 );
				
				objectReader = thisGit.getRepository().newObjectReader();
				ObjectLoader objectLoader = objectReader.open( blobId );
				byte[] bytes = objectLoader.getBytes();
				output = new String(bytes);
			}			
		} catch (MissingObjectException moe) {
			logger.error(Constants.STACKTRACE, moe);
		} catch (IncorrectObjectTypeException iote) {
			logger.error(Constants.STACKTRACE, iote);
		} catch (CorruptObjectException coe) {
			logger.error(Constants.STACKTRACE, coe);
		} catch (LargeObjectException loe) {
			logger.error(Constants.STACKTRACE, loe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(fis != null) {
		          try {
		        	  fis.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
			if(br != null) {
		          try {
		        	  br.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
			if(thisGit != null) {
		          thisGit.close();
		        }
			if(objectReader != null) {
				objectReader.close();
		        }
		}

		return output;
		
	}

	// gets a particular file
	// showing file content for a particular ID
	// this will be utilized where the user goes
	// ok what did the user abcd check in for this file without the needing to revert / reset
	// frankly we should have a way for the user to go back and forth
	public static byte[] getBinary(String commId, String fileName, String gitFolder)
	{
		byte[] bytes = null;
		Git thisGit = null;
		ObjectReader objectReader = null;
		try {
			
			RevCommit comm = null;
			if(commId == null)
			{
				// there is a good possibility the user has not saved this !?
				File file = new File(gitFolder + "/" + fileName);
				if(file.exists())
				{
					bytes = FileUtils.readFileToByteArray(file);
					// not going to process the head for now
					//ObjectId commId2 = thisGit.getRepository().resolve(Constants.HEAD);
					//RevWalk walk = new RevWalk(thisGit.getRepository());
					//comm = walk.lookupCommit(commId2);
				}
			}
			else
			{
				thisGit = Git.open(new File(gitFolder));

				comm = findCommit(gitFolder, commId);
			
				TreeWalk treeWalk = TreeWalk.forPath( thisGit.getRepository(), fileName, comm.getTree());
				ObjectId blobId = treeWalk.getObjectId( 0 );
				
				objectReader = thisGit.getRepository().newObjectReader();
				ObjectLoader objectLoader = objectReader.open( blobId );
				bytes = objectLoader.getBytes();
			}			
		} catch (MissingObjectException moe) {
			logger.error(Constants.STACKTRACE, moe);
		} catch (IncorrectObjectTypeException iote) {
			logger.error(Constants.STACKTRACE, iote);
		} catch (CorruptObjectException coe) {
			logger.error(Constants.STACKTRACE, coe);
		} catch (LargeObjectException loe) {
			logger.error(Constants.STACKTRACE, loe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(thisGit != null) {
		          thisGit.close();
		        }
			if(objectReader != null) {
				objectReader.close();
		        }
		}
		
		return bytes;
		
	}

	
	public static void addAllFiles(String gitFolder, boolean ignoreTheIgnoreFiles) {
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(gitFolder));
			status = thisGit.status().call();
		} catch (IOException | NoWorkTreeException | GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + gitFolder);
		}
		
		AddCommand ac = thisGit.add();
		boolean added = false;
		
		// get new files
		Iterator <String> upFiles = status.getUntracked().iterator();
		while(upFiles.hasNext()) {
			String daFile = upFiles.next();
			if(ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
				added = true;
				ac.addFilepattern(daFile);
			}
		}
		
		// get the modified files
		Iterator <String> modFiles = status.getModified().iterator();
		while(modFiles.hasNext()) {
			String daFile = modFiles.next();
			if(ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
				added = true;
				ac.addFilepattern(daFile);
			}
		}

		if(added) {
			try {
				ac.call();
			} catch (GitAPIException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to add files to Git directory at " + gitFolder);
			}
		}
		
		thisGit.close();
	}
	
	/**
	 * Add specific files to a given git
	 * @param thisGit
	 * @param files
	 */
	public static void addSpecificFiles(String localRepository, List<String> files) {
		if(files == null || files.isEmpty()) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(Utility.normalizePath(localRepository)));
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}
		AddCommand ac = thisGit.add();
		for(String daFile : files) {
			if(daFile.contains("version")) {
				daFile = daFile.substring(daFile.indexOf("version") + 8);
			}
			daFile = daFile.replace("\\", "/");
			ac.addFilepattern(daFile);
		}
		try {
			ac.call();
		} catch (GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		thisGit.close();
	}
	
	/**
	 * Add specific files to a given git
	 * @param thisGit
	 * @param files
	 */
	public static void addSpecificFiles(String localRepository, File[] files) {
		if(files == null || files.length == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		if (thisGit != null) {
			AddCommand ac = thisGit.add();
			for(File f : files) {
				String daFile = f.getAbsolutePath();
				if(daFile.contains("version")) {
					daFile = daFile.substring(daFile.indexOf("version") + 8);
				}
				daFile = daFile.replace("\\", "/");
				ac.addFilepattern(daFile);
			}
			try {
				ac.call();
			} catch (GitAPIException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			thisGit.close();
		}
	}
	
	public static void commitAddedFiles(String gitFolder) {
		commitAddedFiles(gitFolder, null);
	}
	
	public static void commitAddedFiles(String gitFolder, String message) {
		commitAddedFiles(gitFolder, message, null, null);
	}
	
	public static void commitAddedFiles(String gitFolder, String message, User user) {
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();
		commitAddedFiles(gitFolder, message, author, email);
	}

	public static void commitAddedFiles(String gitFolder, String message, String author, String email) {
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(gitFolder));
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + gitFolder);
		}

		CommitCommand cc = thisGit.commit();
		try {
			if(message == null)
				message = GitUtils.getDateMessage("Commited on.. ");
			if(author == null)
				author = "SEMOSS";
			if(email == null)
				email = "semoss@semoss.org";
			cc
			.setMessage(message)
			.setAuthor(author, email)
			.call();
		} catch (GitAPIException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		thisGit.close();
	}
	
	public static void revertCommit(String gitFolder, String comm1) {
		
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(gitFolder));
			RevCommit comm = findCommit(gitFolder, comm1);
			// revert sets it up where you go back as if nothing has happened
			//thisGit.reset().setRef(comm.getId().getName()).setMode(ResetType.HARD).call();
			
			// this is the revert
			// the revert works perfectly if you want to go one back
			// if you further than that.. it will create a change log on your file which you need to find and resolve
			//thisGit.revert().include(comm.getId()).setOurCommitName("new Commit").call();
			thisGit.revert().include(thisGit.getRepository().resolve(org.eclipse.jgit.lib.Constants.HEAD)).setOurCommitName("new Commit").call();
			//thisGit.commit().setMessage("Post Revert.. " ).call();
		} catch (RevisionSyntaxException rse) {
			logger.error(Constants.STACKTRACE, rse);
		} catch (NoMessageException nme) {
			logger.error(Constants.STACKTRACE, nme);
		} catch (UnmergedPathsException upe) {
			logger.error(Constants.STACKTRACE, upe);
		} catch (ConcurrentRefUpdateException cfue) {
			logger.error(Constants.STACKTRACE, cfue);
		} catch (WrongRepositoryStateException wrse) {
			logger.error(Constants.STACKTRACE, wrse);
		} catch (AmbiguousObjectException aoe) {
			logger.error(Constants.STACKTRACE, aoe);
		} catch (IncorrectObjectTypeException iote) {
			logger.error(Constants.STACKTRACE, iote);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (GitAPIException gae) {
			logger.error(Constants.STACKTRACE, gae);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
		}
	}
	
	public static void resetCommit(String gitFolder, String comm1) 
	{
		Git thisGit = null;
		try {
			// this is a different animal.. we just want to make sure the user knows what they are doing
			// if you dont do hard reset it doesn't give you shit
			thisGit = Git.open(new File(gitFolder));
			RevCommit comm = findCommit(gitFolder, comm1);
			// revert sets it up where you go back as if nothing has happened
			thisGit.reset().setRef(comm.getId().getName()).setMode(ResetType.HARD).call();
		} catch (CheckoutConflictException cce) {
			logger.error(Constants.STACKTRACE, cce);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (GitAPIException gae) {
			logger.error(Constants.STACKTRACE, gae);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
		}		
	}
	
	// saves aparticular version of the file
	// this has to be in some kind of temp directory
	public static String saveFileForDownload(String commId, String fileName, String gitFolder)
	{
		String output = null;
		ObjectReader objectReader = null;
		// this needs to change
		String cacheFolder = gitFolder;
		String cacheFileName = null;
		Git thisGit = null;
		FileOutputStream fos  = null;
		try {
			thisGit = Git.open(new File(gitFolder));
			//ObjectId masterTreeId= thisGit.getRepository().resolve("refs/heads/master^" + commitID);

			RevCommit comm = null;
			if(commId == null)
			{
				ObjectId commId2 = thisGit.getRepository().resolve(org.eclipse.jgit.lib.Constants.HEAD);
				RevWalk walk = new RevWalk(thisGit.getRepository());
				comm = walk.lookupCommit(commId2);
				commId = comm.getId().toString().substring(0,5);
			}
			else
				comm = findCommit(gitFolder, commId);

			// this file needs to be in the cache
			// infact we should even lookup to see if it is in the cache and if so  create it
			cacheFileName = cacheFolder + "/" + fileName + "_" + commId;
			
			// if this is available send it out
			File cacheFile = new File(cacheFileName);
			if(cacheFile.exists())
				return cacheFileName;
			
			TreeWalk treeWalk = TreeWalk.forPath( thisGit.getRepository(), fileName, comm.getTree());
			ObjectId blobId = treeWalk.getObjectId( 0 );
			
			objectReader = thisGit.getRepository().newObjectReader();
			ObjectLoader objectLoader = objectReader.open( blobId );
			
			byte[] bytes = objectLoader.getBytes();
			
			fos = new FileOutputStream(cacheFile);
			fos.write(bytes);
			
		} catch (MissingObjectException moe) {
			logger.error(Constants.STACKTRACE, moe);
		} catch (IncorrectObjectTypeException iote) {
			logger.error(Constants.STACKTRACE, iote);
		} catch (CorruptObjectException coe) {
			logger.error(Constants.STACKTRACE, coe);
		} catch (LargeObjectException loe) {
			logger.error(Constants.STACKTRACE, loe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
			if (objectReader != null) {
				objectReader.close();
			}
			if(fos != null) {
		          try {
		        	  fos.flush();
		        	  fos.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
		
		return cacheFileName;
	}
	
	// I dont know how it will work for non text files
	public static void createASCIIFile(String gitFolder, String fileName, String content, String message)
	{
		// makes the file
		// adds it
		// commits it
		try {
			if(message == null) {
				message = GitUtils.getDateMessage("Edited on");
			}
			File file = new File(gitFolder + "/" + fileName);
			PrintWriter pw = new PrintWriter(new FileWriter(fileName), true);
			pw.write(content);
			pw.close();
			addAllFiles(gitFolder, false);
			commitAddedFiles(gitFolder, message); 
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	public static void checkout(String gitFolder, String comm) throws Exception
	{
		try(Git git = Git.open(new File(gitFolder))){
		git.checkout().setName(comm).call();
		}
		
	}
	
	public static void resetCheckout(String gitFolder) throws Exception
	{
		try(Git git = Git.open(new File(gitFolder))){
		git.checkout().setName("master").call();
		}
	}
	
	public static void init(String folder)
	{
		try {
			addGitIgnore(folder);
			Git.init().setDirectory(new File(folder)).call();
			Git.open(new File(folder)).close();
			if (ClusterUtil.IS_CLUSTER) {
				ClusterUtil.validateFolder(folder);
			}
		} catch (IllegalStateException ise) {
			logger.error(Constants.STACKTRACE, ise);
		} catch (GitAPIException gae) {
			logger.error(Constants.STACKTRACE, gae);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	public static void addGitIgnore(String folder)
	{
		FileWriter fw = null;
		BufferedWriter bw  = null;
		try {
			File f = new File(folder, ".gitignore");
			f.createNewFile();
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);
			bw.write("*.cache");
			bw.newLine();
			bw.write("*/Temp/*");
			bw.newLine();
		} catch(Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		} finally {
			if(bw != null) {
				try {
					bw.close();
				} catch(IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(fw != null) {
				try {
					fw.close();
				} catch(IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
}
