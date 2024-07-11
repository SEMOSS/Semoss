package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import prerna.security.InstallCertNow;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// main class for all things fetch
// need a way to install the cert here too

public class GitFetchUtils {
	
	protected static final Logger classLogger = LogManager.getLogger(GitFetchUtils.class);

		
	public static void fetchGeneric(String fullRemoteAppName, String localFolder, String host)
	{
		Logger logger = LogManager.getLogger(GitFetchUtils.class);
		if(host == null)
			host = "github.com";
		// need to get the database folder
		try {
			// circumventing for the purposes of netskope
			prerna.security.InstallCertNow.please(host, null, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}

		
		File db = new File(localFolder);
		if(!db.exists()) {
			// make the folder
			db.mkdirs();
			//			throw new IllegalArgumentException("You already have a folder named " + yourName4App + " in your directory."
			//					+ " Please delete it or choose a different name for the app");
		}

		String[] appNameSplit = fullRemoteAppName.split("/");
		String appUserName = appNameSplit[0];
		String repoName = appNameSplit[1];

		// initialize the version folder
		logger.info("Start creating local git folder");
		//GitRepoUtils.makeLocalAppGitVersionFolder(yourName4App);
		logger.info("Done creating local git folder");

		String versionFolder = localFolder;
		// write a random file so we can add/commit
		logger.info("Init local git...");
		//GitUtils.semossInit(versionFolder);
		// add/commit all the files
		logger.info("Add local git...");
		GitPushUtils.addAllFiles(versionFolder, false);
		logger.info("Commit local git...");
		GitPushUtils.commitAddedFiles(versionFolder);
		logger.info("Done...");

		List<Map<String, String>> remotes = GitRepoUtils.listConfigRemotes(versionFolder);
		// need to loop through and see if remote exists
		boolean existing = false;
		REMOTE_LOOP : for(Map<String, String> remoteMap : remotes) {
			String existingRemoteAppName = remoteMap.get("name");
			// need to compare combination of name space + app name
			if(existingRemoteAppName.equals(repoName)) {
				existing = true;
				break REMOTE_LOOP ;
			}
		}
		if(!existing) {
			GitUtils.removeAllIgnore(versionFolder);
			GitRepoUtils.addRemote(versionFolder, appUserName, repoName);
		}

		// switch to correct remote
		logger.info("Fetch remote git directory");
		ProgressMonitor mon = GitRepoUtils.fetchRemote(versionFolder, repoName, "", "");
		// merge the remote - this will bring in the files from the remote repo
		logger.info("Merge remote git directory");
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);
		// commit
		GitPushUtils.commitAddedFiles(versionFolder);

		// move the smss to the db folder
		logger.info("Initialize new app...");
		
		// so, we used a random id to create the folder
		// but we need to rename to use the correct app id for the engine
		
		// before you do this.. wait for the monitor to finish
		// so this started succeeding even without so leaving it for now
		/*while(!((GitProgressMonitor)mon).complete)
		{
			try{
				Thread.sleep(200);
			}catch(Exception ignored)
			{
				
			}
		}*/
		//moveDataFilesToApp(baseFolder, actualAppId, yourName4App, logger);
		//return actualAppId;

	}
	
	public static ProgressMonitor fetchApp(String localRepo, String remoteRepo, String userName, String password) {
		int attempt = 1;
		return fetchApp(localRepo, remoteRepo, userName, password, attempt);
	}

	/**
	 * Switch to a specific git remote
	 * @param localRepo
	 * @param remoteRepo
	 * @param userName
	 * @param password
	 */
	public static ProgressMonitor fetchApp(String localRepo, String remoteRepo, String userName, String password, int attempt) {

		ProgressMonitor mon = new GitProgressMonitor();
		try {
			InstallCertNow.please("github.com", null, null);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e1);
		}

		if(attempt < 3)
		{
			File file = new File(localRepo);
			RefSpec spec = new RefSpec("refs/heads/master:refs/remotes/" + remoteRepo +"/master");
			List <RefSpec> refList = new ArrayList<RefSpec>();
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
				
			}catch(TransportException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				return fetchApp(localRepo, remoteRepo, userName, password, attempt);
				
			} catch (IOException | GitAPIException e) {
				classLogger.error(Constants.STACKTRACE, e);
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

	public static void fetchApp(String localRepo, String remoteRepo, String token) {
		int attempt = 1;
		fetchApp(localRepo, remoteRepo, token, attempt);
	}

	/**
	 * Switch to a specific git remote
	 * @param localRepo
	 * @param remoteRepo
	 * @param userName
	 * @param password
	 */
	public static void fetchApp(String localRepo, String remoteRepo, String token, int attempt) {
		
		if(attempt < 3)
		{
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
			}catch(SSLHandshakeException ex)
			{
				classLogger.error(Constants.STACKTRACE, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				attempt = attempt + 1;
				fetchApp(localRepo, remoteRepo, token, attempt);
				
			} catch (IOException | GitAPIException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error with fetching the remote respository at " + remoteRepo);
			} finally {
				if(thisGit != null) {
					thisGit.close();
				}
			}
		}
	}	
	
	// wipes it and puts a new clone
	public static void cloneApp(String remoteRepo, String localFolder)
	{
		GitRepoUtils.addCertForDomain(remoteRepo); 
		// tries to find if the local folder is available
		// deletes it and then clones it back
		try {
			File file = new File(localFolder);
			if(file.exists() && file.isDirectory())
				FileUtils.deleteDirectory(file);

			CloneCommand clone = Git.cloneRepository().setURI(remoteRepo).setDirectory(file);
			Git gclone = clone.call();
			gclone.close();
		} catch (InvalidRemoteException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (org.eclipse.jgit.api.errors.TransportException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}

		
	}

}
