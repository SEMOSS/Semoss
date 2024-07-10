package prerna.util.git;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.kohsuke.github.GitHub;

import prerna.util.AssetUtility;
import prerna.util.Constants;

public class GitCreator {
	
	private static final Logger classLogger = LogManager.getLogger(GitCreator.class);

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitCreator() {

	}

	/**
	 * Push an  to Git
	 * @param databaseId
	 * @param databaseName
	 * @param remoteDatabaseName
	 * @param username
	 * @param password
	 */
	public static void makeRemoteFromDatabase(String databaseId, String databaseName, String remoteDatabaseName, String username, String password, boolean syncDatabase, String token) {
		// first, need to login
		GitHub git = null;
		if (!token.isEmpty()) {
			git = GitUtils.login(token);
		} else {
			git = GitUtils.login(username, password);
		}

		// need to get the database folder
		String dbFolder = AssetUtility.getProjectBaseFolder(databaseName, databaseId);;

		// the remote location
		// is of the form account_name/repo_name
		// so we want to split this out
		String repoName = "";
		if(remoteDatabaseName.contains("/")) {
			String[] remoteLocationSplit = remoteDatabaseName.split("/");
			String accountName = remoteLocationSplit[0];
			repoName = remoteLocationSplit[1];
		} else {
			repoName = remoteDatabaseName;
		}
		
		// now, we need to check and see if this folder is also a git
		boolean isGit = GitUtils.isGit(dbFolder);
		if(!isGit) {
			GitRepoUtils.makeLocalDatabaseGitVersionFolder(dbFolder);
		}

		String versionFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);;
		File versionDir = new File(versionFolder);
		if(!versionDir.exists()) {
			versionDir.mkdirs();
		}
		// write a random file so we can add/commit
		GitUtils.semossInit(versionFolder);
		
		// if we are going to sync the databases
		// we will add them in a separate add statement
		// or, if we have any, we delete them
		if(syncDatabase) {
			// these may have been removed from git before
			// so we need to remove the ignore file to properly add
			GitUtils.removeAllIgnore(versionFolder);
			pushFilesToVersionFolder(dbFolder, versionFolder);
			GitPushUtils.addSpecificFiles(versionFolder, getDatabaseFiles(versionFolder, true));
		} else {
			// if the files are present, we want to remove them so they dont get pushed
			// to this new repository
			GitDestroyer.removeSpecificFiles(versionFolder, false, getDatabaseFiles(versionFolder, false));
		}
		GitPushUtils.addAllFiles(versionFolder, false);
		GitPushUtils.commitAddedFiles(versionFolder);

		// now that our local is all good
		// we need to push this to the remote instance
		// make the remote on git
		GitRepoUtils.makeRemoteRepository(git, username, repoName);

		// now set the remote to the local
		GitRepoUtils.addRemote(versionFolder, username, repoName);

		if (!token.isEmpty()) {
			// now fetch it
			// this will shift us from our local to the master
			GitRepoUtils.fetchRemote(versionFolder, repoName, token);
			// now push our local to the remote repo
			GitPushUtils.push(versionFolder, repoName, "master", token);
		} else {
			// now fetch it
			// this will shift us from our local to the master
			GitRepoUtils.fetchRemote(versionFolder, repoName, username, password);

			// now push our local to the remote repo
			GitPushUtils.push(versionFolder, repoName, "master", username, password);
		}

	}
	
	private static void pushFilesToVersionFolder(String appFolder, String gitFolder) {
		File versionDir = new File(gitFolder);
		File appDir = new File(appFolder);

		File[] filesToMove = getDatabaseFiles(appFolder, false);
		int numFiles = filesToMove.length;
		for(int i = 0; i < numFiles; i++) {
			try {
				FileUtils.copyFileToDirectory(filesToMove[i], versionDir);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		// push the data folder if it exists
		pushDataFolder(appFolder, gitFolder);

		// we also need to move the smss file
		File smssFile = getSmssFile(appDir);
		try {
			FileUtils.copyFileToDirectory(smssFile, versionDir);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @param databaseFolder
	 * @param gitFolder
	 */
	private static void pushDataFolder(String databaseFolder, String gitFolder) {
		String dataFile = databaseFolder + "/data";
		File dataDir = new File(dataFile);
		if(dataDir.exists()) {
			String gitDataFolder = gitFolder + "/data";
			File gitDataDir = new File(gitDataFolder);
			gitDataDir.mkdir();
			
			List<String> grabItems = new Vector<String>();
			grabItems.add("*.csv");
			grabItems.add("*.tsv");
			FileFilter fileFilter = fileFilter = new WildcardFileFilter(grabItems);
			File[] filesToMove = dataDir.listFiles(fileFilter);
			
			int numFiles = filesToMove.length;
			for(int i = 0; i < numFiles; i++) {
				try {
					FileUtils.copyFileToDirectory(filesToMove[i], gitDataDir);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	private static File[] getDatabaseFiles(String folder, boolean includeSmss) {
		File appDir = new File(folder);
		// we need to push the db/owl/jnl into this folder
		List<String> grabItems = new Vector<String>();
		grabItems.add("*.db");
		grabItems.add("*.jnl");
		grabItems.add("*.OWL");
		grabItems.add("*.csv");
		grabItems.add("*.tsv");
		if(includeSmss) {
			grabItems.add("*.smss");
		}
		FileFilter fileFilter = new WildcardFileFilter(grabItems);
		File[] filesToMove = appDir.listFiles(fileFilter);
		return filesToMove;
	}
	
	private static File getSmssFile(File databaseDir) {
		String smssLocation = databaseDir.getParent() + "/" + databaseDir.getName() + ".smss";
		File smssFile = new File(smssLocation);
		return smssFile;
	}
}
