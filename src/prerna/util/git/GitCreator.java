package prerna.util.git;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.codehaus.plexus.util.FileUtils;
import org.kohsuke.github.GitHub;

import prerna.util.DIHelper;

public class GitCreator {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitCreator() {

	}

	/**
	 * Push an app to Git
	 * @param appName
	 * @param remoteLocation
	 * @param username
	 * @param password
	 */
	public static void makeRemoteFromApp(String appName, String remoteLocation, String username, String password) {
		// first, need to login
		GitHub git = GitUtils.login(username, password);

		// need to get the database folder
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbFolder = baseFolder + "/db/" + appName;

		// the remote location
		// is of the form account_name/repo_name
		// so we want to split this out
		String[] remoteLocationSplit = remoteLocation.split("/");
		String accountName = remoteLocationSplit[0];
		String repoName = remoteLocationSplit[1];

		// now, we need to check and see if this folder is also a git
		boolean isGit = GitUtils.isGit(dbFolder);
		if(!isGit) {
			GitRepoUtils.makeLocalAppGitVersionFolder(dbFolder);
		}

		// the folder we use the /version folder
		// we want to push the database + metadata files into the /version
		// this way, when another SEMOSS user pulls the information
		// they will have the data to use
		pushFilesToVersionFolder(dbFolder);

		// now we push everything locally
		String versionFolder = dbFolder + "/version";
		GitPushUtils.addAllFiles(versionFolder, true);
		GitPushUtils.commitAddedFiles(versionFolder);

		// now that our local is all good
		// we need to push this to the remote instance
		// first, remove all the ignores if present
		GitUtils.removeAllIgnore(versionFolder);
		// make the remote on git
		GitRepoUtils.makeRemoteRepository(git, username, repoName);

		// now set the remote to the local
		GitRepoUtils.addRemote(versionFolder, username, repoName);
		// now fetch it
		// this will shift us from our local to the master
		GitRepoUtils.fetchRemote(versionFolder, repoName, username, password);
		// merge everything
		// TODO: why do we need to do this???
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);
		// now push our local to the remote repo
		GitPushUtils.push(versionFolder, repoName, "master", username, password);
	}

	private static void pushFilesToVersionFolder(String appFolder) {
		// make a version folder if it does not already exist
		File versionDir = new File(appFolder + "/version");
		if(!versionDir.exists()) {
			versionDir.mkdirs();
		}
		// we need to push the db/owl/jnl into this folder
		List<String> grabItems = new Vector<String>();
		grabItems.add("*.db");
		grabItems.add("*.jnl");
		grabItems.add("*.OWL");
		FileFilter fileFilter = fileFilter = new WildcardFileFilter(grabItems);
		File appDir = new File(appFolder);
		File[] filesToMove = appDir.listFiles(fileFilter);
		int numFiles = filesToMove.length;
		for(int i = 0; i < numFiles; i++) {
			try {
				FileUtils.copyFileToDirectory(filesToMove[i], versionDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// we also need to move the smss file
		String smssLocation = appDir.getParent() + "/" + appDir.getName() + ".smss";
		File smssFile = new File(smssLocation);
		try {
			FileUtils.copyFileToDirectory(smssFile, versionDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
