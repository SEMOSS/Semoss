package prerna.util.git;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;

import prerna.util.AssetUtility;
import prerna.util.Utility;

public class GitSynchronizer {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitSynchronizer() {

	}
	
	public static void syncDatabases(String localDatabaseId, String localDatabaseName, String remoteDatabaseName, String username, String password, Logger logger) {
		String baseFolder = Utility.getBaseFolder();
		String appFolder = AssetUtility.getProjectBaseFolder(localDatabaseName, localDatabaseId); //baseFolder + "/db/" + SmssUtilities.getUniqueName(localAppName, localAppId);
		
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
		
		// we need to move the database files from the current db
		// into the version folder
		pushFilesToVersionFolder(appFolder);
		
		String versionFolder = AssetUtility.getProjectVersionFolder(localDatabaseName, localDatabaseId);
		// we want to get rid of the ignore 
		GitUtils.removeAllIgnore(versionFolder);
		// now we push everything locally
		GitPushUtils.addAllFiles(versionFolder, true);
		GitDestroyer.removeFiles(versionFolder, true, true);
		GitPushUtils.commitAddedFiles(versionFolder);
		GitPushUtils.push(versionFolder, repoName, "master", username, password);

		// add back the ignore
		String [] filesToIgnore = new String[] {"*.mv.db", "*.db", "*.jnl"};
		GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
		GitUtils.checkoutIgnore(versionFolder, filesToIgnore);
		
		// we now need to move over these new files
		GitConsumer.moveDataFilesToDatabase(baseFolder, localDatabaseId, localDatabaseName, logger);
	}
	
	private static void pushFilesToVersionFolder(String appFolder) {
		File appDir = new File(appFolder);
		String versionFolder = appFolder + "/version";
		File versionDir = new File(versionFolder);

		// we need to push the db/owl/jnl into this folder
		List<String> grabItems = new Vector<String>();
		grabItems.add("*.db");
		grabItems.add("*.jnl");
		grabItems.add("*.OWL");
		FileFilter fileFilter = fileFilter = new WildcardFileFilter(grabItems);
		
		File[] filesToMove = appDir.listFiles(fileFilter);
		File[] currentVersionFiles = versionDir.listFiles(fileFilter);
		
		int numFiles = filesToMove.length;
		for(int i = 0; i < numFiles; i++) {
			try {
				// if the current version folder
				// has a file with the same name
				// as one we are about to push
				// delete it
				CURFILE_LOOP : for(File curFile : currentVersionFiles) {
					if(filesToMove[i].getName().equals(curFile.getName())) {
						// we found a file that matches
						// delete it so we can copy the new one to replace
						curFile.delete();
						break CURFILE_LOOP ;
					}
				}
				FileUtils.copyFileToDirectory(filesToMove[i], versionDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		pushDataFolder(appFolder, versionFolder);
		
		// we also need to move the smss file
		String smssLocation = appDir.getParent() + "/" + appDir.getName() + ".smss";
		File smssFile = new File(smssLocation);
		try {
			FileUtils.copyFileToDirectory(smssFile, versionDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void pushDataFolder(String appFolder, String gitFolder) {
		String dataFile = appFolder + "/data";
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
			
			File[] curDataFiles = gitDataDir.listFiles(fileFilter);
			
			int numFiles = filesToMove.length;
			for(int i = 0; i < numFiles; i++) {
				try {
					// if the current version folder
					// has a file with the same name
					// as one we are about to push
					// delete it
					CURFILE_LOOP : for(File curFile : curDataFiles) {
						if(filesToMove[i].getName().equals(curFile.getName())) {
							// we found a file that matches
							// delete it so we can copy the new one to replace
							curFile.delete();
							break CURFILE_LOOP ;
						}
					}
					FileUtils.copyFileToDirectory(filesToMove[i], gitDataDir);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Synchronize files to a specific Git repo
	 * @param localAppName
	 * @param remoteAppName
	 * @param username
	 * @param password
	 * @param filesToAdd
	 * @param dual
	 * @return 
	 */
	public static Map<String, List<String>> synchronizeSpecific(String appId, String localAppName, String remoteAppName, String username, String password, List<String> filesToSync, boolean dual) {
		String baseFolder = Utility.getBaseFolder();
		String versionFolder = AssetUtility.getProjectVersionFolder(localAppName, appId);
		
		String repoName = "";
		if(remoteAppName.contains("/")) {
			String[] remoteLocationSplit = remoteAppName.split("/");
			String accountName = remoteLocationSplit[0];
			repoName = remoteLocationSplit[1];
		} else {
			repoName = remoteAppName;
		}
		
		GitRepoUtils.fetchRemote(versionFolder, repoName, username, password);
		List<String>[] filesSplit = determineFileOperation(filesToSync);
		GitPushUtils.addSpecificFiles(versionFolder, filesSplit[0]);
		GitDestroyer.removeSpecificFiles(versionFolder, true, filesSplit[1]);
		GitPushUtils.commitAddedFiles(versionFolder);
		
		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + repoName +"/master";
		
		Hashtable<String, List<String>> returnFiles = getFilesToAdd(versionFolder, thisMaster, remoteMaster);
		
		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);
		List <String> conflicted = getConflictedFiles(versionFolder);
		
		if(conflicted.size() > 0) {
			// we cannot proceed with merging.. until the conflicts are resolved
			// so abort!
			GitMergeHelper.abortMerge(versionFolder);
		}
		// only push back if there are no conflicts
		// and the user wants to push as well as pull
		else if(dual) {
			GitPushUtils.push(versionFolder, repoName, "master",username, password);
		}
		return returnFiles;
	}
	
	/**
	 * If the file exists, then we want to add the file
	 * If the file doesn't exist, then we want to remove the file
	 * @param filesToSync
	 * @return
	 */
	private static List<String>[] determineFileOperation(List<String> filesToSync) {
		List<String>[] fileOps = new Vector[2];
		List<String> addFiles = new Vector<String>();
		List<String> delFiles = new Vector<String>();
		
		for(String filePath : filesToSync) {
			File f = new File(Utility.normalizePath(filePath));
			if(f.exists()) {
				addFiles.add(filePath);
			} else {
				delFiles.add(filePath);
			}
		}
		
		fileOps[0] = addFiles;
		fileOps[1] = delFiles;
		return fileOps;
	}
	
	
	
	public static Map<String, List<String>> synchronize(String appId, String localAppName, String remoteAppName, String username, String password, boolean dual) {
		String baseFolder = Utility.getBaseFolder();
		String versionFolder = AssetUtility.getProjectVersionFolder(localAppName, appId);

		String [] filesToIgnore = new String[] {"*.mv.db", "*.db", "*.jnl"};
		GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
		// get everything
		String repoName = "";
		if(remoteAppName.contains("/")) {
			String[] remoteLocationSplit = remoteAppName.split("/");
			String accountName = remoteLocationSplit[0];
			repoName = remoteLocationSplit[1];
		} else {
			repoName = remoteAppName;
		}

		GitUtils.checkoutIgnore(versionFolder, filesToIgnore);
		// add the files we need to
		GitPushUtils.addAllFiles(versionFolder, false);
		// drop the files that are missing / deleted
		GitDestroyer.removeFiles(versionFolder, false, true);
		// commit
		GitPushUtils.commitAddedFiles(versionFolder);

		GitRepoUtils.fetchRemote(versionFolder, repoName, username, password);

		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + repoName +"/master";
		Hashtable <String, List<String>> files = getFilesToAdd(versionFolder, thisMaster, remoteMaster);

		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);

		List<String> conflicted = getConflictedFiles(versionFolder);
		// TODO: need to return back conflicted files
		// TODO: need to have conversation with front end on it
		if(conflicted.size() > 0) {
			// we cannot proceed with merging.. until the conflicts are resolved
			// so abort!
			GitMergeHelper.abortMerge(versionFolder);
		}
		// only push back if there are no conflicts
		// and the user wants to push as well as pull
		else if(dual) {
			GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
			GitPushUtils.push(versionFolder, repoName, "master",username, password);
		}

		return files;
	}
	
	public static Map<String, List<String>> synchronize(String appId, String localAppName, String remoteAppName, String token, boolean dual) {
		String baseFolder = Utility.getBaseFolder();
		String versionFolder = AssetUtility.getProjectVersionFolder(localAppName, appId);

		String [] filesToIgnore = new String[] {"*.mv.db", "*.db", "*.jnl"};
		GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
		// get everything
		String repoName = "";
		if(remoteAppName.contains("/")) {
			String[] remoteLocationSplit = remoteAppName.split("/");
			String accountName = remoteLocationSplit[0];
			repoName = remoteLocationSplit[1];
		} else {
			repoName = remoteAppName;
		}

		GitUtils.checkoutIgnore(versionFolder, filesToIgnore);
		// add the files we need to
		if(dual) {
			GitPushUtils.addAllFiles(versionFolder, false);
			// drop the files that are missing / deleted
			GitDestroyer.removeFiles(versionFolder, false, true);
			// commit
			GitPushUtils.commitAddedFiles(versionFolder);
		}
		
		GitRepoUtils.fetchRemote(versionFolder, repoName, token);

		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + repoName +"/master";
		Hashtable <String, List<String>> files = getFilesToAdd(versionFolder, thisMaster, remoteMaster);

		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);

		List<String> conflicted = getConflictedFiles(versionFolder);
		// TODO: need to return back conflicted files
		// TODO: need to have conversation with front end on it
		if(conflicted.size() > 0) {
			// we cannot proceed with merging.. until the conflicts are resolved
			// so abort!
			GitMergeHelper.abortMerge(versionFolder);
		}
		// only push back if there are no conflicts
		// and the user wants to push as well as pull
		else if(dual) {
			GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
			GitPushUtils.push(versionFolder, repoName, "master", token);
		}

		return files;
	}

	/**
	 * Get the conflicted files
	 * @param dbName
	 * @return
	 */
	private static List<String> getConflictedFiles(String dbName) {
		Vector <String> output = new Vector<String>();
		try(Git thisGit = Git.open(new File(dbName))) {
			Status status = thisGit.status().call();
			Iterator <String> cFiles = status.getConflicting().iterator();
			while(cFiles.hasNext()) {
				output.add(cFiles.next());
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		}		
		return output;
	}

	/**
	 * 
	 * @param dir
	 * @param baseRepo			This is usually the local repo
	 * @param newRepo			This is usually the remote repo
	 * @return
	 */
	private static Hashtable <String, List<String>> getFilesToAdd(String dir, String baseRepo, String newRepo) {
		// this assumes that you have run a fetch
		Hashtable <String, List<String>> finalHash = new Hashtable();
		// I need to update the remote first
		// https://stackoverflow.com/questions/3258243/check-if-pull-needed-in-git
		// git remote update 
		// git status uno
		File dirFile = new File(dir);

		try(Git thisGit = Git.open(dirFile)) {

			AbstractTreeIterator oldTreeParser = prepareTreeParser(thisGit.getRepository(), baseRepo);
			AbstractTreeIterator newTreeParser = prepareTreeParser(thisGit.getRepository(), newRepo);		
			// then the procelain diff-command returns a list of diff entries
			List<DiffEntry> diff = thisGit.diff().setOldTree(oldTreeParser).setNewTree(newTreeParser).call();
			List <String> addFiles = new Vector<String>();
			List <String> modFiles = new Vector<String>();
			List <String> renFiles = new Vector<String>();
			List <String> delFiles = new Vector<String>();
			for (DiffEntry entry : diff) {
				// if the new path is /dev/null
				// it means we are adding a new file to the newRepo that currently doesn't exist
				// so it thinks it is a file delete but that is not actually the case
				String newEntryPath = entry.getNewPath();
				// if the old path is /dev/null
				// it means we are adding a new file to the baseRepo that currnetly doesn't exist
				String oldEntryPath = entry.getOldPath();
				
				String filePath = dir + "/" + newEntryPath; 
				System.out.println("Entry: " + filePath);
				
				ChangeType cType = entry.getChangeType();
				if(cType == ChangeType.ADD) {
					addFiles.add(filePath);
				} else if(cType == ChangeType.MODIFY) {
					modFiles.add(filePath);
				} else if(cType == ChangeType.RENAME) {
					renFiles.add(filePath);
				} else if(cType == ChangeType.DELETE) {
					if(newEntryPath.equals("/dev/null")) {
						// if it is /dev/null, it actually means we added a file
						// to the repo that wasn't previously there
						// so we will treat it like an add instead of a delete
						addFiles.add(dir + "/" + oldEntryPath);
					} else {
						delFiles.add(filePath);
					}
				}
			}
			if(addFiles.size() > 0) {
				finalHash.put("ADD", addFiles);
			}
			if(modFiles.size() > 0) {
				finalHash.put("MOD", modFiles);
			}
			if(renFiles.size() > 0) {
				finalHash.put("REN", renFiles);
			}
			if(delFiles.size() > 0) {
				finalHash.put("DEL", delFiles);
			}
		} catch (IOException | GitAPIException e) {
			e.printStackTrace();
		}

		return finalHash;
	}
	
	private static AbstractTreeIterator prepareTreeParser(Repository repository, String ref) throws IOException {
		// from the commit we can build the tree which allows us to construct the TreeParser
		Ref head = repository.exactRef(ref);
		if(head != null)
		{
			try (RevWalk walk = new RevWalk(repository)) {
				RevCommit commit = walk.parseCommit(head.getObjectId());
				RevTree tree = walk.parseTree(commit.getTree().getId());

				CanonicalTreeParser treeParser = new CanonicalTreeParser();
				try (ObjectReader reader = repository.newObjectReader()) {
					treeParser.reset(reader, tree.getId());
				}

				walk.dispose();

				return treeParser;
			}
		}
		return null;
	}
	
	
	/*************** OAUTH Overloads Go Here ***********************/
	/***************************************************************/

	
	
	public static void syncDatabases(String appId, String localAppName, String remoteAppName, String token, Logger logger) {
		String baseFolder = Utility.getBaseFolder();
		String appFolder = AssetUtility.getProjectBaseFolder(localAppName, appId);;
		
		// the remote location
		// is of the form account_name/repo_name
		// so we want to split this out
		String repoName = "";
		if(remoteAppName.contains("/")) {
			String[] remoteLocationSplit = remoteAppName.split("/");
			String accountName = remoteLocationSplit[0];
			repoName = remoteLocationSplit[1];
		} else {
			repoName = remoteAppName;
		}
		
		// we need to move the database files from the current db
		// into the version folder
		pushFilesToVersionFolder(appFolder);
		
		String versionFolder = AssetUtility.getProjectVersionFolder(localAppName, appId);;
		// we want to get rid of the ignore 
		GitUtils.removeAllIgnore(versionFolder);
		// now we push everything locally
		GitPushUtils.addAllFiles(versionFolder, true);
		GitDestroyer.removeFiles(versionFolder, true, true);
		GitPushUtils.commitAddedFiles(versionFolder);
		GitPushUtils.push(versionFolder, repoName, "master", token);

		// add back the ignore
		String [] filesToIgnore = new String[] {"*.mv.db", "*.db", "*.jnl"};
		GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
		GitUtils.checkoutIgnore(versionFolder, filesToIgnore);
		
		// we now need to move over these new files
		GitConsumer.moveDataFilesToDatabase(baseFolder, appId, localAppName, logger);
	}

	/**
	 * Synchronize files to a specific Git repo
	 * @param localAppName
	 * @param remoteAppName
	 * @param username
	 * @param password
	 * @param filesToAdd
	 * @param dual
	 * @return 
	 */
	public static Map<String, List<String>> synchronizeSpecific(String appId, String localAppName, String remoteAppName, String token, List<String> filesToSync, boolean dual) {
		String baseFolder = Utility.getBaseFolder();
		String versionFolder = AssetUtility.getProjectVersionFolder(localAppName, appId);;
		
		String repoName = "";
		if(remoteAppName.contains("/")) {
			String[] remoteLocationSplit = remoteAppName.split("/");
			String accountName = remoteLocationSplit[0];
			repoName = remoteLocationSplit[1];
		} else {
			repoName = remoteAppName;
		}
		
		GitRepoUtils.fetchRemote(versionFolder, repoName, token);
		List<String>[] filesSplit = determineFileOperation(filesToSync);
		GitPushUtils.addSpecificFiles(versionFolder, filesSplit[0]);
		GitDestroyer.removeSpecificFiles(versionFolder, true, filesSplit[1]);
		GitPushUtils.commitAddedFiles(versionFolder);
		
		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + repoName +"/master";
		
		Hashtable<String, List<String>> returnFiles = getFilesToAdd(versionFolder, thisMaster, remoteMaster);
		
		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		GitMergeHelper.merge(versionFolder, "master", repoName + "/master", 0, 2, true);
		List <String> conflicted = getConflictedFiles(versionFolder);
		
		if(conflicted.size() > 0) {
			// we cannot proceed with merging.. until the conflicts are resolved
			// so abort!
			GitMergeHelper.abortMerge(versionFolder);
		}
		// only push back if there are no conflicts
		// and the user wants to push as well as pull
		else if(dual) {
			GitPushUtils.push(versionFolder, repoName, "master",token);
		}
		return returnFiles;
	}
	
	
	
}
