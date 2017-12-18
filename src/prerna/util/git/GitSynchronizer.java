package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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

import prerna.util.DIHelper;

public class GitSynchronizer {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitSynchronizer() {

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
	public static Map<String, List<String>> synchronizeSpecific(String localAppName, String remoteAppName, String username, String password, List<String> filesToAdd, boolean dual) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String versionFolder = baseFolder + "/db/" + localAppName + "/version";
		
		remoteAppName = remoteAppName.split("/")[1]; 
		GitRepoUtils.fetchRemote(versionFolder, remoteAppName, username, password);
		GitPushUtils.addSpecificFiles(versionFolder, filesToAdd);
		GitPushUtils.commitAddedFiles(versionFolder);
		
		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + remoteAppName +"/master";
		
		Hashtable<String, List<String>> returnFiles = getFilesToAdd(versionFolder, thisMaster, remoteMaster);
		
		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		GitMergeHelper.merge(versionFolder, "master", remoteAppName + "/master", 0, 2, true);
		List <String> conflicted = getConflictedFiles(versionFolder);
		
		if(conflicted.size() > 0) {
			// we cannot proceed with merging.. until the conflicts are resolved
			// so abort!
			GitMergeHelper.abortMerge(versionFolder);
		}
		// only push back if there are no conflicts
		// and the user wants to push as well as pull
		else if(dual) {
			GitPushUtils.push(versionFolder, remoteAppName, "master",username, password);
		}
		return returnFiles;
	}
	
	public static Map<String, List<String>> synchronize(String localAppName, String remoteAppName, String username, String password, boolean dual)
	{
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String versionFolder = baseFolder+ "/db/" + localAppName + "/version";

		String [] filesToIgnore = new String[] {"*.mv.db", "*.db", "*.jnl"};
		GitUtils.writeIgnoreFile(versionFolder, filesToIgnore);
		// get everything
		String remoteUserName = remoteAppName.split("/")[0];
		remoteAppName = remoteAppName.split("/")[1]; 

		GitUtils.checkoutIgnore(versionFolder, filesToIgnore);
		GitPushUtils.addAllFiles(versionFolder, false);
		GitPushUtils.commitAddedFiles(versionFolder);

		GitRepoUtils.fetchRemote(versionFolder, remoteAppName, username, password);

		// need to get a list of files to process
		String thisMaster = "refs/heads/master";
		String remoteMaster = "refs/remotes/" + remoteAppName +"/master";
		Hashtable <String, List<String>> files = getFilesToAdd(versionFolder, thisMaster, remoteMaster);

		// check to see if there are conflicts
		// it is now done as part of merge
		// merge everything
		GitMergeHelper.merge(versionFolder, "master", remoteAppName + "/master", 0, 2, true);

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
			GitPushUtils.push(versionFolder, remoteAppName, "master",username, password);
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
		try {
			Git thisGit = Git.open(new File(dbName));
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

	private static Hashtable <String, List<String>> getFilesToAdd(String dir, String baseRepo, String newRepo)
	{
		// this assumes that you have run a fetch
		Hashtable <String, List<String>> finalHash = new Hashtable();
		// I need to update the remote first
		// https://stackoverflow.com/questions/3258243/check-if-pull-needed-in-git
		// git remote update 
		// git status uno
		
		try {
			File dirFile = new File(dir);
			Git thisGit = Git.open(dirFile);

			AbstractTreeIterator oldTreeParser = prepareTreeParser(thisGit.getRepository(), baseRepo);
			AbstractTreeIterator newTreeParser = prepareTreeParser(thisGit.getRepository(), newRepo);		
			// then the procelain diff-command returns a list of diff entries
			List<DiffEntry> diff = thisGit.diff().setOldTree(oldTreeParser).setNewTree(newTreeParser).call();
			List <String> addFiles = new Vector<String>();
			List <String> modFiles = new Vector<String>();
			List <String> renFiles = new Vector<String>();
			List <String> delFiles = new Vector<String>();
			for (DiffEntry entry : diff) {
				String fileName = dir + "/" + entry.getNewPath(); 
				System.out.println("Entry: " + fileName);
				System.out.println("File : " + entry.getNewPath());
				System.out.println("File : " + entry.getOldId());
				System.out.println("File : " + entry.getNewId());
				
				if(entry.getChangeType() == ChangeType.ADD) {
					addFiles.add(fileName);
				}
				if(entry.getChangeType() == ChangeType.MODIFY) {
					modFiles.add(fileName);
				}
				if(entry.getChangeType() == ChangeType.RENAME) {
					renFiles.add(fileName);
				}
				if(entry.getChangeType() == ChangeType.DELETE) {
					delFiles.add(fileName);
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
}
