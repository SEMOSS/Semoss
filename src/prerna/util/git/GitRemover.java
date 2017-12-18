package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;

public class GitRemover {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitRemover() {
		
	}
	
	public static void removeFiles(String gitFolder) {
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(gitFolder));
			status = thisGit.status().call();
		} catch (IOException | NoWorkTreeException | GitAPIException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to connect to Git directory at " + gitFolder);
		}
		
		RmCommand rm = thisGit.rm();
		boolean removed = false;
		
		// get removed files
		Iterator <String> upFiles = status.getRemoved().iterator();
		while(upFiles.hasNext()) {
			removed = true;
			String daFile = upFiles.next();
			rm.addFilepattern(daFile);
		}
		
		// get missing files
		Iterator <String> modFiles = status.getMissing().iterator();
		while(modFiles.hasNext()) {
			removed = true;
			String daFile = upFiles.next();
			rm.addFilepattern(daFile);
		}

		if(removed) {
			try {
				rm.call();
			} catch (GitAPIException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Unable to remove files to Git directory at " + gitFolder);
			}
		}
		
		thisGit.close();
	}
}
