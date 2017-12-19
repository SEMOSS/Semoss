package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;

public class GitDestroyer {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitDestroyer() {

	}

	public static void removeFiles(String localRepository) {
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(localRepository));
			status = thisGit.status().call();
		} catch (IOException | NoWorkTreeException | GitAPIException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
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
				throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
			}
		}

		thisGit.close();
	}

	public static void removeSpecificFiles(String localRepository, List<String> files) {
		if(files == null || files.size() == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}

		RmCommand rm = thisGit.rm();
		for(String daFile : files) {
			if(daFile.contains("version")) {
				daFile = daFile.substring(daFile.indexOf("version") + 8);
			}
			daFile = daFile.replace("\\", "/");
			rm.addFilepattern(daFile);
		}

		try {
			rm.call();
		} catch (GitAPIException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
		}

		thisGit.close();
	}
	
	public static void removeSpecificFiles(String localRepository, File... files) {
		if(files == null || files.length == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}

		RmCommand rm = thisGit.rm();
		for(File f : files) {
			String daFile = f.getAbsolutePath();
			if(daFile.contains("version")) {
				daFile = daFile.substring(daFile.indexOf("version") + 8);
			}
			daFile = daFile.replace("\\", "/");
			rm.addFilepattern(daFile);
		}

		try {
			rm.call();
		} catch (GitAPIException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
		}

		thisGit.close();
	}
}
