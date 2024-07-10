package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;

import prerna.util.Constants;

public class GitDestroyer {
	
	private static final Logger classLogger = LogManager.getLogger(GitDestroyer.class);

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitDestroyer() {

	}

	public static void removeFiles(String localRepository, boolean ignoreTheIgnoreFiles, boolean deleteFile) {
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(localRepository));
			status = thisGit.status().call();
		} catch (IOException | NoWorkTreeException | GitAPIException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}

		// if we want to delete the file, setCached needs to be set to false
		RmCommand rm = thisGit.rm().setCached(!deleteFile);
		boolean removed = false;

		// get removed files
		Iterator <String> remFiles = status.getRemoved().iterator();
		while(remFiles.hasNext()) {
			String daFile = remFiles.next();
			if(ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
				rm.addFilepattern(daFile);
				removed = true;
			}
		}

		// get missing files
		Iterator <String> misFiles = status.getMissing().iterator();
		while(misFiles.hasNext()) {
			String daFile = misFiles.next();
			if(ignoreTheIgnoreFiles || !GitUtils.isIgnore(daFile)) {
				rm.addFilepattern(daFile);
				removed = true;
			}
		}

		if(removed) {
			try {
				rm.call();
			} catch (GitAPIException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
			}
		}

		thisGit.close();
	}

	public static void removeSpecificFiles(String localRepository, boolean deleteFile, List<String> files) {
		if(files == null || files.size() == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}

		// if we want to delete the file, setCached needs to be set to false
		RmCommand rm = thisGit.rm().setCached(!deleteFile);
		for(String daFile : files) {
			if(daFile.contains("/version")) {
				daFile = daFile.substring(daFile.indexOf("/version") + 9);
			} else if(daFile.contains("\\version")) {
				daFile = daFile.substring(daFile.indexOf("\\version") + 9);
			}
			daFile = daFile.replace("\\", "/");
			rm.addFilepattern(daFile);
		}

		try {
			rm.call();
		} catch (GitAPIException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
		}

		thisGit.close();
	}
	
	public static void removeSpecificFiles(String localRepository, boolean deleteFile, File... files) {
		if(files == null || files.length == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}

		// if we want to delete the file, setCached needs to be set to false
		RmCommand rm = thisGit.rm().setCached(!deleteFile);
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to remove files to Git directory at " + localRepository);
		}

		thisGit.close();
	}
}
