package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

public class GitPushUtils {

	/**
	 * This class is not intended to be extended or used outside of its static method
	 */
	private GitPushUtils() {
		
	}
	
	@Deprecated
	// this is moved to git repo utils
	public static void addAllFiles(String gitFolder, boolean ignoreTheIgnoreFiles) {
		Git thisGit = null;
		Status status = null;
		try {
			thisGit = Git.open(new File(gitFolder));
			status = thisGit.status().call();
		} catch (IOException | NoWorkTreeException | GitAPIException e) {
			e.printStackTrace();
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
				e.printStackTrace();
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
	@Deprecated
	// this is moved to git repo utils
	public static void addSpecificFiles(String localRepository, List<String> files) {
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
			e.printStackTrace();
		}
		thisGit.close();
	}
	
	/**
	 * Add specific files to a given git
	 * @param thisGit
	 * @param files
	 */
	@Deprecated
	// this is moved to git repo utils
	public static void addSpecificFiles(String localRepository, File[] files) {
		if(files == null || files.length == 0) {
			return;
		}
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			e.printStackTrace();
		}
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
			e.printStackTrace();
		}
		thisGit.close();
	}
	
	@Deprecated
	// this is moved to git repo utils
	public static void commitAddedFiles(String gitFolder) {
		commitAddedFiles(gitFolder, null);
	}
	
	@Deprecated
	// this is moved to git repo utils
	public static void commitAddedFiles(String gitFolder, String message) {
		commitAddedFiles(gitFolder, message, null, null);
	}

	@Deprecated
	// this is moved to git repo utils
	public static void commitAddedFiles(String gitFolder, String message, String author, String email) {
		Git thisGit = null;
		try {
			thisGit = Git.open(new File(gitFolder));
		} catch (IOException e) {
			e.printStackTrace();
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
			e.printStackTrace();
		}
		thisGit.close();
	}

	
	public static void push(String repository, String remoteToPush, String branch, String userName, String password)
	{
		int attempt = 1;
		push(repository, remoteToPush, branch, userName, password, attempt);
	}

	public static void push(String repository, String remoteToPush, String branch, String userName, String password, int attempt)
	{
		if(attempt < 3)
		{
			File dirFile = new File(repository);
			Git thisGit = null;
			try {
				thisGit = Git.open(dirFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider(userName, password);
			RefSpec spec = new RefSpec("+refs/heads/master:refs/heads/master");
	
			PushCommand pc = thisGit.push();
			pc.setRefSpecs(spec);
			pc.setRemote(remoteToPush);
			pc.setCredentialsProvider(cp);
			try {
				pc.call();
			} catch (GitAPIException e) {
				e.printStackTrace();
			}
			thisGit.close();
		}
	}
	
	/*************** OAUTH Overloads Go Here ***********************/
	/***************************************************************/

	public static void push(String repository, String remoteToPush, String branch, String token)
	{
		int attempt = 1;
		push(repository, remoteToPush, branch, token, attempt);
	}

	public static void push(String repository, String remoteToPush, String branch, String token, int attempt)
	{
		if(attempt < 3)
		{
			File dirFile = new File(repository);
			Git thisGit = null;
			try {
				thisGit = Git.open(dirFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider(token, "");
			RefSpec spec = new RefSpec("+refs/heads/master:refs/heads/master");
	
			PushCommand pc = thisGit.push();
			pc.setRefSpecs(spec);
			pc.setRemote(remoteToPush);
			pc.setCredentialsProvider(cp);
			try {
				pc.call();
			} catch (GitAPIException e) {
				e.printStackTrace();
			}
			thisGit.close();
		}
	}

	
}
