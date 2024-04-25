package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.CreateBranchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import prerna.auth.AuthProvider;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GitPushUtils {
	
	private static final Logger logger = LogManager.getLogger(GitPushUtils.class);

	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

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
	@Deprecated
	// this is moved to git repo utils
	public static void addSpecificFiles(String localRepository, List<String> files) {
		if(files == null || files.isEmpty()) {
			return;
		}
		Git thisGit = null;
		AddCommand ac = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to Git directory at " + localRepository);
		}
		if (thisGit != null) {
			ac = thisGit.add();
		}

		if (ac != null) {
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
		}

		if (thisGit != null) {
			thisGit.close();
		}
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
		AddCommand ac = null;
		try {
			thisGit = Git.open(new File(localRepository));
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		if (thisGit != null) {
			ac = thisGit.add();
		}

		if (ac != null) {
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
		}

		if (thisGit != null) {
			thisGit.close();
		}
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
				logger.error(Constants.STACKTRACE, e);
			}
			CredentialsProvider cp = new UsernamePasswordCredentialsProvider(userName, password);
			RefSpec spec = new RefSpec("+refs/heads/master:refs/heads/master");
			
			if (thisGit != null) {
				PushCommand pc = thisGit.push();
				pc.setRefSpecs(spec);
				pc.setRemote(remoteToPush);
				pc.setCredentialsProvider(cp);
				try {
					pc.call();
				} catch (GitAPIException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				thisGit.close();
			}
		}
	}
	
	/*************** OAUTH Overloads Go Here ***********************/
	/***************************************************************/

	public static void push(String repository, String remoteToPush, String branch, String token) {
		int attempt = 1;
		
		String gitProvider = Utility.getDIHelperProperty(Constants.GIT_PROVIDER);
		if(gitProvider != null && !(gitProvider.isEmpty()) && gitProvider.toLowerCase().equals(AuthProvider.GITLAB.toString().toLowerCase())) {
			push(repository, remoteToPush, branch, token, AuthProvider.GITLAB, attempt);
		} else {
			push(repository, remoteToPush, branch, token, AuthProvider.GITHUB, attempt);
		}
	}

	public static void push(String repository, String remoteToPush, String branch, String token, AuthProvider prov, int attempt) {
		if(attempt < 3) {
			Boolean isGitlab = (prov == AuthProvider.GITLAB);

			File dirFile = new File(Utility.normalizePath(repository));
			Git thisGit = null;
			try {
				thisGit = Git.open(dirFile);
			
				CredentialsProvider cp = null; 
				if(isGitlab) {
					cp = new UsernamePasswordCredentialsProvider("oauth2", token);
				} else {
					cp = new UsernamePasswordCredentialsProvider(token, "");
				}
	
				PushCommand pc = thisGit.push();
				pc.setRemote(remoteToPush);
				if(branch != null && !branch.isEmpty()) {
					pc.add(branch);
				}
				pc.setCredentialsProvider(cp);
				try {
					pc.call();
				} catch (GitAPIException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(thisGit != null) {
					thisGit.close();
				}
			}
		}
	}
	
	
	public static NounMetadata pull(String repository, String token, AuthProvider prov )
	{
		
			Boolean isGitlab = null;
			if(prov.toString().equals(AuthProvider.GITLAB.toString())) {
				isGitlab=true;
			} else {
				isGitlab=false;
			}
			
			File dirFile = new File(Utility.normalizePath(repository));
			Git thisGit = null;
			try {
				thisGit = Git.open(dirFile);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			CredentialsProvider cp = null; 
			if(isGitlab) {
				 cp = new UsernamePasswordCredentialsProvider("oauth2", token);
			} else {
				 cp = new UsernamePasswordCredentialsProvider(token, "");
			}

			if (thisGit != null) {
				PullCommand pc = thisGit.pull();
				pc.setCredentialsProvider(cp);
				try {
					PullResult pr = pc.call();
					if(pr.isSuccessful()) {
						return new NounMetadata("Git Pulled: " + pr.isSuccessful() , PixelDataType.CONST_STRING, PixelOperationType.HELP);
					} else{
						return new NounMetadata("Git pull error", PixelDataType.CONST_STRING, PixelOperationType.HELP);
					}
				} catch (GitAPIException e) {
					logger.error(Constants.STACKTRACE, e);
					return new NounMetadata("Git Pull Error: "+ e, PixelDataType.ERROR, PixelOperationType.HELP);
				} finally {
					thisGit.close();
				}

		}
			return new NounMetadata("Git Pull Error - Git is empty ", PixelDataType.ERROR, PixelOperationType.HELP);

	}

	public static NounMetadata checkout(String repository, String branch, String token, AuthProvider prov )
	{
		
			Boolean isGitlab = null;
			if(prov.toString().equals(AuthProvider.GITLAB.toString())) {
				isGitlab=true;
			} else {
				isGitlab=false;
			}
			
			File dirFile = new File(Utility.normalizePath(repository));
			Git thisGit = null;
			boolean exists = false;
			try {
				thisGit = Git.open(dirFile);
				exists = branchNameExist(thisGit, branch);
			} catch (IOException | GitAPIException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			CredentialsProvider cp = null; 
			if(isGitlab) {
				 cp = new UsernamePasswordCredentialsProvider("oauth2", token);
			} else {
				 cp = new UsernamePasswordCredentialsProvider(token, "");
			}

			if (thisGit != null) {
				CheckoutCommand checkout = thisGit.checkout();

				if(!exists) {

				checkout.setCreateBranch(true);
				checkout.setName(branch);
				checkout.setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.TRACK);
				checkout.setStartPoint("origin/"+branch);
				} else {
					checkout.setName(branch);
				}
				try {
					checkout.call();
					return new NounMetadata("Git checkout: " + branch, PixelDataType.CONST_STRING, PixelOperationType.HELP);
				} catch (GitAPIException e) {
					logger.error(Constants.STACKTRACE, e);
					return new NounMetadata("Git Checkout Error: "+ e, PixelDataType.ERROR, PixelOperationType.HELP);
				} finally {
					thisGit.close();
				}
			
		}
			return new NounMetadata("Git Checkout Error - Git is empty ", PixelDataType.ERROR, PixelOperationType.HELP);
	}
	
	/**
    *
    * <p>
    * Description: Determine whether the local branch name exists
    * </p>
    *
    * @param git
    * @param branchName
    * @return
    * @throws GitAPIException
    * @author wgs
          * @date July 20, 2019 2:49:46 PM
    *
    */
   public static boolean branchNameExist(Git git, String branchName) throws GitAPIException {
       List<Ref> refs = git.branchList().call();
       for (Ref ref : refs) {
           if (ref.getName().contains(branchName)) {
               return true;
           }
       }
       return false;
   }

   public static NounMetadata clone(String workingDir, String repo, String token, AuthProvider prov) {
	   return clone(workingDir, repo, token, prov, true);
   }

   
   public static NounMetadata clone(String workingDir, String repo, String token, AuthProvider prov, boolean appendFolderName) {
	   Boolean isGitlab = (prov == AuthProvider.GITLAB);
	   workingDir = Utility.normalizePath(workingDir);
	   
	   File dirFile = null;
	   if(appendFolderName) {
		   String dirName = Utility.getInstanceName(repo).split(Pattern.quote("."))[0];
		   dirFile = new File(workingDir+FILE_SEPARATOR+dirName);
	   } else {
		   dirFile = new File(workingDir);
	   }

	   String trustedRepo = Utility.getDIHelperProperty(Constants.GIT_TRUSTED_REPO);
	   String defaultBranch =  Utility.getDIHelperProperty(Constants.GIT_DEFAULT_BRANCH);

	   if(trustedRepo!=null && !trustedRepo.isEmpty()) {
		   if(!repo.startsWith(trustedRepo)) {
			   return new NounMetadata("Git clone Error: Cloning from unapproved git registry" , PixelDataType.ERROR, PixelOperationType.HELP);
		   }
	   }

	   CredentialsProvider cp = null;
	   if(token != null) {
		   if(isGitlab) {
			   cp = new UsernamePasswordCredentialsProvider("oauth2", token);
		   } else {
			   cp = new UsernamePasswordCredentialsProvider(token, "");
		   }
		   logger.info("Cloning project " + repo + " with " + prov + " credentials");
	   } else {
		   logger.info("Cloning project " + repo + " without any credentials");
	   }

	   CloneCommand clone = Git.cloneRepository();
	   clone.setURI(repo);
	   clone.setDirectory(dirFile);
	   if(cp != null) {
		   clone.setCredentialsProvider(cp);
	   }
	   if(trustedRepo!=null && !trustedRepo.isEmpty()) {
		   if(defaultBranch!=null && !defaultBranch.isEmpty()) {
			   clone.setBranch(defaultBranch);
		   }
	   }

	   try {
		   clone.call();
		   return new NounMetadata("Git clone success: " + repo, PixelDataType.CONST_STRING, PixelOperationType.HELP);
	   } catch (GitAPIException e) {
		   logger.error(Constants.STACKTRACE, e);
		   return new NounMetadata("Git clone error: "+ e, PixelDataType.ERROR, PixelOperationType.HELP);
	   }

   }

}
