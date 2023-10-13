package prerna.reactor.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;

import prerna.auth.AuthProvider;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.reactors.GitBaseReactor;

public class CommandReactor extends GitBaseReactor {
	
	private static final String CLASS_NAME = CommandReactor.class.getName();
	
	static final Set<String> approvedProdCommands = new HashSet<String>(
		       Arrays.asList("PULL", "CLONE", "RESET", "STATUS"));
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public CommandReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey()};
		this.keyRequired = new int[]{1};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		String gitProvider = DIHelper.getInstance().getProperty(Constants.GIT_PROVIDER);
		
		if(gitProvider == null) {
			gitProvider="";
		}
		
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_terminal)) {
					throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			 }
		}
		//check if git is disabled
		String disable_git_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_GIT_TERMINAL);
		if(disable_git_terminal != null && !disable_git_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_git_terminal)) {
					throw new IllegalArgumentException("Git terminal has been disabled.");
			 }
		}
	
		
		organizeKeys();
		String command = keyValue.get(keysToGet[0]);
		CmdExecUtil util = this.insight.getCmdUtil();

		if(util == null) {
			return getError("No context is set - please use SetContext(<mount point>) to set context");
		}
		
		// uncomment this line to see it in action. We want to test it for .. etc. before committing into play.
		//util = null;
		String git = "";
		String gitCommand = null;
		String preCloneMessage = null;
		String postCloneMessage = null;
		
		StringTokenizer commands = new StringTokenizer(command);
		if(commands.countTokens() >= 2)
		{
			git = commands.nextToken().trim();
			gitCommand = commands.nextToken().trim();
		}
		
		////////////////////////////////////////// PRE PROCESSING //////////////////////////////////////////////
		
		// process push
		// try to see if this is a push
		if(git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("push"))
		{
			NounMetadata pushOutput = processPush(command, util.getWorkingDir());		
			if(pushOutput != null)
				return pushOutput;
		}
		
		
		// pre processing for clone
		Boolean isCloneAllowed = null;
		
		if(git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("clone"))
		{
			isCloneAllowed = preProcessClone(command, util.getWorkingDir());
			
			// allow it but basically say we will blow your git folder away 
			if(isCloneAllowed != null && !isCloneAllowed)
				preCloneMessage = "You are cloning into a folder that is already part of git. Tracking at this level will be disabled";
				//return NounMetadata.getErrorNounMessage("Clone is not allowed at this level ");				
		}
				
		// pre-processing for cd
		// basically we try to see if the cd dir being done exists
		// if so no issue ootherwise need to gitclone from the repository
		if(git != null && git.equalsIgnoreCase("cd") && gitCommand != null)
		{
			File file = new File(Utility.normalizePath(util.getWorkingDir()) + "/" + gitCommand);
			if(!file.exists())
			{
				// clone the git repository
				cloneRepo(gitCommand, util.getWorkingDir());
			}
		}	
		
		
		// pre-process mkdir to say you cannot create folders at main level
		if(git.equalsIgnoreCase("mkdir") && util.getWorkingDir().endsWith("app_root"))
			return NounMetadata.getErrorNounMessage("You cannot make directory in app root folder");

		
		// pre process commit
		// add user name and email
		if(git != null && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("commit")) {
			// add the user name
			// git config user.name
			// git confir user.email
			// and user email
			String [] userEmail = this.insight.getUser().getUserCredential(AuthProvider.GITHUB);
			if(userEmail [0] == null) {
				// get it from the email
				userEmail[0] = userEmail[1].substring(0, userEmail[1].indexOf("@"));
			}
			this.insight.getCmdUtil().executeCommand("git config user.name " + userEmail[0]);
			this.insight.getCmdUtil().executeCommand("git config user.email " + userEmail[1]);
		}
		
		if(git != null &&  git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("config")) {
			// command should not be allowed.. 
			if(command.contains("global")) {
				return NounMetadata.getErrorNounMessage("Global config cannot be set in this environment");
			}
		}
		
		
		//check that it is only git pull or git clone in prod for CFG
		// for this, trusted repo and default branch must be limited
		if(git != null && git.equalsIgnoreCase("git") && gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			
			
			 String trustedRepo =  DIHelper.getInstance().getProperty(Constants.GIT_TRUSTED_REPO);
			 String defaultBranch =  DIHelper.getInstance().getProperty(Constants.GIT_DEFAULT_BRANCH);

			 if(trustedRepo!=null && !trustedRepo.isEmpty()) {
				 if(defaultBranch!=null && !defaultBranch.isEmpty()) {
					 if(!approvedProdCommands.contains(gitCommand.toUpperCase())){
							return NounMetadata.getErrorNounMessage("Only git clone, pull, status, reset are allowed in this environment");
					 }
				 }
			 }
		 
		} 
		
		if(git != null && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("pull") && gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String token = getToken();
			return GitPushUtils.pull(util.getWorkingDir(), token, AuthProvider.GITLAB);	
			//return new NounMetadata("Git Pulled", PixelDataType.CONST_STRING, PixelOperationType.HELP);

		} 
		
		
		if(git != null  && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("checkout") && gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String token = getToken();
			String branch = commands.nextToken();

			return GitPushUtils.checkout(util.getWorkingDir(), branch, token, AuthProvider.GITLAB);	
			//return new NounMetadata("Checked out " + branch, PixelDataType.CONST_STRING, PixelOperationType.HELP);

		}
		
		if(git != null  && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("clone") && gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String token = getToken();
			String repo = commands.nextToken();

			return GitPushUtils.clone(util.getWorkingDir(), repo, token, AuthProvider.GITLAB);	
			//return new NounMetadata("Cloned " + repo, PixelDataType.CONST_STRING, PixelOperationType.HELP);

		}


		String output = util.executeCommand(command);
		
		////////////////////////////////////////// POST PROCESSING //////////////////////////////////////////////
		
		// post processing
		if(git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("clone"))
		{
			// try to see if this is a clone if so add it to the clone properties
			postProcessClone(command, util.getWorkingDir(), isCloneAllowed);
			postCloneMessage = "If this is a java project, please make sure to adjust the target directory (XML Element build/directory) to ${classesDir}";
		}	

		if((command.startsWith("dir") || command.startsWith("ls")))
		{
			// try to see if this is a clone if so add it to the clone properties
			String dir = util.getWorkingDir();
			//if(gitCommand != null) - this will break for ls -l
			//	dir = gitCommand;
			output = postProcessDir(command, dir, output);
		}	
		if(preCloneMessage != null)
			output = preCloneMessage + "\n" + output;
		
		if(postCloneMessage != null)
			output = output + "\n" + postCloneMessage;
		
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.HELP);
	}
	
	private NounMetadata processPush(String command, String workingDir)
	{
		StringTokenizer commands = new StringTokenizer(command);
		if(commands.countTokens() >= 2)
		{
			String gitCommand = commands.nextToken().trim();
			String push = commands.nextToken().trim();
			
			
			if(gitCommand.equalsIgnoreCase("git") && push.equalsIgnoreCase("push"))
			{
				
				//TODO Kunal - This is where I can add the limitations on where you can push to
				// check should be if its not master, its probably okay
				
				String remoteName = "origin";
				if(commands.hasMoreTokens())
					remoteName = commands.nextToken();
				
				String branch = "master";
				if(commands.hasMoreTokens())
					branch = commands.nextToken();

				// need to process this further
				// typically git push origin master

				// get the oauth token
				String token = getToken();
				
				// do a quick check to see if the remote is 
				String url = GitRepoUtils.getConfigRemoteURL(workingDir, remoteName);
				//&& url.contains("github")
				if(url != null ) // need something to say these are Oauth2'able
				{
					GitPushUtils.push(workingDir, remoteName, branch, token);	
					return new NounMetadata("Pushing Git", PixelDataType.CONST_STRING, PixelOperationType.HELP);
				}
			}
		}
		return null;
	}
	
	
	
	private void postProcessClone(String command, String workingDir, boolean cloneAllowed)
	{
		StringTokenizer commands = new StringTokenizer(command);
		if(commands.countTokens() >= 2)
		{
			String gitCommand = commands.nextToken().trim();
			String push = commands.nextToken().trim();
			
			if(gitCommand.equalsIgnoreCase("git") && push.equalsIgnoreCase("clone"))
			{
				String repoURL = null;
				if(commands.hasMoreTokens())
					repoURL = commands.nextToken();
				
				String dirName = Utility.getInstanceName(repoURL);
				
				// see if this directory exists in base folder
				String appBaseFolder =  Utility.normalizePath(workingDir);
				if(cloneAllowed && appBaseFolder.endsWith("app_root") && new File(appBaseFolder + "/version").exists() && new File(appBaseFolder + "/" + dirName).exists())
				{
					// we are in the right location process now
					// add this to the properties
					// get the root
					FileInputStream fis = null;
					FileOutputStream fos = null;
					try {
						File repoFile = new File(appBaseFolder + "/version/repoList.txt");
						if(!repoFile.exists())
							repoFile.createNewFile();
						
						Properties prop = new Properties();
						fis = new FileInputStream(repoFile);

						prop.load(fis);
						prop.put(dirName, repoURL);
						fos = new FileOutputStream(repoFile);
						prop.store(fos, "Updating");
						
						// need to commit this file
						// cd into the version
						// git add *
						// git commit -m "adding repos"
						
						
						this.insight.getCmdUtil().executeCommand("cd version");
						this.insight.getCmdUtil().executeCommand("git add *");
						this.insight.getCmdUtil().executeCommand("git commit -m \"adding repos\" "); // dont know if we need to add the author here else it complains on config
						
						
						
						
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						try {
							if(fis != null)
								fis.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							if(fos != null)
								fos.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
				else if(!cloneAllowed)
				{
					File gitFolder = new File(Utility.normalizePath(workingDir) + File.separator + dirName + File.separator + ".git");
					if(gitFolder.exists())
					{
						try {
							FileUtils.deleteDirectory(gitFolder);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	private Boolean preProcessClone(String command, String workingDir)
	{
		StringTokenizer commands = new StringTokenizer(command);
		if(commands.countTokens() >= 2)
		{
			String gitCommand = commands.nextToken().trim();
			String push = commands.nextToken().trim();
			
			
			if(gitCommand.equalsIgnoreCase("git") && push.equalsIgnoreCase("clone"))
			{
				String repoURL = null;
				if(commands.hasMoreTokens())
					repoURL = commands.nextToken();
				
				String dirName = Utility.getInstanceName(repoURL);
				
				// see if this directory exists in base folder
				String appBaseFolder =  workingDir;
				if(appBaseFolder.endsWith("app_root") && new File(appBaseFolder + "/version").exists())
				{
					// we are in the right location process now
					// add this to the properties
					return true;
				}
				else 
					return false;
			}
		}
		return null;
	}	
	
	
	private void cloneRepo(String repoName, String workingDir)
	{
		FileInputStream fis = null;
		try {
			File repoFile = new File(Utility.normalizePath(workingDir) + "/version/repoList.txt");
			
			if(repoFile.exists())
			{
				Properties prop = new Properties();
				fis = new FileInputStream(repoFile);
				prop.load(fis);
				
				
				String url = prop.getProperty(repoName);
				
				insight.getCmdUtil().executeCommand("git clone " + url);
				
				fis.close();
			}			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if(fis != null)
					fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	private String postProcessDir(String repoName, String workingDir, String output)
	{
		String newOutput = output;
		
		File repoFile = new File(Utility.normalizePath(workingDir) + "/version/repoList.txt");

		if(repoFile.exists())
		{
			Properties prop = new Properties();
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(repoFile);
				prop.load(fis);
				
				String repos = "While the directories are not shown, Following Repos are available:";
				Enumeration keys = prop.keys();
				while(keys.hasMoreElements())
					repos = repos + "   " + keys.nextElement();
				
						
				repos = repos + "\n" + "You can cd into any of these dirs and when you do the git clone will be invoked at this level automatically ";		
				repos = repos + "\n\n" + "Version is SEMOSS's default git repository.";
				fis.close();
				newOutput = newOutput +"\n" + repos;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if(fis != null)
						fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return newOutput;
	}
	
}
