package prerna.sablecc2.reactor.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.reactors.GitBaseReactor;

public class CommandReactor extends GitBaseReactor {
	
	private static final String CLASS_NAME = CommandReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public CommandReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey()};
		this.keyRequired = new int[]{1};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String command = keyValue.get(keysToGet[0]);
		CmdExecUtil util = this.insight.getCmdUtil();

		if(util == null)
			return getError("No context is set - please use SetContext(<mount point>) to set context");

		
		// uncomment this line to see it in action. We want to test it for .. etc. before committing into play.
		//util = null;
		String git = "";
		String gitCommand = null;
		
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
			if(isCloneAllowed != null && !isCloneAllowed)
				return NounMetadata.getErrorNounMessage("Clone is not allowed at this level ");				
		}		
				
		// pre-processing for cd
		// basically we try to see if the cd dir being done exists
		// if so no issue ootherwise need to gitclone from the repository
		if(git != null && git.equalsIgnoreCase("cd") && gitCommand != null)
		{
			File file = new File(util.getWorkingDir() + "/" + gitCommand);
			if(!file.exists())
			{
				// clone the git repository
				cloneRepo(gitCommand, util.getWorkingDir());
			}
		}	
		
		
		// pre-process mkdir to say you cannot create folders at main level
		if(git.equalsIgnoreCase("mkdir") && util.getWorkingDir().endsWith("app_root"))
			return NounMetadata.getErrorNounMessage("You cannot make directory in app root folder");

		String output = util.executeCommand(command);
		
		////////////////////////////////////////// POST PROCESSING //////////////////////////////////////////////
		
		// post processing
		if(git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("clone"))
		{
			// try to see if this is a clone if so add it to the clone properties
			postProcessClone(command, util.getWorkingDir());
		}	

		if((command.startsWith("dir") || command.startsWith("ls")))
		{
			// try to see if this is a clone if so add it to the clone properties
			String dir = util.getWorkingDir();
			//if(gitCommand != null) - this will break for ls -l
			//	dir = gitCommand;
			output = postProcessDir(command, dir, output);
		}	
		
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
				
				if(url != null && url.contains("github")) // need something to say these are Oauth2'able
				{
					GitPushUtils.push(workingDir, remoteName, branch, token);	
					return new NounMetadata("Pushing Git", PixelDataType.CONST_STRING, PixelOperationType.HELP);
				}
			}
		}
		return null;
	}
	
	private void postProcessClone(String command, String workingDir)
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
				if(appBaseFolder.endsWith("app_root") && new File(appBaseFolder + "/version").exists() && new File(appBaseFolder + "/" + dirName).exists())
				{
					// we are in the right location process now
					// add this to the properties
					// get the root
					try {
						File repoFile = new File(appBaseFolder + "/version/repoList.txt");
						if(!repoFile.exists())
							repoFile.createNewFile();
						
						Properties prop = new Properties();
						FileInputStream fis = new FileInputStream(repoFile);

						prop.load(fis);
						prop.put(dirName, repoURL);
						FileOutputStream fos = new FileOutputStream(repoFile);
						prop.store(fos, "Updating");
						
						// need to commit this file
						// cd into the version
						// git add *
						// git commit -m "adding repos"
						
						
						this.insight.getCmdUtil().executeCommand("cd version");
						this.insight.getCmdUtil().executeCommand("git add *");
						this.insight.getCmdUtil().executeCommand("git commit -m \"adding repos\" "); // dont know if we need to add the author here else it complains on config
						
						fis.close();
						fos.close();
						
						
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
		try {
			File repoFile = new File(workingDir + "/version/repoList.txt");
			
			Properties prop = new Properties();
			FileInputStream fis = new FileInputStream(repoFile);
			prop.load(fis);
			
			
			String url = prop.getProperty(repoName);
			
			insight.getCmdUtil().executeCommand("git clone " + url);
			
			fis.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private String postProcessDir(String repoName, String workingDir, String output)
	{
		String newOutput = output;
		
		File repoFile = new File(workingDir + "/version/repoList.txt");

		if(repoFile.exists())
		{
			Properties prop = new Properties();
			
			try {
				FileInputStream fis = new FileInputStream(repoFile);
				prop.load(fis);
				
				String repos = "While the directories are not shown, Following Repos are available:";
				Enumeration keys = prop.keys();
				while(keys.hasMoreElements())
					repos = repos + "   " + keys.nextElement();
				
						
				repos = repos + "\n" + "You can cd into any of these dirs and when you do the git clone will be invoked at this level automatically ";		
				fis.close();
				newOutput = newOutput +"\n" + repos;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return newOutput;
	}
	
}
