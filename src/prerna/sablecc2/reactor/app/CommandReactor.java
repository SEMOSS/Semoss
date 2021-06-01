package prerna.sablecc2.reactor.app;

import java.util.StringTokenizer;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.git.GitPushUtils;
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
		
		StringTokenizer commands = new StringTokenizer(command);

		
		if(commands.countTokens() >= 2)
		{
			String gitCommand = commands.nextToken();
			String push = commands.nextToken();
			
			
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
				String workingDir = util.getWorkingDir();

				// get the oauth token
				String token = getToken();
				
				GitPushUtils.push(workingDir, remoteName, branch, token);
				
				return new NounMetadata("Pushing Git", PixelDataType.CONST_STRING, PixelOperationType.HELP);
			}
		}
		
		
		// uncomment this line to see it in action. We want to test it for .. etc. before committing into play.
		//util = null;
		
		// all of this can be moved into the context reactor
		if(util == null)
			return getError("No context is set - please use SetContext(<mount point>) to set context");
		String output = util.executeCommand(command);
		// need to replace the app with the 
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.HELP);
	}

}
