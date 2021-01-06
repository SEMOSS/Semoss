package prerna.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

public class CmdExecUtil {

	// the user already keeps a list of mappings
	// just need to use that directly
	String mountName = "appName";
	String mountDir =  "c:/users/pkapaleeswaran/workspacej3/gittest";
	String workingDir = mountDir;
	String commandAppender = "cmd";
	String pwdCommand = "pwd";

	
	public CmdExecUtil(String mountName, String mountDir)
	{
		getCommandAppender();
		mountDir = mountDir.replace("\\", "/");
		this.mountName = mountName;
		this.mountDir = mountDir;
		this.workingDir = mountDir;
		System.err.println("Working Dir is set to ..  " + workingDir);
	}
	
	public String executeCommand(String command)
	{
		// need a way to whitelist all the stuff here
		// like rm, del etc. etc. 
		String commandNotAllowed = commandAllowed(command);
		if(commandNotAllowed != null)
			return commandNotAllowed;
		
		String output = null;
		try {
			
			if(command.startsWith("cd"))
			{
				// remoe the cd and then add to working dir
				command = command.replace("cd", "");
				command = command.trim();
				if(command.startsWith("/"))
					output =  " Invalid command ";
				output = adjustWorkingDir(command);
			}
			else if(command.startsWith("pwd"))
			{
				output = workingDir;
			}
			else if(!command.startsWith("cd") && !command.startsWith("dir"))
			{
				String finalCommand = new String("");
				//command = "cd " + workingDir + " && " + command;
				// concat everything and then execute
				output = runCommand(command)[1];
			}
			else if(command.toLowerCase().startsWith("dir") || command.toLowerCase().startsWith("ls"))
			{
				
				StringBuilder finalCommand = new StringBuilder("");
				//command = "cd " + workingDir + " && " + command;
				String [] foutput = runCommand(command);
				boolean success = foutput[0].equalsIgnoreCase("true");
				output = foutput[1];
				
				if(success && output.length() > 0 && !output.toUpperCase().contains(mountDir.toUpperCase()))
					output = "No Such Directory ";
				else if(success && output.length() > 0)
				{
					//output = output.replace(mountDir, mountName);
				}
			}
			else // this is where we allow other commands
			{
				String []foutput = runCommand(command);
				output = foutput[1];
				if(output.length() == 0)
				{
					// add only if the output is not resulting
					String newCommand = command; // + " & cd"; 
					output = runCommand(newCommand)[1];
					output = output.replace("\\", "/");
					output = output.replace("\\r","");
					output = output.replace("\\n","");
					output = output.trim();
					System.err.println("[" + output + "]");
					if(output.toUpperCase().contains(mountDir.toUpperCase()))
					{
						workingDir = output;
					}
					else if(output.length() > 0)
						output = "Already at the root";
				}
				//output = output.replace(mountDir, mountName);

			}
		}  catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// replace the mount dir location
		output = replaceAppAlias(output);
		return output;
	}
	
	private String commandAllowed(String command)
	{
		// Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, git
		String upCommand = command.toUpperCase();
		upCommand = upCommand.trim();
		upCommand = upCommand.replace("\\","/"); // replace to forward slashes
		
		if(workingDir.equalsIgnoreCase(mountDir) && command.contains("..")) // you cannot do anything in the root
			return mountName;

		if((upCommand.startsWith("DEL") || upCommand.startsWith("RM") || upCommand.startsWith("CP") || upCommand.startsWith("COPY") || upCommand.startsWith("MV") || upCommand.startsWith("MOVE") ||  upCommand.startsWith("LS") || upCommand.startsWith("DIR") || upCommand.startsWith("PWD")) && (command.contains("..") || command.contains("\\") || command.contains("/")))
			return " Delete, move, copy, list is only allowed for a single level ";
		
		if(command.contains("&") || command.contains("&&"))
			return "Concatenating commands is not allowed";

		if(!upCommand.startsWith("CP") && !upCommand.startsWith("COPY") 
				&& !upCommand.startsWith("CD") 
				&& !upCommand.startsWith("DIR") && !upCommand.startsWith("LS") 
				&& !upCommand.startsWith("MV") && !upCommand.startsWith("MOVE") 
				&& !upCommand.startsWith("GIT") && !upCommand.startsWith("PWD"))
			return "Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, pwd, git ";
		
		return null;
		
	}

	private void getCommandAppender()
	{
		String osName = System.getProperty("os.name").toLowerCase();


		String starter = ""; 
		String[] commandsStarter = null;

		if (osName.indexOf("win") >= 0) 
		{
			this.commandAppender = "cmd";
			pwdCommand = "cd";
		}
		else
			this.commandAppender = "/bin/bash";

	}
	
	
	private String replaceAppAlias(String output)
	{
		String origOutput = output;
		while(output.toUpperCase().contains(mountDir.toUpperCase()))
		{
			// there could be 
			int index = output.toUpperCase().indexOf(mountDir.toUpperCase());
			output = output.substring(0, index) + mountName + output.substring(index + mountDir.length());
		}
		return output;
	}
	
	private String[] runCommand(String command) 
	{
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		boolean success = true;
		//command = commandAppender + command;
		DefaultExecutor executor;
		CommandLine cmdLine = new CommandLine(commandAppender);
		if(commandAppender.equalsIgnoreCase("/bin/bash"))
			cmdLine.addArgument("-c");
		else
			cmdLine.addArgument("/C");

		//command = "\"" + command + "\"";
		cmdLine.addArgument(command, false);
		
		//System.err.println("Running command ..  " + cmdLine);
		
		CollectingLogOutputStream clos = new CollectingLogOutputStream();
		executor = new DefaultExecutor();
		PumpStreamHandler streamHandler = new PumpStreamHandler(clos);
		executor.setStreamHandler(streamHandler);
		executor.setWorkingDirectory(new File(workingDir));
		ExecuteWatchdog watchdog = new ExecuteWatchdog(20000); // 20 seconds is plenty of time.. if the process doesnt return kill it
		executor.setWatchdog(watchdog);
		try
		{
			int exitValue = executor.execute(cmdLine);
		}catch(Exception ex)
		{
			success = false;
		}
		List <String> lines = clos.getLines();
		StringBuilder builder = new StringBuilder();
		for(int lineIndex = 0;lineIndex < lines.size();builder.append(lines.get(lineIndex)).append("\n"), lineIndex++);
		//System.out.println(" List " + lines);
		String output = builder.toString();;
		output = output.replace("\\", "/");
		
		String [] foutput = new String[2];
		foutput[0] = success +"";
		foutput[1] = output;
		
		return foutput;
	}
	
	
	public void runUserCommand()
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String data = br.readLine();
			while(data != null)
			{
				String output = executeCommand(data);
				System.err.println(output);
				System.err.println("Next Command : ");
				data = br.readLine();
			}
		}catch(Exception ex)
		{
			System.err.println("Exception is  " + ex);
		}
		
	}
	
	private String adjustWorkingDir(String command)
	{
		String [] cdTokens = command.split("/");
		for(int tokenIndex = 0;tokenIndex < cdTokens.length;tokenIndex++)
		{
			String curToken = cdTokens[tokenIndex];
			//System.out.println("Processing CD " + curToken);
			if(curToken.equalsIgnoreCase(".."))
			{
				String [] workdirTokens = workingDir.split("/");
				// take out the last one
				int wdTokenLength = workdirTokens.length;
				if(wdTokenLength > 1)
				{
					String lastToken = workdirTokens[workdirTokens.length -1];
					int lastIndex = workingDir.lastIndexOf("/" + lastToken);
					workingDir = workingDir.substring(0, lastIndex);
					//System.out.println("Working Dir " + workingDir);
				}
				else
				{
					workingDir = mountDir;
					return " Directory levels doesnt match navigation ";
				}
			}
			else 
			{
				if(!workingDir.endsWith("/"))
					workingDir = workingDir + "/" + curToken;
				else
					workingDir = workingDir + curToken;					
			}
		}
		return workingDir;
		
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		CmdExecUtil test = new CmdExecUtil("mango", "c:/users/pkapaleeswaran/workspacej3/gittest");
		test.runUserCommand();
		
	}
}
