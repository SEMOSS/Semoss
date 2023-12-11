package prerna.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;

public class CmdExecUtil {

	private static final Logger classLogger = LogManager.getLogger(CmdExecUtil.class);
	
	// the user already keeps a list of mappings
	// just need to use that directly
	String mountName = "appName";
	String mountDir =  "c:/users/pkapaleeswaran/workspacej3/gittest";
	String workingDir = mountDir;
	String commandAppender = "cmd";
	String pwdCommand = "pwd";
	SocketClient tcpClient = null;
	String insightId = null;
	boolean init = false;

	
	public CmdExecUtil(String mountName, String mountDir, SocketClient tcpClient) {
		getCommandAppender();
		mountDir = mountDir.replace("\\", "/");
		this.mountName = mountName;
		this.mountDir = mountDir;
		this.workingDir = mountDir;
		this.tcpClient = tcpClient;
		classLogger.info("Working Dir is set to ..  " + workingDir);
		
		if(tcpClient != null) {
			pushMountToSocket();
		}
	}
	
	public void pushMountToSocket()
	{
		if(	//(tcpClient != null && !(tcpClient instanceof NativePySocketClient))
				//&&
				(DIHelper.getInstance().getLocalProp("core") == null || DIHelper.getInstance().getLocalProp("core").toString().equalsIgnoreCase("true"))
		   )
		{
			PayloadStruct ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {mountName, mountDir};
			ps.methodName = "constructor";
			ps.hasReturn = false;
			ps.insightId = mountName + "__" + mountDir;
			PayloadStruct retPS = (PayloadStruct)tcpClient.executeCommand(ps);	
			//init = true;
		}
	}
	
	public String executeCommand(String command)
	{
		String output = null;
		
		// may be do the check to see if tcp server is there
		if(	//(tcpClient != null && !(tcpClient instanceof NativePySocketClient))
			//		&&
				(DIHelper.getInstance().getLocalProp("core") == null || DIHelper.getInstance().getLocalProp("core").toString().equalsIgnoreCase("true"))
		   )
		{
			if(tcpClient == null)
			{
				return "Client is not connected on socket";
			}

			PayloadStruct ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {command};
			ps.methodName = "executeCommand";
			ps.insightId = mountName + "__" + mountDir;
			ps.payloadClasses = new Class[] {String.class};

			PayloadStruct retPS = (PayloadStruct)tcpClient.executeCommand(ps);
			return (String)retPS.payload[0];
		}
		else
		{
	
			// need a way to whitelist all the stuff here
			// like rm, del etc. etc. 
			String commandNotAllowed = commandAllowed(command);
			// allowing all commands
			
			/*if(commandNotAllowed != null)
				return commandNotAllowed;
			*/
			
			try {
				
				if(command.equalsIgnoreCase("reset"))
				{
					workingDir = mountDir;
					output = workingDir;
				}
				else if(command.startsWith("cd"))
				{
					// remoe the cd and then add to working dir
					command = command.replace("cd", "");
					command = command.trim();
					// disable this
					if(command.startsWith("/"))
					{
						output =  " Invalid command ";
					}
					else
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
	}
	
	private String commandAllowed(String command)
	{
		// Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, git
		String upCommand = command.toUpperCase();
		upCommand = upCommand.trim();
		upCommand = upCommand.replace("\\","/"); // replace to forward slashes
		
		if(workingDir.equalsIgnoreCase(mountDir) && command.contains("..")) // you cannot do anything in the root
			return mountName;

		//if((upCommand.startsWith("DEL") || upCommand.startsWith("RM") || upCommand.startsWith("CP") || upCommand.startsWith("COPY") || upCommand.startsWith("MV") || upCommand.startsWith("MOVE") ||  upCommand.startsWith("LS") || upCommand.startsWith("DIR") || upCommand.startsWith("PWD")) && (command.contains("..") || command.contains("\\") || command.contains("/")))
		//	return " Delete, move, copy, list is only allowed for a single level ";
		
		if(command.contains("&") || command.contains("&&"))
			return "Concatenating commands is not allowed";

		if(!upCommand.startsWith("CP") && !upCommand.startsWith("COPY") 
				&& !upCommand.startsWith("CD") 
				&& !upCommand.startsWith("DIR") && !upCommand.startsWith("LS") 
				&& !upCommand.startsWith("MV") && !upCommand.startsWith("MOVE") 
				&& !upCommand.startsWith("GIT") && !upCommand.startsWith("PWD") 
				&& !upCommand.startsWith("RESET") && !upCommand.startsWith("MVN")
				&& !upCommand.startsWith("DEL"))
			return "Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, pwd, git, mvn (Experimental) ";
		
		return null;
		
	}

	private void getCommandAppender() {
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.indexOf("win") >= 0) {
			String terminalMode = Utility.getDefaultTerminalMode();
			if(terminalMode.equals("cmd")) {
				this.commandAppender = "cmd";
				this.pwdCommand = "cd";
			} else {
				// if not cmd, then we are powershell
				this.commandAppender = "powershell.exe";
				this.pwdCommand = "pwd";
			}
		} else {
			this.commandAppender = "/bin/bash";
			this.pwdCommand = "pwd";
		}
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
		boolean success = true;
		DefaultExecutor executor;
		CommandLine cmdLine = new CommandLine(commandAppender);
		if(commandAppender.equalsIgnoreCase("/bin/bash")) {
			cmdLine.addArgument("-c");
		} else {
			cmdLine.addArgument("/C");
		}
		//command = "\"" + command + "\"";
		cmdLine.addArgument(command, false);
		
		//System.err.println("Running command ..  " + cmdLine);
		String [] foutput = new String[2];
		try(CollectingLogOutputStream clos = new CollectingLogOutputStream()){
			executor = new DefaultExecutor();
			PumpStreamHandler streamHandler = new PumpStreamHandler(clos);
			executor.setStreamHandler(streamHandler);
			executor.setWorkingDirectory(new File(Utility.normalizePath(workingDir)));
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
			
			foutput[0] = success +"";
			foutput[1] = output;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}		
		
		return foutput;
	}
	
	
	public void runUserCommand()
	{
		try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
			String data = br.readLine();
			while(data != null)
			{
				String output = executeCommand(data);
				System.err.println(output);
				System.err.println("Next Command : ");
				data = br.readLine();
			}
		} catch(Exception e) {
			System.err.println("Exception is  " + e);
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	private String adjustWorkingDir(String command)
	{
		//System.out.println("Working Dir Before " + workingDir);
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
					
					String newDir = Utility.normalizePath(workingDir.substring(0, lastIndex));
					if(new File(newDir).exists())
						workingDir = newDir;
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
				String newDir = null;
				if(!workingDir.endsWith("/"))
					newDir = workingDir + "/" + curToken;
				else
					newDir = workingDir + curToken;					
				
				// check to see if this is valid
				if(new File(Utility.normalizePath( newDir )).exists())
					workingDir = newDir;

			}
		}
		
		//System.out.println("Working Dir Set to " + workingDir);
		return workingDir;
	}
	
	public String getWorkingDir()
	{
		return this.workingDir;
	}
	
	public String getMountName()
	{
		return this.mountName;
	}
	
	public void setTcpClient(SocketClient tcpClient)
	{
		this.tcpClient = tcpClient;
		pushMountToSocket();
	}
	
//	public static void main(String[] args) throws Exception{
//		// TODO Auto-generated method stub
//		CmdExecUtil test = new CmdExecUtil("mango", "c:/users/pkapaleeswaran/workspacej3/gittest", null);
//		test.runUserCommand();
//	}
	
}


