package prerna.om;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;

import org.apache.commons.lang.SystemUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

public class UserVenv implements Serializable {
	private static final Logger classLogger = LogManager.getLogger(UserVenv.class);
	
	public String userVenvId = "";
	// This is the path to the temporary insight server directory used to run python and virtual envs
	// EX: C:/workspace/Semoss/InsightCache/a653411963489424001
	public String tempInsightDir = "";
	public String venvPath = "";
	public String[] pipList = {};
	
	public UserVenv(String tempInsightDir) {
		String[] idAndPaths = this.extractIdAndCreatePaths(tempInsightDir);
		this.userVenvId = idAndPaths[0];
		this.tempInsightDir = idAndPaths[1];
		this.venvPath = idAndPaths[2];
		
		try {
			createVirtualEnv(idAndPaths[2]);
		} catch (InterruptedException ie) {
			classLogger.info("FAILED TO CREATE USER VIRTUAL ENV!");
			classLogger.error(Constants.STACKTRACE, ie);
		} catch (IOException ioe) {
	        classLogger.info("FAILED TO CREATE USER VIRTUAL ENV DUE TO I/O ERROR!");
	        classLogger.error(Constants.STACKTRACE, ioe);
	    }
		
	}
	
	// Extract the id and create the normalized paths for the constructor
	private String[] extractIdAndCreatePaths(String tempInsightDir) {
		String normalizedPath = Utility.normalizePath(tempInsightDir);
		int lastIndex = normalizedPath.lastIndexOf("/");
		
		String extractedId = normalizedPath.substring(lastIndex + 1);
		String venvPath = normalizedPath + "/venv";
		
		return new String[] {extractedId, normalizedPath, venvPath};
	}
	
	// This method creates a Python virtual environment to a given path
	public static void createVirtualEnv(String venvPath) throws IOException, InterruptedException {
		String[] creationCommand = getVenvCreationCmd();
	    ProcessBuilder pb = new ProcessBuilder(creationCommand[0], creationCommand[1], creationCommand[2], venvPath);
	    pb.redirectErrorStream(true);
	    Process process = pb.start();
	    
	    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
	        String line;
	        while ((line = reader.readLine()) != null) {
	            System.out.println(line);
	        }
	    }
	    
	    int exitCode = process.waitFor();
	    if (exitCode != 0) {
	        throw new IOException("Failed to create virtual environment, exit code: " + exitCode);
	    }
	}
	
	public static final String[] getVenvCreationCmd() {
	    if (SystemUtils.IS_OS_WINDOWS) {
	        return new String[]{"python", "-m", "venv"};
	    } else {
	        return new String[]{"python3", "-m", "venv"};
	    }
	}
	
    public static final String getVenvActivationCmd(String venvPath) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return venvPath + "\\Scripts\\activate";
        } else {
            return venvPath + "/bin/activate";
        }
    }
    
    public static final String[] getPipInstallCmd(String library) {
        if (SystemUtils.IS_OS_WINDOWS) {
        	return new String[]{"cmd", "/c", "pip", "install", library};
        } else {
        	return new String[]{"/bin/bash", "-c", "pip install " + library};
        }
    }
    
    // Combining the activation and installation commands to be used in one process builder
    public static final String[] getFullCommand(String activationCommand, String[] installCommand) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return new String[]{"cmd", "/c", activationCommand + " && " + String.join(" ", installCommand)};
        } else {
        	return new String[] {"/bin/bash", "-c", ". " + activationCommand + " && " + String.join(" ", installCommand)};
        }
    }
    
    public String installLibrary(Insight insight, String library) throws IOException, InterruptedException {
        String venvPath = this.venvPath;
        String activationCommand = getVenvActivationCmd(venvPath);
        String[] installCommand = getPipInstallCmd(library);
        
        // Combine activation and installation command
        String[] combinedCommand = getFullCommand(activationCommand, installCommand); 

        ProcessBuilder pb = new ProcessBuilder(combinedCommand[0], combinedCommand[1], combinedCommand[2]);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        int exitCode = process.waitFor();
        if (exitCode == 0) {
            return "Successfully installed library: " + library;
        } else {
            output.append("Failed to install library ").append(library).append(", exit code: ").append(exitCode);
            return output.toString();
        }
    }
}
