package prerna.om;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.StringReader;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang.SystemUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.util.Constants;
import prerna.util.Utility;


public class UserVenv implements Serializable {
	private static final Logger classLogger = LogManager.getLogger(UserVenv.class);
	// EX: a3369751167853578692
	public String userbaseId = "";
	
	// EX: C:\\workspace\\Semoss\\InsightCache\\a3369751167853578692
	public String tempInsightDir = "";
	
	// EX: C:\\workspace\\Semoss\\InsightCache\\a3369751167853578692\\user_base\\Lib\\site-packages
	public String userbasePath = "";
	
	// EX: C:\\Users\\rweiler\\AppData\\Local\\anaconda3\\envs\\semoss-pytorch\\python.exe
	public String executablePath = "";
	
	// EX: C:\\Users\\rweiler\\AppData\\Local\\anaconda3\\envs\\semoss-pytorch\\Lib\\site-packages\\user_packages.pth
	public String pthFilePath = "";
	
	
	public String[] pipList = {};
	
	
	public String venvPath = "";


	public UserVenv(String tempInsightDir) {
		String tempInsightDirectory = Utility.normalizePath(tempInsightDir);
		this.tempInsightDir = tempInsightDirectory;
		int lastIndex = tempInsightDirectory.lastIndexOf("/");
		
		// Pull out the ID based on the random insight cache folder id
		this.userbaseId = tempInsightDirectory.substring(lastIndex + 1);
		
		// REMOVE
		String venvPath = tempInsightDirectory + "/venv";
		this.venvPath = venvPath;
		
		
		String pythonHomeDir = PyUtils.getPythonHomeDir();
		
		if (SystemUtils.IS_OS_WINDOWS) {
			this.executablePath = Utility.normalizePath(pythonHomeDir + "\\python.exe");
		} else {
			this.executablePath = Utility.normalizePath(pythonHomeDir + "/bin/python3");
		}
		
		
		String userbasePath = Utility.normalizePath(tempInsightDirectory + "\\user_base\\Lib\\site-packages");
		this.userbasePath = userbasePath;
		
		String pthFilePath = Utility.normalizePath(pythonHomeDir + "\\Lib\\site-packages\\user_packages.pth");
		this.pthFilePath = pthFilePath;
		

		// Creating the user_packages.pth file in the Python home site-packages directory 
		// This file points to the site-packages directory in the insight cache
//        try (FileWriter fileWriter = new FileWriter(pthFilePath, true)) {
//            fileWriter.write(userbasePath + "\n");
//        } catch (IOException e) {
//            classLogger.error("Failed to write to user_packages.pth file", e);
//        }
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
    
//    public final String[] getInstallCmd(String library) {
//        if (SystemUtils.IS_OS_WINDOWS) {
//            return new String[]{"cmd", "/c", this.executablePath, "-m", "pip", "install", "--target", this.userbasePath + "/Lib", library};
//        } else {
//            return new String[]{"/bin/bash", "-c", this.executablePath + " -m pip install --target " + this.userbasePath + "/Lib/site-packages " + library};
//        }
//    }
    
    public final String[] getInstallCmd(String library) {
        if (SystemUtils.IS_OS_WINDOWS) {
            // Construct the full path to site-packages
            String sitePackagesPath = this.userbasePath + "\\Lib\\site-packages";
            return new String[]{"cmd", "/c", this.executablePath, "-m", "pip", "install", "--no-cache-dir",  // Disable cache to ensure fresh install
                    "-t", sitePackagesPath, // Use -t to specify target directory directly
                    library}; 
        } else {
            // Existing Linux/macOS command should work 
            return new String[]{"/bin/bash", "-c", this.executablePath + " -m pip install --target " + this.userbasePath + "/Lib/site-packages " + library};
        }
    }


    public static final String[] getUninstallCmd(String library) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return new String[]{"cmd", "/c", "pip", "uninstall", "-y", library};
        } else {
            return new String[]{"/bin/bash", "-c", "pip uninstall -y " + library};
        }
    }
    
    // Combining the activation and a secondary command to be used in process builder
    public static final String[] getFullCommand(String activationCommand, String[] secondaryCommand) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return new String[]{"cmd", "/c", activationCommand + " && " + String.join(" ", secondaryCommand)};
        } else {
        	return new String[] {"/bin/bash", "-c", ". " + activationCommand + " && " + String.join(" ", secondaryCommand)};
        }
    }
    
    public static final String[] getListCommand(String venvPath) {
        String activationCommand = getVenvActivationCmd(venvPath);
        if (SystemUtils.IS_OS_WINDOWS) {
            return new String[]{"cmd", "/c", activationCommand + " && pip list --format=json"};
        } else {
            return new String[]{"/bin/bash", "-c", ". " + activationCommand + " && pip list --format=json"};
        }
    }
    
    public String installLibrary(String library, String version) throws IOException, InterruptedException {
        if (!version.isEmpty()) {
        	library = library + "==" + version;
        }
        
        String[] installCommand = getInstallCmd(library);
        

        ProcessBuilder pb = new ProcessBuilder(installCommand);
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
//        	updateUserSitePackagesPath();
            return "Successfully installed library: " + library;
        } else {
            output.append("Failed to install library ").append(library).append(", exit code: ").append(exitCode);
            return output.toString();
        }
    }
    
    public String removeLibrary(String library) throws IOException, InterruptedException {
    	String venvPath = this.venvPath;
    	String activationCommand = getVenvActivationCmd(venvPath);
    	String[] uninstallCommand = getUninstallCmd(library);
    	
    	// Combine activation command and uninstall command
    	String[] combinedCommand = getFullCommand(activationCommand, uninstallCommand);
    	
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
            return "Successfully uninstalled library: " + library;
        } else {
            output.append("Failed to uninstall library ").append(library).append(", exit code: ").append(exitCode);
            return output.toString();
        }
    }

    public static List<LibraryInfo> parsePipList(String pipListOutput) {
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new StringReader(pipListOutput));
        reader.setLenient(true); // Set lenient mode

        try {
            Type libraryListType = new TypeToken<List<LibraryInfo>>(){}.getType();
            return gson.fromJson(reader, libraryListType);
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            return new ArrayList<>(); // Return an empty list on failure
        }
    }

    // List the installed libraries in the user venv
    public List<LibraryInfo> pipList() throws IOException, InterruptedException {
        String[] listCommand = getListCommand(this.venvPath);
        ProcessBuilder pb = new ProcessBuilder(listCommand);
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
            return parsePipList(output.toString());
        } else {
            throw new IOException("Failed to list pip packages, exit code: " + exitCode);
        }
    }

    public static class LibraryInfo {
        private String name;
        private String version;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return "LibraryInfo{" +
                    "name='" + name + '\'' +
                    ", version='" + version + '\'' +
                    '}';
        }
    }
}



