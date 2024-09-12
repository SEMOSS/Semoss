package prerna.om;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.StringReader;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang.SystemUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

public class UserVenv implements Serializable {
	private static final Logger classLogger = LogManager.getLogger(UserVenv.class);
	// EX: a4461281385086976069
	public String userVenvId = "";
	// EX: C:\\workspace\\Semoss\\InsightCache\\a4461281385086976069
	public String tempInsightDir = "";
	// EX: C:\\workspace\\Semoss\\InsightCache\\a4461281385086976069\\venv
	public String venvPath = "";
	// EX: [{"name": "ephem", "version": "4.1.5"}, {"name": "pip", "version": "23.2.1"}, {"name": "setuptools", "version": "65.5.0"}]
	public List<LibraryInfo> libList;
	
	public Integer sitepackgeDirSize = 0;
	
	public UserVenv(String tempInsightDir) {
		String temporaryInsightDir = Utility.normalizePath(tempInsightDir);
		int lastIndex = temporaryInsightDir.lastIndexOf("/");
		this.tempInsightDir = temporaryInsightDir;
		this.userVenvId = temporaryInsightDir.substring(lastIndex + 1);
		String venvPath = Utility.normalizePath(temporaryInsightDir + "\\venv");
		this.venvPath = venvPath;
		try {
			createVirtualEnv(venvPath);
		} catch (InterruptedException ie) {
			classLogger.info("FAILED TO CREATE USER VIRTUAL ENV!");
			classLogger.error(Constants.STACKTRACE, ie);
		} catch (IOException ioe) {
	        classLogger.info("FAILED TO CREATE USER VIRTUAL ENV DUE TO I/O ERROR!");
	        classLogger.error(Constants.STACKTRACE, ioe);
	    }
		
	}
	
	// This method creates a Python virtual environment to a given path
	public static void createVirtualEnv(String venvPath) throws IOException, InterruptedException {
		String[] creationCommand = getVenvCreationCmd(venvPath);
	    ProcessBuilder pb = new ProcessBuilder(creationCommand);
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
	
	public static final String[] getVenvCreationCmd(String venvPath) {
	    if (SystemUtils.IS_OS_WINDOWS) {
	        return new String[]{"python", "-m", "venv", venvPath};
	    } else {
	        return new String[]{"python3", "-m", "venv", venvPath};
	    }
	}
	
    public static final String getVenvActivationCmd(String venvPath) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return venvPath + "\\Scripts\\activate";
        } else {
            return venvPath + "/bin/activate";
        }
    }
    
    public static final String[] getInstallCmd(String library) {
        if (SystemUtils.IS_OS_WINDOWS) {
        	return new String[]{"cmd", "/c", "pip", "install", library};
        } else {
        	return new String[]{"/bin/bash", "-c", "pip install " + library};
        }
    }
    
    public static final String[] getInstallFromRequirementsCmd(String requirementsPath) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return new String[]{"cmd", "/c", "pip", "install", "-r", requirementsPath};
        } else {
            return new String[]{"/bin/bash", "-c", "pip install -r " + requirementsPath};
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
        
        String venvPath = this.venvPath;
        String activationCommand = getVenvActivationCmd(venvPath);
        String[] installCommand = getInstallCmd(library);
        
        // Combine activation and installation command
        String[] combinedCommand = getFullCommand(activationCommand, installCommand); 

        ProcessBuilder pb = new ProcessBuilder(combinedCommand);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }
        
        venvUpdateHook();

        int exitCode = process.waitFor();
        if (exitCode == 0) {
            return "Successfully installed library: " + library;
        } else {
            output.append("Failed to install library ").append(library).append(", exit code: ").append(exitCode);
            return output.toString();
        }
    }
    
    public String installFromRequirements(String requirementsPath) throws IOException, InterruptedException {
    	String venvPath = this.venvPath;
        String activationCommand = getVenvActivationCmd(venvPath);
        String[] installCommand = getInstallFromRequirementsCmd(requirementsPath);
        
        String[] combinedCommand = getFullCommand(activationCommand, installCommand);
        
        ProcessBuilder pb = new ProcessBuilder(combinedCommand);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }
        
        venvUpdateHook();

        int exitCode = process.waitFor();
        if (exitCode == 0) {
            return "Successfully installed from requirements.txt";
        } else {
            output.append("Failed to install from requirements.txt").append(", exit code: ").append(exitCode);
            return output.toString();
        }
    }
    
    public String createRequirementsFile(String filePath) throws IOException, InterruptedException {
        if (!filePath.endsWith("requirements.txt")) {
            if (filePath.endsWith("/")) {
                filePath += "requirements.txt";
            } else {
                filePath += "/requirements.txt";
            }
        }
        
        File requirementsFile = new File(filePath);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(requirementsFile))) {
            for (LibraryInfo lib : this.libList) {
                writer.write(lib.getName() + "==" + lib.getVersion());
                writer.newLine();
            }
        }
        
        return "Successfully created requirements file at: " + filePath;
    }

    
    public String removeLibrary(String library) throws IOException, InterruptedException {
    	if (library == "pip" || library == "setuptools") {
    		return "Please do not remove " + library + " as this library is required.";
    	}
    	
    	String venvPath = this.venvPath;
    	String activationCommand = getVenvActivationCmd(venvPath);
    	String[] uninstallCommand = getUninstallCmd(library);
    	
    	// Combine activation command and uninstall command
    	String[] combinedCommand = getFullCommand(activationCommand, uninstallCommand);
    	
        ProcessBuilder pb = new ProcessBuilder(combinedCommand);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }
        
        venvUpdateHook();

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
        reader.setLenient(true);

        try {
            Type libraryListType = new TypeToken<List<LibraryInfo>>(){}.getType();
            return gson.fromJson(reader, libraryListType);
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            return new ArrayList<>();
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
    
    public static long getDirectorySize(File directory) {
        long size = 0;
        if (directory.isDirectory()) {
            for (File file : directory.listFiles()) {
                if (file.isFile()) {
                    size += file.length();
                } else if (file.isDirectory()) {
                    size += getDirectorySize(file);
                }
            }
        }
        return size;
    }

    // Get the size of the site-packages directory in MB
    public int getSitePackagesSize() {
        String sitePackagesPath = Utility.normalizePath(this.venvPath + "\\Lib\\site-packages");
        File sitePackagesDir = new File(sitePackagesPath);
        long sizeInBytes = getDirectorySize(sitePackagesDir);
        
        // Convert bytes to MB and round
        int sizeInMB = (int) Math.round(sizeInBytes / (1024.0 * 1024.0));
        return sizeInMB; 
    }
    
    // Update the site package directory size and current library list in a hook
    public void venvUpdateHook() {
    	this.sitepackgeDirSize = this.getSitePackagesSize();
    	try {
    	this.libList = this.pipList();
    	} catch(IOException io) {
    		classLogger.error(Constants.STACKTRACE, io);
    	} catch(InterruptedException ie) {
    		classLogger.error(Constants.STACKTRACE, ie);
    	}
    }
    
    public List<LibraryInfo> getLibraryList(){
    	return this.libList;
    }

    // This is a data structure for how we store the libraries list
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



