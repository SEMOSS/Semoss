package prerna.engine.impl.venv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.engine.api.VenvTypeEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.git.GitRepoUtils;

public class PythonVenvEngine extends AbstractVenvEngine {

	private static final Logger classLogger = LogManager.getLogger(PythonVenvEngine.class);

	private static final String DIR_SEPERATOR = "/";
	
	private String providerName = null;
	private String remoteName = null;
	private String remoteUrl = null;
	private String defaultBranch = null;
	private String requirementsFileName = null;
	private String repoUsername = null;
	private String repoPassword = null;
	
	private String venvFolderPath = null;
	private String localVenvVersionFolder = null;
	
	// keep track of folders and insert master py
	private File sitePackagesDirectory = null;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		if (this.smssProp.containsKey(Constants.GIT_PROVIDER)) {
			this.providerName = this.smssProp.getProperty(Constants.GIT_PROVIDER);
		}
		if (this.smssProp.containsKey("REMOTE_URL")) {
			this.remoteUrl = this.smssProp.getProperty("REMOTE_URL");
		}
		if (this.smssProp.containsKey(Constants.GIT_PROVIDER)) {
			this.defaultBranch = this.smssProp.getProperty(Constants.GIT_DEFAULT_BRANCH);
		}
		
		this.remoteName = this.smssProp.getProperty("REMOTE_NAME", "origin");
		this.requirementsFileName = this.smssProp.getProperty("REQUIREMENTS_FILE_NAME", "requirements.txt");
		this.repoUsername = this.smssProp.getProperty(Constants.USERNAME, null);
		this.repoPassword = this.smssProp.getProperty(Constants.PASSWORD, null);
						
		this.venvFolderPath = EngineUtility.getSpecificEngineBaseFolder(getCatalogType(), this.engineId, this.engineName);
		
		this.localVenvVersionFolder = this.venvFolderPath +
									  DIR_SEPERATOR + 
									  Constants.VERSION_FOLDER;
		
		File localRepo =  new File(this.localVenvVersionFolder);
		if (!localRepo.exists()) {
			localRepo.mkdirs();
		}
		
		if(!AssetUtility.isGit(this.localVenvVersionFolder) && this.remoteUrl != null) {
			GitRepoUtils.init(this.localVenvVersionFolder);
			
			Git thisGit = null;
			Repository thisRepo = null;
			StoredConfig config;
			try {
				thisGit = Git.open(new File(this.localVenvVersionFolder));
				thisRepo = thisGit.getRepository();
	            
				thisRepo = thisGit.getRepository();
				config = thisRepo.getConfig();
				config.setString("remote", this.remoteName , "url", this.remoteUrl);
				config.setString("remote", this.remoteName , "fetch", "+refs/heads/*:refs/remotes/" + this.remoteName + "/*");
				config.save();

			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error with adding the remote repository");
			} finally {
				if(thisRepo != null) {
					thisRepo.close();
				}
				if(thisGit != null) {
					thisGit.close();
				}
			}
		}
		
		if (this.sitePackagesDirectory == null && (new File(this.localVenvVersionFolder, "lib").exists())) {
			String sitePackagesPath = PyUtils.appendSitePackagesPath(this.localVenvVersionFolder);
			this.sitePackagesDirectory = new File(sitePackagesPath);
		}
	}
	
	@Override
	public List<Map<String, String>> listPackages() throws IOException, InterruptedException {
		String[] pipListCommand = new String[] {PyUtils.appendVenvPipExecutable(this.localVenvVersionFolder), "list", "--format", "json"};
		ProcessBuilder pipProcessBuilder = new ProcessBuilder(pipListCommand);
        Process pipProcess = pipProcessBuilder.start();
		//pipProcess.waitFor();
		
        // Get the input stream from the process
        InputStream inputStream = pipProcess.getInputStream();

        // Read the input stream using a BufferedReader and collect lines into a single string
        String jsonOutput = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining(System.lineSeparator()));
        
        // Convert JSON string to a List of Maps
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, String>> packages = objectMapper.readValue(jsonOutput, new TypeReference<List<Map<String, String>>>() {});

		return packages;
	}

	@Override
	public void pullRequirementsFile() {
		try (Git git = Git.open(new File(this.localVenvVersionFolder))) {
 
            FetchCommand fetchCommand = git.fetch();
      
            if(this.repoUsername != null && this.repoPassword != null && !this.repoUsername.isEmpty() && !this.repoPassword.isEmpty()) {
                fetchCommand.setCredentialsProvider(new UsernamePasswordCredentialsProvider(this.repoUsername, this.repoPassword));
			}
                        
            fetchCommand.call();
            
            // Create a CheckoutCommand
            CheckoutCommand checkoutCommand = git.checkout();

            checkoutCommand.setStartPoint(this.remoteName + DIR_SEPERATOR + this.defaultBranch);
            checkoutCommand.addPath(this.requirementsFileName);
                         
            checkoutCommand.call();

            classLogger.info("Successfully pulled requirements file from remote branch.");
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error pulling requirements file from repo.");
        }
	}
	
	@Override
	public void uploadRequirementsFile(String filePath) {
		File sourceFile = new File(filePath);
		File destinationFile = new File(this.localVenvVersionFolder, sourceFile.getName());
		
		try {
			// Check if the destination file exists, and if so, delete it
	        if (destinationFile.exists()) {
	        	FileUtils.forceDelete(destinationFile);
	        }
	        
			FileUtils.moveFileToDirectory(sourceFile, destinationFile, true);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to move " + sourceFile.getName() + " to virtual environment");
		}
	}

	@Override
	public void updateVirtualEnv() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void createVirtualEnv() throws Exception {
		if(!PyUtils.isPyPIReachable()){
    		throw new SemossPixelException("Unable to connect to PyPI. Please contact your administrator");
    	}
		
		// get the base python home path
		String py = PyUtils.getPythonHomeDir();
		// append the executable
		// append the executable
		if (SystemUtils.IS_OS_WINDOWS) {
			py = py + "/python.exe";
		} else {
			py = py + "/bin/python3";
		}
		py = py.replace("\\", DIR_SEPERATOR);

		// this is the command to create a python virtual environment
		String[] commands = new String[] {py, "-m", "venv", this.localVenvVersionFolder};
		ProcessBuilder processBuilder = new ProcessBuilder(commands);
		processBuilder.inheritIO(); 
		Process p = processBuilder.start();
		
		int createVenvExitCode = 127;
		try {
			createVenvExitCode = p.waitFor();
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			classLogger.error(Constants.STACKTRACE, ie);
		}
		classLogger.info("Created the virtual enviorment for engine " + this.engineId);
		
		if (createVenvExitCode == 0) {
			String[] pipInstallCommand = new String[] {PyUtils.appendVenvPipExecutable(this.localVenvVersionFolder), "install","-r", this.localVenvVersionFolder + "/" + this.requirementsFileName};
			ProcessBuilder pipProcessBuilder = new ProcessBuilder(pipInstallCommand);
            Process pipProcess = pipProcessBuilder.start();
            pipProcess.waitFor();
		}
		
		classLogger.info("Finished downloading packages for venv " + this.engineId);
		
		String sitePackagesPath = PyUtils.appendSitePackagesPath(this.localVenvVersionFolder);
		this.sitePackagesDirectory = new File(sitePackagesPath);
		
		// get the base pythons package locations as backups
		List<String> mainPySitePackages = PyUtils.getPythonHomeSitePackages();
		
		// create the .pth file to hold the base package paths
        Path path = Paths.get(this.sitePackagesDirectory.getAbsolutePath() + DIR_SEPERATOR + "mainPySitePackages.pth");
        Files.createFile(path);

        // Write the string content to the file
        Files.write(path, mainPySitePackages, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

        classLogger.info("File created and content written successfully.");
	}

	@Override
	public void addPackage(Map<String, Object> parameters) throws Exception {
		if (!parameters.containsKey("packageName")) {
			throw new IllegalArgumentException("\"packageName\" must be provided to add a package to this virtual environment.");
		}
		String packageName = (String) parameters.get("packageName");
		
		String packageVersion = null;
		if (parameters.containsKey("packageName")) {
			packageVersion = (String) parameters.get("packageVersion");
		}
		
		String packageArgument;
		if (packageVersion != null) {
			packageArgument = packageName + "==" + packageVersion;
		} else {
			packageArgument = packageName;
		}
		
		ProcessBuilder processBuilder = new ProcessBuilder(PyUtils.appendVenvPipExecutable(this.localVenvVersionFolder), "install", packageArgument);

        // Redirect the error stream to the output stream
        processBuilder.redirectErrorStream(true);

        // Start the process
        
        if (ClusterUtil.IS_CLUSTER) {
        	Set<String> foldersBeforePipProcess = new HashSet<>(Arrays.asList(this.sitePackagesDirectory.list()));
        	
        	Process process = processBuilder.start();

            // Wait for the process to complete
            int exitCode = process.waitFor();

            // Check the exit code
            if (exitCode == 0) {
            	classLogger.info("Package installation successful for virtual environment engine " + this.engineId);
            } else {
            	classLogger.error("Package installation failed with exit code: " + exitCode);
            	
            	// Get the input stream from the process
                InputStream inputStream = process.getInputStream();
            	// Read the input stream using a BufferedReader and collect lines into a single string
                String errorOutput = new BufferedReader(new InputStreamReader(inputStream))
                        .lines()
                        .collect(Collectors.joining(System.lineSeparator()));
                
                throw new RuntimeException(errorOutput);
            }
            
            Set<String> foldersAfterPipProcess = new HashSet<>(Arrays.asList(this.sitePackagesDirectory.list()));
            
            // Compare sets to find added and removed folders
            Set<String> addedFolders = new HashSet<>(foldersAfterPipProcess);
            addedFolders.removeAll(foldersBeforePipProcess);

            Set<String> removedFolders = new HashSet<>(foldersBeforePipProcess);
            removedFolders.removeAll(foldersAfterPipProcess);
            
            addPackagesToCloudStorage(addedFolders);
            removePackagesFromCloudStorage(removedFolders);
        } else {
        	Process process = processBuilder.start();

            // Wait for the process to complete
            int exitCode = process.waitFor();

            // Check the exit code
            if (exitCode == 0) {
            	classLogger.info("Package installation successful for virtual environment engine " + this.engineId);
            } else {
            	classLogger.error("Package installation failed with exit code: " + exitCode);
                      	
            	// Get the input stream from the process
                InputStream inputStream = process.getInputStream();
            	// Read the input stream using a BufferedReader and collect lines into a single string
                String errorOutput = new BufferedReader(new InputStreamReader(inputStream))
                        .lines()
                        .collect(Collectors.joining(System.lineSeparator()));
                
                throw new RuntimeException(errorOutput);
            }
        }
	}

	@Override
	public void removePackage(Map<String, Object> parameters) throws Exception {
		if (!parameters.containsKey("packageName")) {
			throw new IllegalArgumentException("\"packageName\" must be provided to remove the package from this virtual environment.");
		}
		String packageName = (String) parameters.get("packageName");
		
        ProcessBuilder processBuilder = new ProcessBuilder(PyUtils.appendVenvPipExecutable(this.localVenvVersionFolder), "uninstall", packageName, "-y");

        // Redirect the error stream to the output stream
        processBuilder.redirectErrorStream(true);
        
        if (ClusterUtil.IS_CLUSTER) {
    		Set<String> foldersBeforePipProcess = new HashSet<>(Arrays.asList(this.sitePackagesDirectory.list()));

        	// Start the process
            Process process = processBuilder.start();

            // Wait for the process to complete
            int exitCode = process.waitFor();
            
            // Check the exit code
            if (exitCode == 0) {
            	classLogger.info("Package uninstallation successful for virtual environment engine " + this.engineId);
            } else {
            	classLogger.error("Package uninstallation failed with exit code: " + exitCode);
            	// Get the input stream from the process
                InputStream inputStream = process.getInputStream();
            	// Read the input stream using a BufferedReader and collect lines into a single string
                String errorOutput = new BufferedReader(new InputStreamReader(inputStream))
                        .lines()
                        .collect(Collectors.joining(System.lineSeparator()));
                
                throw new RuntimeException(errorOutput);
            }
            
        	Set<String> foldersAfterPipProcess = new HashSet<>(Arrays.asList(this.sitePackagesDirectory.list()));
            Set<String> removedFolders = new HashSet<>(foldersBeforePipProcess);
            removedFolders.removeAll(foldersAfterPipProcess);
            removePackagesFromCloudStorage(removedFolders);
        } else {
        	// Start the process
            Process process = processBuilder.start();

            // Wait for the process to complete
            int exitCode = process.waitFor();

            // Check the exit code
            if (exitCode == 0) {
            	classLogger.info("Package uninstallation successful for virtual environment engine " + this.engineId);
            } else {
            	classLogger.error("Package uninstallation failed with exit code: " + exitCode);
            	// Get the input stream from the process
                InputStream inputStream = process.getInputStream();
            	// Read the input stream using a BufferedReader and collect lines into a single string
                String errorOutput = new BufferedReader(new InputStreamReader(inputStream))
                        .lines()
                        .collect(Collectors.joining(System.lineSeparator()));
                
                throw new RuntimeException(errorOutput);
            }
        }
	}

	private void removePackagesFromCloudStorage(Set<String> foldersToRemove) {
		if (foldersToRemove.size() > 0) {
        	List<String> foldersToRemoveFromCloud = new ArrayList<String>();
        	for (String removedPackage: foldersToRemove) {
        		File packageFile = new File(this.sitePackagesDirectory, removedPackage);
        		foldersToRemoveFromCloud.add(packageFile.getAbsolutePath());
        	}

        	Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), foldersToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
        }
	}
	
	private void addPackagesToCloudStorage(Set<String> foldersToAdd) {
		if (foldersToAdd.size() > 0) {
        	List<String> filesToAddToCloud = new ArrayList<String>();
        	for (String removedPackage: foldersToAdd) {
        		File packageFile = new File(this.sitePackagesDirectory, removedPackage);
        		filesToAddToCloud.add(packageFile.getAbsolutePath());
        	}
			
        	Thread copyFilesToCloudThread = new Thread(new CopyFilesToEngineRunner(engineId, this.getCatalogType(), filesToAddToCloud.stream().toArray(String[]::new)));
			copyFilesToCloudThread.start();
        }
	}
	
	@Override
	public VenvTypeEnum getVenvType() {
		return VenvTypeEnum.PYTHON;
	}

	@Override
	public String pathToExecutable() {
		if (SystemUtils.IS_OS_WINDOWS) {
    		return  this.localVenvVersionFolder + "/Scripts";
    	} else {
    		return  this.localVenvVersionFolder;
    	}
	}
}
