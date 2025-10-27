package prerna.engine.api;

import java.util.List;
import java.util.Map;

/**
 * Interface for virtual environment engines that manage isolated execution environments.
 * 
 * <p>This interface extends {@link IEngine} to provide comprehensive virtual environment
 * management capabilities for programming languages like Python, R, and others. Virtual
 * environment engines enable dependency isolation, version management, and reproducible
 * execution contexts for code and analytics operations.</p>
 * 
 * <p>Key capabilities include:</p>
 * <ul>
 *   <li><strong>Environment Creation:</strong> Setup isolated virtual environments</li>
 *   <li><strong>Package Management:</strong> Install, update, and remove packages</li>
 *   <li><strong>Requirements Management:</strong> Handle requirements files and dependencies</li>
 *   <li><strong>Version Control:</strong> Manage multiple package versions</li>
 *   <li><strong>Execution Context:</strong> Provide paths to environment executables</li>
 * </ul>
 * 
 * <p>Virtual environments are essential for:</p>
 * <ul>
 *   <li>Dependency isolation between different projects</li>
 *   <li>Reproducible analytics environments</li>
 *   <li>Security sandboxing for code execution</li>
 *   <li>Version compatibility management</li>
 * </ul>
 * 
 * @see {@link IEngine} for base engine functionality
 * @see {@link VenvTypeEnum} for supported virtual environment types
 * @author SEMOSS
 */
public interface IVenvEngine extends IEngine {
	
	/** Configuration key for the virtual environment type */
	String VENV_TYPE = "VENV_TYPE";
	
	/**
	 * Gets the specific type of virtual environment managed by this engine.
	 * 
	 * @return The {@link VenvTypeEnum} representing this engine's environment type
	 * @see {@link VenvTypeEnum} for available virtual environment types
	 */
	VenvTypeEnum getVenvType();
	
	/**
	 * Lists all packages installed in the virtual environment with their versions.
	 * 
	 * <p>This method provides a comprehensive inventory of all packages currently
	 * installed in the virtual environment, including their version numbers.
	 * This is useful for environment auditing, dependency management, and
	 * troubleshooting compatibility issues.</p>
	 * 
	 * @return List of maps containing package names and versions
	 * @throws Exception If package listing fails or environment is not accessible
	 */
	List<Map<String, String>> listPackages() throws Exception;

	/**
	 * Pulls the requirements file from a remote Git repository.
	 * 
	 * <p>This method retrieves a requirements file (e.g., requirements.txt for Python)
	 * from a configured remote Git repository, enabling centralized dependency
	 * management and version control of environment specifications.</p>
	 */
	void pullRequirementsFile();
	
	/**
	 * Uploads a requirements file from a local file path.
	 * 
	 * <p>This method provides an alternative to pulling requirements from Git
	 * by allowing direct upload of a requirements file from the local file system.
	 * This is useful for custom environments or when Git integration is not available.</p>
	 * 
	 * @param filePath Absolute path to the local requirements file to upload
	 */
	void uploadRequirementsFile(String filePath);
	
	/**
	 * Creates the virtual environment and installs all specified packages.
	 * 
	 * <p>This method performs the complete virtual environment setup process:</p>
	 * <ol>
	 *   <li>Creates the isolated virtual environment</li>
	 *   <li>Installs the base language runtime (Python, R, etc.)</li>
	 *   <li>Installs all packages specified in the requirements file</li>
	 *   <li>Configures the environment for use by the engine</li>
	 * </ol>
	 * 
	 * @throws Exception If environment creation or package installation fails
	 */
	void createVirtualEnv() throws Exception;

	/**
	 * Updates the virtual environment with new or modified requirements.
	 * 
	 * <p>This method refreshes the virtual environment by re-pulling the
	 * requirements file and updating packages as needed. This is typically
	 * called when the requirements file has been modified in the repository.</p>
	 */
	void updateVirtualEnv();
	
	/**
	 * Adds a new package to the virtual environment.
	 * 
	 * <p>This method installs additional packages into the virtual environment.
	 * Access to this operation is restricted to administrators only for security
	 * and environment stability reasons.</p>
	 * 
	 * @param parameters Map containing package specification including name, version,
	 *                   and other installation parameters
	 * @throws Exception If package installation fails or user lacks permissions
	 */
	void addPackage(Map<String, Object> parameters) throws Exception;
	
	/**
	 * Removes a package from the virtual environment.
	 * 
	 * <p>This method uninstalls packages from the virtual environment.
	 * Access to this operation is restricted to administrators only to prevent
	 * accidental removal of critical dependencies.</p>
	 * 
	 * @param parameters Map containing package identification and removal options
	 * @throws Exception If package removal fails or user lacks permissions
	 */
	void removePackage(Map<String, Object> parameters) throws Exception;
	
	/**
	 * Gets the absolute path to the virtual environment's executable.
	 * 
	 * <p>This method returns the path to the primary executable (python, R, etc.)
	 * within the virtual environment. This path is used for executing code
	 * within the isolated environment context.</p>
	 * 
	 * @return Absolute path to the virtual environment executable
	 */
	String pathToExecutable();
}
