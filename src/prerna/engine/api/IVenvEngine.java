package prerna.engine.api;

import java.util.List;
import java.util.Map;

public interface IVenvEngine extends IEngine {
	
	String VENV_TYPE = "VENV_TYPE";
	
	VenvTypeEnum getVenvType();
	
	/*
	 * List all packages and the respective versions	
	*/	
	List<Map<String, String>> listPackages() throws Exception;

	/*
	 * Pull the requirements file from a remote repo using git
	*/	
	void pullRequirementsFile();
	
	
	/*
	 * Instead of pulling a requirements file let it be uploaded
	*/	
	void uploadRequirementsFile(String filePath);
	
	/*
	 * The actual process implementation to create the virtual environment and install the relevant packages
	*/	
	void createVirtualEnv() throws Exception;
	

	
	/*
	 * The requirements file has been updated and needs to be re-pulled
	*/	
	void updateVirtualEnv();
	
	
	/*
	 *  Add a package to the venv. Restricted for Admins only
	*/	
	void addPackage(Map<String, Object> parameters) throws Exception;
	
	
	/*
	 *  Remove a package to the venv. Restricted for Admins only
	*/	
	void removePackage(Map<String, Object> parameters) throws Exception;
	
	
	/*
	 *  Get the path to the venv executable
	*/	
	String pathToExecutable();
}
