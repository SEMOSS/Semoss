package prerna.engine.api;

import prerna.engine.impl.venv.PythonVenvEngine;

/**
 * Enumeration defining virtual environment types supported by the SEMOSS platform.
 * 
 * <p>This enum provides a registry of virtual environment engines that manage
 * isolated programming language environments for code execution. Virtual environments
 * enable dependency isolation, version management, and secure execution contexts
 * for different programming languages and their associated packages.</p>
 * 
 * <p>Virtual environment capabilities include:</p>
 * <ul>
 *   <li><strong>Dependency Isolation:</strong> Separate package installations per environment</li>
 *   <li><strong>Version Management:</strong> Multiple language versions and package versions</li>
 *   <li><strong>Security Isolation:</strong> Sandboxed execution environments</li>
 *   <li><strong>Reproducible Environments:</strong> Consistent execution contexts across deployments</li>
 * </ul>
 * 
 * <p>Currently supported environments focus on Python, with potential for expansion
 * to other languages like R, JavaScript (Node.js), Julia, and others based on
 * platform requirements.</p>
 * 
 * @see {@link IVenvEngine} for the virtual environment engine interface
 * @see {@link PythonVenvEngine} for Python virtual environment implementation
 * @author SEMOSS
 */
public enum VenvTypeEnum {

	/** Python virtual environment for isolated Python package management and execution */
	PYTHON("PYTHON", PythonVenvEngine.class.getName());
	
	/** The human-readable name identifier for this virtual environment type */
	private String venvName;
	/** The fully qualified class name of the implementing virtual environment engine */
	private String venvClass;
	
	/**
	 * Constructs a virtual environment type enum with the specified name and implementation class.
	 * 
	 * @param venvName The human-readable identifier for this virtual environment type
	 * @param venvClass The fully qualified class name of the implementation
	 */
	VenvTypeEnum(String venvName, String venvClass) {
		this.venvName = venvName;
		this.venvClass = venvClass;
	}
	
	/**
	 * Gets the fully qualified class name of the implementing virtual environment engine.
	 * 
	 * @return The complete class path for the virtual environment engine implementation
	 */
	public String getVenvClass() {
		return this.venvClass;
	}
	
	/**
	 * Gets the human-readable name identifier for this virtual environment type.
	 * 
	 * @return The virtual environment type name used for identification and configuration
	 */
	public String getVenvName() {
		return this.venvName;
	}
	
	/**
	 * Retrieves the virtual environment type enum that matches the specified name.
	 * 
	 * <p>This method performs a case-insensitive search through all available
	 * virtual environment types to find the one that matches the provided name.
	 * This is commonly used for configuration parsing and dynamic engine selection.</p>
	 * 
	 * @param name The virtual environment type name to search for (case-insensitive)
	 * @return The matching {@link VenvTypeEnum} instance
	 * @throws IllegalArgumentException If no virtual environment type matches the provided name
	 */
	public static VenvTypeEnum getEnumFromName(String name) {
		VenvTypeEnum[] allValues = values();
		for(VenvTypeEnum v : allValues) {
			if(v.getVenvName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}