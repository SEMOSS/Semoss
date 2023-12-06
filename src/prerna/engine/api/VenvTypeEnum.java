package prerna.engine.api;

import prerna.engine.impl.venv.PythonVenvEngine;


public enum VenvTypeEnum {

	PYTHON("PYTHON", PythonVenvEngine.class.getName());
	
	private String venvName;
	private String venvClass;
	
	VenvTypeEnum(String venvName, String venvClass) {
		this.venvName = venvName;
		this.venvClass = venvClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVenvClass() {
		return this.venvClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVenvName() {
		return this.venvName;
	}
	
	/**
	 * 
	 * @param name
	 * @return
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