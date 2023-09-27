package prerna.engine.api;

public enum VectorDatabaseTypeEnum {

	FAISS("FAISS","prerna.engine.impl.vector.FaissDatabaseEngine");
	
	private String vectorDbName;
	private String vectorDbClass;
	
	VectorDatabaseTypeEnum(String storageName, String storageClass) {
		this.vectorDbName = storageName;
		this.vectorDbClass = storageClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVectorDatabaseClass() {
		return this.vectorDbClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVectorDatabaseName() {
		return this.vectorDbName;
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public static VectorDatabaseTypeEnum getEnumFromName(String name) {
		VectorDatabaseTypeEnum[] allValues = values();
		for(VectorDatabaseTypeEnum v : allValues) {
			if(v.getVectorDatabaseName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
