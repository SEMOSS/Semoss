package prerna.algorithm.api;

public enum DataFrameTypeEnum {

	GRID ("GRID"),
	GRAPH ("GRSPH"),
	R ("R"),
	PYTHON ("PY"),
	NATIVE ("NATIVE"), 
	IGRAPH ("IGRAPH");
	
	private String type;
	
	DataFrameTypeEnum(String type) {
		this.type = type;
	}
	
	public String getTypeAsString() {
		return this.type;
	}
	
	@Override
	public String toString() {
		return getTypeAsString();
	}
	
}
