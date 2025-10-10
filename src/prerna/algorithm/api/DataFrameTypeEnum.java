package prerna.algorithm.api;

/**
 * Enumeration that defines the different types of data frames supported by the system.
 * Each data frame type represents a different underlying implementation or technology
 * for storing and processing data.
 * 
 * <p>
 * Data frame types determine how data is stored, accessed, and manipulated within
 * the system. Different types may offer different performance characteristics,
 * feature sets, or integration capabilities.
 * </p>
 * 
 * @see {@link #getTypeAsString()} for string representation of the type
 */
public enum DataFrameTypeEnum {

	/** Grid-based data frame implementation for tabular data structures. */
	GRID ("GRID"),
	
	/** Graph-based data frame implementation for network and relationship data. */
	GRAPH ("GRAPH"),
	
	/** R language integration data frame for statistical computing. */
	R ("R"),
	
	/** Python language integration data frame for data science operations. */
	PYTHON ("PY"),
	
	/** Native Java implementation data frame for optimal performance. */
	NATIVE ("NATIVE"), 
	
	/** iGraph library integration data frame for advanced graph operations. */
	IGRAPH ("IGRAPH");
	
	/** The string representation of the data frame type. */
	private String type;
	
	/**
	 * Constructs a data frame type with the specified string representation.
	 *
	 * @param type The string representation of the data frame type.
	 */
	DataFrameTypeEnum(String type) {
		this.type = type;
	}
	
	/**
	 * Returns the string representation of this data frame type.
	 *
	 * @return The string representation of this data frame type.
	 */
	public String getTypeAsString() {
		return this.type;
	}
	
	/**
	 * Returns the string representation of this data frame type.
	 *
	 * @return The string representation of this data frame type.
	 */
	@Override
	public String toString() {
		return getTypeAsString();
	}
	
}
