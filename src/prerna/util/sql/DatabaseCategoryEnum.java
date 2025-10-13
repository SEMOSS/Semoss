package prerna.util.sql;

/**
 * Enum to categorize database types as SQL or NoSQL
 */
public enum DatabaseCategoryEnum {

	SQL("SQL"), NOSQL("NoSQL"), UNKNOWN("Unknown");

	private final String categoryName;

	DatabaseCategoryEnum(String categoryName) {
		this.categoryName = categoryName;
	}

	public String getCategoryName() {
		return this.categoryName;
	}

	/**
	 * Determines the database category
	 * 
	 * @param rdbmsType The database_subtype string from the SMSS file
	 * @return DatabaseCategoryEnum (SQL or NOSQL)
	 */
	public static DatabaseCategoryEnum getCategoryFromRdbmsType(String rdbmsType) {
		if (rdbmsType == null || rdbmsType.trim().isEmpty()) {
			return UNKNOWN;
		}

		RdbmsTypeEnum rdbmsEnum = RdbmsTypeEnum.getEnumFromString(rdbmsType.trim());
		if (rdbmsEnum != null) {
			switch (rdbmsEnum) {
			case CASSANDRA:
			case ELASTIC_SEARCH:
			case OPEN_SEARCH:
				return NOSQL;
			default:
				return SQL; // most entries are SQL databases
			}
		}

		// Check for known NoSQL types that might not be in RdbmsTypeEnum yet
		String upperRdbmsType = rdbmsType.toUpperCase().trim();
		switch (upperRdbmsType) {
		case "MONGODB":
		case "NEO4J":
		case "JANUSGRAPH":
		case "TINKER":
			return NOSQL;
		default:
			return UNKNOWN;
		}
	}
}