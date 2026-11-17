package prerna.reactor.security;

/**
 * The keys of the catalog dependency reactors
 */
final class CatalogDependencyKeys {

	/** The engine id or project id whose dependencies are read or set */
	static final String CATALOG_ID = "catalogId";
	/**
	 * Its engine type or project type, such as FUNCTION or SKILL. Optional: it is
	 * read from the security database when not given, and checked against it when
	 * given.
	 */
	static final String CATALOG_TYPE = "catalogType";
	/** The dependencies to set, each as a map of id and type */
	static final String DEPENDENCIES = "dependencies";

	private CatalogDependencyKeys() {

	}
}
