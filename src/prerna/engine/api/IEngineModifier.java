package prerna.engine.api;

/**
 * Interface for modifying database engine schemas and structure dynamically.
 * 
 * <p>This interface provides a standardized way to perform schema modifications
 * on database engines without requiring direct database access. It abstracts
 * common database DDL (Data Definition Language) operations such as adding
 * columns, renaming tables, modifying data types, and managing indexes.</p>
 * 
 * <p>The interface uses semantic terminology where:</p>
 * <ul>
 *   <li><strong>Concept:</strong> Represents a table, entity, or data structure</li>
 *   <li><strong>Property:</strong> Represents a column, field, or attribute within a concept</li>
 * </ul>
 * 
 * <p>This abstraction allows the same interface to work across different database
 * types (RDBMS, RDF, Graph) while maintaining consistent semantics for schema
 * modification operations.</p>
 * 
 * @see {@link IDatabaseEngine} for database engine operations
 * @author SEMOSS
 */
public interface IEngineModifier {

	/**
	 * Associates this modifier with a specific database engine.
	 * 
	 * <p>This method establishes the connection between the modifier and the
	 * target database engine that will be modified. The modifier will use
	 * this engine to perform all schema modification operations.</p>
	 * 
	 * @param engine The {@link IDatabaseEngine} to be modified
	 * @see {@link IDatabaseEngine} for database engine interface
	 */
	void setEngine(IDatabaseEngine engine);

	/**
	 * Adds a new property (column) to an existing concept (table).
	 * 
	 * <p>This method extends an existing concept by adding a new property
	 * with the specified data type. The operation is equivalent to an
	 * ALTER TABLE ADD COLUMN statement in SQL databases.</p>
	 * 
	 * @param existingConcept The name of the existing concept to modify
	 * @param newColumn The name of the new property to add
	 * @param dataType The data type specification for the new property
	 * @throws Exception If the concept doesn't exist or the property cannot be added
	 */
	void addProperty(String existingConcept, String newColumn, String dataType) throws Exception;

	/**
	 * Removes an existing property (column) from a concept (table).
	 * 
	 * <p>This method removes a property from an existing concept, including
	 * all data stored in that property. The operation is equivalent to an
	 * ALTER TABLE DROP COLUMN statement in SQL databases.</p>
	 * 
	 * @param existingConcept The name of the existing concept to modify
	 * @param existingColumn The name of the property to remove
	 * @throws Exception If the concept or property doesn't exist or cannot be removed
	 */
	void removeProperty(String existingConcept, String existingColumn) throws Exception;
	
	/**
	 * Renames an existing property (column) within a concept (table).
	 * 
	 * <p>This method changes the name of an existing property while preserving
	 * all data and constraints. The operation is equivalent to an ALTER TABLE
	 * RENAME COLUMN statement in SQL databases.</p>
	 * 
	 * @param existingConcept The name of the existing concept containing the property
	 * @param existingColumn The current name of the property to rename
	 * @param newColumn The new name for the property
	 * @throws Exception If the concept or property doesn't exist or renaming fails
	 */
	void renameProperty(String existingConcept, String existingColumn, String newColumn) throws Exception;

	/**
	 * Modifies the data type of an existing property (column).
	 * 
	 * <p>This method changes the data type of an existing property. Data
	 * conversion may occur if the new type is compatible with existing data.
	 * The operation is equivalent to an ALTER TABLE ALTER COLUMN statement
	 * in SQL databases.</p>
	 * 
	 * @param existingConcept The name of the existing concept containing the property
	 * @param existingColumn The name of the property to modify
	 * @param newDataType The new data type specification for the property
	 * @throws Exception If the concept or property doesn't exist or type conversion fails
	 */
	void editProperty(String existingConcept, String existingColumn, String newDataType) throws Exception;

	/**
	 * Adds a database index to improve query performance on a specific property.
	 * 
	 * <p>This method creates an index on the specified concept/property combination
	 * to improve query performance. The operation is equivalent to a CREATE INDEX
	 * statement in SQL databases.</p>
	 * 
	 * @param existingConcept The name of the existing concept containing the property
	 * @param existingColumn The name of the property to index
	 * @param indexName The name for the new index
	 * @param addIfExists Whether to proceed if an index with the same name already exists
	 * @throws Exception If the concept or property doesn't exist or index creation fails
	 */
	void addIndex(String existingConcept, String existingColumn, String indexName, boolean addIfExists) throws Exception;
	
	/**
	 * Renames an existing concept (table) in the database.
	 * 
	 * <p>This method changes the name of an existing concept while preserving
	 * all data, properties, and relationships. The operation is equivalent to
	 * a RENAME TABLE statement in SQL databases.</p>
	 * 
	 * @param existingConcept The current name of the concept to rename
	 * @param newConcept The new name for the concept
	 * @throws Exception If the concept doesn't exist or renaming fails
	 */
	void renameConcept(String existingConcept, String newConcept) throws Exception;
	
}
