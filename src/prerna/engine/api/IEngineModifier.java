package prerna.engine.api;

public interface IEngineModifier {

	/**
	 * Set the engine for the modifier class
	 * @param engine
	 */
	void setEngine(IDatabase engine);

	/**
	 * Add a property to an existing concept
	 * @param existingConcept
	 * @param newColumn
	 * @param dataType
	 */
	void addProperty(String existingConcept, String newColumn, String dataType) throws Exception;

	/**
	 * Remove a property on an existing concept
	 * @param existingConcept
	 * @param existingColumn
	 * @throws Exception
	 */
	void removeProperty(String existingConcept, String existingColumn) throws Exception;
	
	/**
	 * Renames a property for an existing concept
	 * @param existingConcept
	 * @param existingColumn
	 * @param newColumn
	 * @throws Exception
	 */
	void renameProperty(String existingConcept, String existingColumn, String newColumn) throws Exception;


	/**
	 * Edit the data type of an existing property
	 * @param existingConcept
	 * @param existingColumn
	 * @param newDataType
	 * @throws Exception
	 */
	void editProperty(String existingConcept, String existingColumn, String newDataType) throws Exception;

	/**
	 * Add an index to a specific concept/column
	 * @param existingConcept
	 * @param existingColumn
	 * @param indexName
	 * @param addIfExists
	 * @throws Exception
	 */
	void addIndex(String existingConcept, String existingColumn, String indexName, boolean addIfExists) throws Exception;
	
	/**
	 * Renames an existing concept
	 * @param existingConcept
	 * @param newConcept
	 * @throws Exception
	 */
	void renameConcept(String existingConcept, String newConcept) throws Exception;
	
}
