package prerna.engine.api;

public interface IEngineModifier {

	/**
	 * Set the engine for the modifier class
	 * @param engine
	 */
	void setEngine(IEngine engine);

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
	
}
