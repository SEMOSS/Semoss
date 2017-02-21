package prerna.algorithm.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.ds.QueryStruct;

//import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface IMetaData {

//	@Deprecated
//	Vertex upsertVertex(String type, String uniqueName, String logicalName, String instancesType, String physicalUri,
//			String engineName, String dataType, String parentIfProperty);
//	@Deprecated
//	void addDataType(Vertex vert, String dataType);
	
	/**
	 * Keeps track of how a name came to be associated with a meta node
	 * An alias can be defined by the user, the physical name from a db, etc.
	 * @author bisutton
	 *
	 */
	enum NAME_TYPE {USER_DEFINED, DB_PHYSICAL_NAME, DB_PHYSICAL_URI, DB_LOGICAL_NAME, DB_QUERY_STRUCT_NAME}
	enum DATA_TYPES {NUMBER, STRING, DATE}

	public static DATA_TYPES convertToDataTypeEnum(String dataType) {
		dataType = dataType.toUpperCase();
		if(dataType.contains("STRING") || dataType.contains("TEXT") || dataType.contains("VARCHAR")) {
			return IMetaData.DATA_TYPES.STRING;
		} 
		else if(dataType.contains("INT") || dataType.contains("DECIMAL") || dataType.contains("DOUBLE") || dataType.contains("FLOAT") || dataType.contains("LONG") || dataType.contains("BIGINT")
				|| dataType.contains("TINYINT") || dataType.contains("SMALLINT") || dataType.contains("NUMBER")){
			return IMetaData.DATA_TYPES.NUMBER;
		} 
		else if(dataType.contains("DATE")) {
			return IMetaData.DATA_TYPES.DATE;
		}
		
		return null;
	}
	
	
//////////////////::::::::::::::::::::::: SETTER METHODS :::::::::::::::::::::::::::::::://////////////////////////////
//	/**
//	 * In RDBMS this is a table. In RDF/Tinker this is a node
//	 */
//	void storeVertex(Object uniqueName, Object howItsCalledInDataFrame);
//	
//	/**
//	 * In RDBMS this is a column in a table. In RDF/Tinker this is a property on a node.
//	 */
//	void storeProperty(Object uniqueName, Object howItsCalledInDataFrame, Object parent);
	
	/**
	 * In RDBMS this is a foreign key relationship. In RDF/Tinker this is a relationship.
	 * 
	 * @param uniqueName1
	 * @param uniqueName2
	 */
	void storeRelation(String uniqueName1, String uniqueName2);
	
	/**
	 * Stores an alias associated with a node.
	 * Storing engine details on a node will automatically associate those as aliases, so this method is really only needed for user defined alias
	 * 
	 * @param uniqueName	unique name of node getting an alias
	 * @param aliasName		alias name to associate with the node
	 */
	void storeUserDefinedAlias(String uniqueName, String aliasName);
	
	/**
	 * Store an engine as in "this engine helped populate this node"
	 * Uses physical Uri to get logical name, physical name, and data type for given node
	 * Physical Uri must align with OWL
	 * 
	 * @param uniqueName	unique name of the node that got populated
	 * @param engineName	name of the engine that helped populate
	 * @param physicalUri	physical uri of the node in that engine
	 */
	void storeEngineDefinedVertex(String uniqueName, String uniqueParentNameIfProperty, String engineName, String DB_QUERY_STRUCT_NAME);
	
	void storeVertex(String uniqueName, String value, String uniqueParentNameIfProperty);
	
	/**
	 * Store the data type for a given node
	 * This setter will update the overall data type such that the least restrive data type
	 * is used if multiple are present
	 * @param uniqueName
	 * 				The unique identifier
	 * @param dataType
	 * 				The data type to store
	 */
	void storeDataType(String uniqueName, String dataType);
	
	void storeDataTypes(String[] uniqueNames, String[] dataTypes);
	
	void setFiltered(String uniqueName, boolean filtered);
	
	void setPrimKey(String uniqueName, boolean primKey);
	
	void setDerived(String uniqueName, boolean derived);
	
	void setDerivedCalculation(String uniqueName, String calculation);
	
	void setDerivedUsing(String uniqueName, String... otherUniqueNames);

	void dropVertex(String uniqueName);
	
	void save(String baseFileName);
	
//////////////////::::::::::::::::::::::: GETTER METHODS :::::::::::::::::::::::::::::::://////////////////////////////
	/**
	 * Gets all of the columns that are properties
	 * Return format is:
	 * {
	 * 		uniqueLogicalName ---> parent's uniqueLogicalName
	 * }
	 * @return	All properties currently in datamaker
	 */
	Map<String, String> getProperties();

	/**
	 * Gets the physical uri used for a specific node in a specific engine
	 * 
	 * e.g. http://semoss.org/ontologies/Concept/Title/Title
	 * 
	 * @param uniqueNodeName	Unique name for the node
	 * @param engineName	Engine for which you want the physical name
	 * @return	Physical URI
	 */
	String getPhysicalUriForNode(String uniqueName, String engineName);

	/**
	 * Uses the node unique name to get all engines associate with that node
	 * 
	 * @param uniqueName	Unique name for the meta node
	 * @return	All engines that have been used to fill that column in the datamaker
	 */
	Set<String> getEnginesForUniqueName(String uniqueName);
	
	/**
	 * Get all the aliases for the unique identifier
	 * @param unqiueName
	 * 				The unique identifier
	 * @return
	 */
	Set<String> getAlias(String uniqueName);
	
	String getValueForUniqueName(String uniqueName);
	
	String getAliasForUniqueName(String uniqueName);
	
	String getUniqueNameForAlias(String aliasName);
	
	List<String> getColumnNames();
	List<String> getColumnAliasName();
	
	/**
	 * Get all aliases for the unique identifier and the source they came from
	 * @param uniqueName
	 * 				The unique identifier
	 * @return
	 */
	 Map<String, Map<String, Object>> getAliasMetaData(String uniqueName);
	
	/**
	 * Get the overall data type for the unique identifier: string, number, or date
	 * Least restrictive data type is used when there are multiple data types present	
	 * @param uniqueName
	 * 				The unique identifier
	 * @return
	 */
	DATA_TYPES getDataType(String uniqueName);
	
	/**
	 * Get the specific data type for the database in which the instance data sits: Integer, Varchar, Double, etc.
	 * @param uniqeName
	 * 			The unique identifier
	 * @return
	 */
	String getDBDataType(String uniqeName);
	
	Map<String, String> getAllUniqueNamesToValues();	
	
	boolean isFiltered(String uniqueName);
	
	boolean isPrimKey(String uniqueName);
	
	boolean isDerived(String uniqueName);
	
	Map<String, String[]> getPhysical2LogicalTranslations(Map<String,Set<String>> edgeHash, List<Map<String,String>> joins, Map<String, Boolean> makeUniqueNameMap);

	String getLogicalNameForUniqueName(String uniqueName, String engineName);

	Map<String, Set<String>> getEdgeHash();

	QueryStruct getQueryStruct(String startingPoint);
	
	void open(String baseFileName);

	List<Map<String, Object>> getTableHeaderObjects();

	void unfilterAll();

	Map<String, String> getFilteredColumns();

	boolean isConnectedInDirection(String colValue, String addedType);
	
	String getParentValueOfUniqueNode(String uniqueName);

	List<String> getPrimKeys();

	void setVertexValue(String string, String tableName);
	
	void setVertexAlias(String string, String tableName);

	String getLatestPrimKey();

	Map<String, IMetaData.DATA_TYPES> getColumnTypes();
	
	Map<String, String> getDBColumnTypes();

	void modifyUniqueName(String existingName, String newName);

	void addEngineForUniqueName(String columnName, String engineName);
}
