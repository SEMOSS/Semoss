package prerna.algorithm.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	void setFiltered(String uniqueName, boolean filtered);
	
	void setPrimKey(String uniqueName, boolean primKey);
	
	void addDataType(String uniqueName, String dataType);
	
	
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
	
	List<String> getUniqueNames();
	
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
	String getDataType(String uniqueName);
	
	boolean isFiltered(String uniqueName);
	
	boolean isPrimKey(String uniqueName);
	
	Map<String, String[]> getPhysical2LogicalTranslations(Map<String,Set<String>> edgeHash, List<Map<String,String>> joins);

	String getLogicalNameForUniqueName(String uniqueName, String engineName);

}
