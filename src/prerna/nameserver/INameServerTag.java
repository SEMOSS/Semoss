package prerna.nameserver;

public interface INameServerTag {

	enum TAG_TYPE {ENGINE_TAG, PERSPECTIVE_TAG, QUERY_PARSE_TAG, PARAMETER_TAG, USER_DEFINED_TAG, ALGORITHM_TAG};
	
	/**
	 * Return the type of the insight tag
	 * @return
	 */
	String getType();
	
	/**
	 * Return the value of the tag
	 * @return
	 */
	String getValue();
}
