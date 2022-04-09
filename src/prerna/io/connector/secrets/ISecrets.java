package prerna.io.connector.secrets;

import java.util.Map;

public interface ISecrets {

	String HASHICORP_VAULT = "VAULT";
	String IV = "iv";
	String SECRET = "secret";
	String SALT = "salt";
	
	/**
	 * Get the secrets associated with a project
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	Map<String, String> getProjectSecrets(String projectName, String projectId);
	
	/**
	 * Get the secrets associated with a database
	 * @param databaseName
	 * @param databaseId
	 * @return
	 */
	Map<String, String> getDatabaseSecrets(String databaseName, String databaseId);
	
	/**
	 * Get the secrets associated with an insight
	 * @param insightId
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	Map<String, String> getInsightSecrets(String insightId, String projectName, String projectId);

	/**
	 * Get the insight encryption key
	 * @param insightId
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	Map<String, Object> getInsightEncryptionSecrets(String insightId, String projectName, String projectId);
	
	/**
	 * Write a secret key-value pair for a database
	 * @param databaseName
	 * @param databaseId
	 * @param key
	 * @param value
	 * @return
	 */
	boolean writeDatabaseSecret(String databaseName, String databaseId, String key, Object value);

	/**
	 * Write a set of secret key-value pairs for a database
	 * @param databaseName
	 * @param databaseId
	 * @param nameValuePairs
	 * @return
	 */
	boolean writeDatabaseSecrets(String databaseName, String databaseId, Map<String, Object> nameValuePairs);

	/**
	 * Write a secret key-value pair for a project
	 * @param projectName
	 * @param projectId
	 * @param key
	 * @param value
	 * @return
	 */
	boolean writeProjectSecret(String projectName, String projectId, String key, Object value);

	/**
	 * Write a set of secret key-value pairs for a project
	 * @param projectName
	 * @param projectId
	 * @param key
	 * @param value
	 * @return
	 */
	boolean writeProjectSecrets(String projectName, String projectId, Map<String, Object> nameValuePairs);

	/**
	 * Write a secret key-value pair for a insight
	 * @param insightId
	 * @param projectName
	 * @param projectId
	 * @param key
	 * @param value
	 * @return
	 */
	boolean writeInsightSecret(String insightId, String projectName, String projectId, String key, Object value);

	/**
	 * Write a set of secret key-value pairs for a insight
	 * @param insightId
	 * @param projectName
	 * @param projectId
	 * @param nameValuePairs
	 * @return
	 */
	boolean writeInsightSecrets(String insightId, String projectName, String projectId, Map<String, Object> nameValuePairs);

	/**
	 * Write the secret for the insight encryption
	 * @param insightId
	 * @param projectName
	 * @param projectId
	 * @param nameValuePairs
	 * @return
	 */
	boolean writeInsightEncryptionSecrets(String insightId, String projectName, String projectId, Map<String, Object> nameValuePairs);

}