package prerna.io.connector.secrets;

import java.util.Map;

public interface ISecrets {

	String HASHICORP_VAULT = "HASHICORP_VAULT";

	Map<String, String> getProjectSecrets(String projectName, String projectId);
	
	Map<String, String> getDatabaseSecrets(String databaseName, String databaseId);
	
	Map<String, String> getInsightSecrets(String insightId, String projectName, String projectId);

	Map<String, String> getInsightEncryption(String insightId, String projectName, String projectId);

	boolean writeDatabaseSecrets(String databaseName, String databaseId, String key, Object value);

	boolean writeDatabaseSecrets(String databaseName, String databaseId, Map<String, Object> nameValuePairs);

}
