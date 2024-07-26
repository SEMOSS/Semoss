package prerna.io.connector.secrets.azure.keyvault;

import java.util.Map;

import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.io.connector.secrets.ISecrets;

public class AzureKeyVaultUtil implements ISecrets {

	@Override
	public Map<String, String> getEngineSecrets(CATALOG_TYPE arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getInsightSecrets(String projectId, String projectName, String insightId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean writeEngineSecret(CATALOG_TYPE arg0, String arg1, String arg2, String arg3, Object arg4) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeEngineSecrets(CATALOG_TYPE arg0, String arg1, String arg2, Map<String, Object> arg3) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeInsightSecret(String projectId, String projectName, String insightId, String key,
			Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeInsightSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		// TODO Auto-generated method stub
		return false;
	}

}
