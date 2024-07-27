package prerna.io.connector.secrets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.secrets.azure.keyvault.AzureKeyVaultUtil;
import prerna.io.connector.secrets.hashicorp.vault.HashiCorpVaultUtil;
import prerna.util.Constants;
import prerna.util.Utility;

public final class SecretsFactory {

	private static final Logger classLogger = LogManager.getLogger(SecretsFactory.class);
	
	private SecretsFactory() {
		
	}
	
	public static ISecrets getSecretConnector() {
		if(!Utility.isSecretsStoreEnabled()) {
			return null;
		}
		
		String storeType = Utility.getDIHelperProperty(Constants.SECRET_STORE_TYPE);
		if(storeType.equalsIgnoreCase(ISecrets.HASHICORP_VAULT)) {
			return HashiCorpVaultUtil.getInstance();
		} else if(storeType.equalsIgnoreCase(ISecrets.AZURE_KEYVAULT)) {
			return AzureKeyVaultUtil.getInstance();
		}
		else {
			classLogger.warn("Secret store is enabled but could not find type for input = '" + storeType + "'");
			return null;
		}
	}
	
}
