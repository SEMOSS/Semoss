/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
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
		if (!Utility.isSecretsStoreEnabled()) {
			return null;
		}

		String storeType = Utility.getDIHelperProperty(Constants.SECRET_STORE_TYPE);
		if (storeType.equalsIgnoreCase(ISecrets.HASHICORP_VAULT)) {
			return HashiCorpVaultUtil.getInstance();
		} else if (storeType.equalsIgnoreCase(ISecrets.AZURE_KEYVAULT)) {
			return AzureKeyVaultUtil.getInstance();
		} else {
			classLogger.warn("Secret store is enabled but could not find type for input = '" + storeType + "'");
			return null;
		}
	}
}
