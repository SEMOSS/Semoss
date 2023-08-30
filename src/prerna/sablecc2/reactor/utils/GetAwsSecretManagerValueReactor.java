package prerna.sablecc2.reactor.utils;

import java.util.Map;

import prerna.om.AwsSecretsManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetAwsSecretManagerValueReactor extends AbstractReactor {

	public GetAwsSecretManagerValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), 
				"secretId", "versionId", "versionStage",
				ReactorKeysEnum.USE_APPLICATION_CERT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		Utility.checkIfValidDomain(url);
		String secretId = this.keyValue.get(this.keysToGet[1]);
		String versionId = this.keyValue.get(this.keysToGet[2]);
		String versionStage = this.keyValue.get(this.keysToGet[3]);
		boolean useApplicationCerts = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[4]) + "");

		AwsSecretsManager manager = new AwsSecretsManager();
		manager.setUrl(url);
		manager.setSecretId(secretId);
		manager.setVersionId(versionId);
		manager.setVersionStage(versionStage);
		manager.setUseApplicationCerts(useApplicationCerts);
		manager.makeRequest();
		Map<String, Object> jsonResponse = manager.getResponseJson();
		return new NounMetadata(jsonResponse, PixelDataType.MAP);
	}

}
