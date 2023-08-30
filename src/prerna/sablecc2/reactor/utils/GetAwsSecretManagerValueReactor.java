package prerna.sablecc2.reactor.utils;

import java.util.Map;

import prerna.aws.AwsSecretsManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetAwsSecretManagerValueReactor extends AbstractReactor {

	public GetAwsSecretManagerValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), 
				"accessKey", "secretKey",
				"secretId", "versionId", "versionStage",
				ReactorKeysEnum.USE_APPLICATION_CERT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(ReactorKeysEnum.URL.getKey());
		Utility.checkIfValidDomain(url);
		String accessKey = this.keyValue.get("accessKey");
		String secretKey = this.keyValue.get("secretKey");
		
		String secretId = this.keyValue.get("secretId");
		String versionId = this.keyValue.get("versionId");
		String versionStage = this.keyValue.get("versionStage");
		boolean useApplicationCerts = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.USE_APPLICATION_CERT.getKey()) + "");

		AwsSecretsManager manager = new AwsSecretsManager();
		manager.setUrl(url);
		manager.setAccessKey(accessKey);
		manager.setSecretKey(secretKey);
		manager.setSecretId(secretId);
		manager.setVersionId(versionId);
		manager.setVersionStage(versionStage);
		manager.setUseApplicationCerts(useApplicationCerts);
		manager.makeRequest();
		Map<String, Object> jsonResponse = manager.getResponseJson();
		return new NounMetadata(jsonResponse, PixelDataType.MAP);
	}

}
