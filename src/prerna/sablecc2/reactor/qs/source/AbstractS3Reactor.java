package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import prerna.sablecc2.reactor.AbstractReactor;

/**
 * Common reactor base for S3 operations allowing customization of credential and endpoint configuration
 */
public abstract class AbstractS3Reactor extends AbstractReactor {
	
	protected static final String ENDPOINT = "endpoint";
	protected static final String REGION = "region";
	protected static final String CONFIG_PATH = "configPath";
	protected static final String PROFILE = "profile";
	protected static final String ACCESS_KEY = "accessKey";
	protected static final String SECRET = "secret";
	
	protected String[] getS3KeysToGet(String[] additionalKeys) {
		String[] base = new String[] { ENDPOINT, REGION, CONFIG_PATH, PROFILE, ACCESS_KEY, SECRET };
		
		if(additionalKeys == null || additionalKeys.length == 0) {
			return base;
		}
		
		String[] both = Arrays.copyOf(additionalKeys, additionalKeys.length + base.length);
		System.arraycopy(base, 0, both, additionalKeys.length, base.length);
		return both;
	}
	
	@Override
	public String getDescriptionForKey(String key) {
		if(key.equals(ENDPOINT)) {
			return "Base service endpoint";
		} else if(key.equals(REGION)) {
			return "Region name for request signing and service endpoint modification. Defaults to " + Regions.DEFAULT_REGION.getName();
		} else if(key.equals(CONFIG_PATH)) {
			return "Path of credentials configuration file to use if not the default ~/.aws/credentials";
		} else if(key.equals(PROFILE)) {
			return "Profile name the credentials are under if not the default";
		} else if(key.equals(ACCESS_KEY)) {
			return "Account access key";
		} else if(key.equals(SECRET)) {
			return "Account secret key";
		}
		return super.getDescriptionForKey(key);
	}
	
	protected AmazonS3 getS3Client() {
		AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard().enablePathStyleAccess();
		
		// endpoint & region
		String region = this.keyValue.get(REGION);
		if(region == null || region.isEmpty()) {
			region = Regions.DEFAULT_REGION.getName();
		}
		
		String endpoint = this.keyValue.get(ENDPOINT);
		if(endpoint != null && !endpoint.isEmpty()) {
			EndpointConfiguration endpointConfig = new EndpointConfiguration(endpoint, region);
			clientBuilder.setEndpointConfiguration(endpointConfig);
		} else {
			clientBuilder.setRegion(region);
		}
		
		// credentials (first provided key & secret, then by profile)
		List<AWSCredentialsProvider> credProviders = new ArrayList<>();
		String accessKey = this.keyValue.get(ACCESS_KEY);
		String secret = this.keyValue.get(SECRET);
		if(accessKey != null && !accessKey.isEmpty() && secret != null && !secret.isEmpty()) {
			credProviders.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret)));
		}
		String configPath = this.keyValue.get(CONFIG_PATH);
		String profileName = this.keyValue.get(PROFILE);
		if(configPath != null && !configPath.isEmpty()) {
			credProviders.add(new ProfileCredentialsProvider(configPath, profileName));
		} else {
			credProviders.add(new ProfileCredentialsProvider(profileName));
		}
		
		// fall back on social.properties and/or env vars
		AWSCredentialsProviderChain socialCredsChain = S3Utils.getInstance().getAwsCredsChain();
		if(socialCredsChain != null) {
			credProviders.add(socialCredsChain);
		} else {
			credProviders.add(new EnvironmentVariableCredentialsProvider());
		}
		
		clientBuilder.setCredentials(new AWSCredentialsProviderChain(credProviders));
		
		return clientBuilder.build();
	}
}
