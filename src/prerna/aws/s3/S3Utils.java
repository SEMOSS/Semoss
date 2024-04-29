package prerna.aws.s3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import prerna.util.Utility;


public class S3Utils {

	// shared reactor keys
	public static final String BUCKET = "bucket";
	public static final String ENDPOINT = "endpoint";
	public static final String REGION = "region";
	public static final String CONFIG_PATH = "configPath";
	public static final String PROFILE = "profile";
	public static final String ACCESS_KEY = "accessKey";
	public static final String SECRET = "secret";
	public static final String NO_DEFAULT_CREDS = "noDefaultCreds";

	// property keys
	private static final String AWS_SHARED_ACCOUNT_KEY = "aws_shared_account";
	private static final String AWS_SHARED_ACCESS_KEY = "aws_shared_key";
	private static final String AWS_SHARED_ENDPOINT_KEY = "aws_shared_endpoint";

	// loaded properties
	private static S3Utils INSTANCE;
	private static AWSCredentialsProviderChain AWS_CREDS_CHAIN = null;
	private static String AWS_ENDPOINT = null;

	private S3Utils() {
		loadProps();
	}

	public static S3Utils getInstance() throws IllegalArgumentException {
		if(INSTANCE == null) {
			INSTANCE = new S3Utils();
		}
		return INSTANCE;
	}

	private void loadProps() {
		Map<String, String> env = System.getenv();

		// credentials
		List<AWSCredentialsProvider> credProviders = new ArrayList<>();
		String account = null;
		if (env.containsKey(AWS_SHARED_ACCOUNT_KEY)) {
			account = env.get(AWS_SHARED_ACCOUNT_KEY);
		} else if (Utility.getDIHelperProperty(AWS_SHARED_ACCOUNT_KEY) != null
				&& !(Utility.getDIHelperProperty(AWS_SHARED_ACCOUNT_KEY).isEmpty())) {
			account = Utility.getDIHelperProperty(AWS_SHARED_ACCOUNT_KEY);
		}
		String key = null;
		if (env.containsKey(AWS_SHARED_ACCESS_KEY)) {
			key = env.get(AWS_SHARED_ACCESS_KEY);
		} else if (Utility.getDIHelperProperty(AWS_SHARED_ACCESS_KEY) != null
				&& !(Utility.getDIHelperProperty(AWS_SHARED_ACCESS_KEY).isEmpty())) {
			key = Utility.getDIHelperProperty(AWS_SHARED_ACCESS_KEY);
		}
		if (account != null && account.length() > 0 && key != null && key.length() > 0) {
			credProviders.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(account, key)));
		}
		credProviders.add(DefaultAWSCredentialsProviderChain.getInstance());
		AWS_CREDS_CHAIN = new AWSCredentialsProviderChain(credProviders);

		// endpoint
		if (env.containsKey(AWS_SHARED_ENDPOINT_KEY)) {
			AWS_ENDPOINT = env.get(AWS_SHARED_ENDPOINT_KEY);
		} else if (Utility.getDIHelperProperty(AWS_SHARED_ENDPOINT_KEY) != null
				&& !(Utility.getDIHelperProperty(AWS_SHARED_ENDPOINT_KEY).isEmpty())) {
			AWS_ENDPOINT = Utility.getDIHelperProperty(AWS_SHARED_ENDPOINT_KEY);
		}
	}

	public AmazonS3 getS3Client(Map<String, String> keyValue) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();

		// endpoint & region
		String region = keyValue.get(REGION);
		if(region == null || region.isEmpty()) {
			region = Regions.DEFAULT_REGION.getName();
		}

		String endpoint = keyValue.get(ENDPOINT);
		if(endpoint == null || endpoint.isEmpty()) {
			endpoint = AWS_ENDPOINT;
		}
		if(endpoint != null && !endpoint.isEmpty()) {
			EndpointConfiguration endpointConfig = new EndpointConfiguration(endpoint, region);
			clientBuilder.setEndpointConfiguration(endpointConfig);
		} else {
			clientBuilder.setRegion(region);
		}

		// credentials (first provided key & secret, then by profile)
		List<AWSCredentialsProvider> credProviders = new ArrayList<>();
		String accessKey = keyValue.get(ACCESS_KEY);
		String secret = keyValue.get(SECRET);
		if(accessKey != null && !accessKey.isEmpty() && secret != null && !secret.isEmpty()) {
			credProviders.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret)));
		}

		String configPath = keyValue.get(CONFIG_PATH);
		String profileName = keyValue.get(PROFILE);
		if(configPath != null && !configPath.isEmpty()) {
			credProviders.add(new ProfileCredentialsProvider(configPath, profileName));
		} else if (profileName != null && !profileName.isEmpty()) {
			credProviders.add(new ProfileCredentialsProvider(profileName));
		}

		// fall back on core props and/or env vars unless not desired
		Object noDefaultCreds = keyValue.get(NO_DEFAULT_CREDS);
		if(noDefaultCreds == null || !Boolean.parseBoolean(noDefaultCreds.toString())) {
			AWSCredentialsProviderChain sharedCredsChain = AWS_CREDS_CHAIN;
			if(sharedCredsChain != null) {
				credProviders.add(sharedCredsChain);
			} else {
				credProviders.add(DefaultAWSCredentialsProviderChain.getInstance());
			}
		}

		if(credProviders.isEmpty()) {
			throw new IllegalArgumentException("No AWS credentials configured");
		}

		clientBuilder.setCredentials(new AWSCredentialsProviderChain(credProviders));

		return clientBuilder.build();
	}

	public static String[] addCommonS3Keys(String[] additionalKeys) {
		String[] base = new String[] { ENDPOINT, REGION, CONFIG_PATH, PROFILE, ACCESS_KEY, SECRET, NO_DEFAULT_CREDS };

		if(additionalKeys == null || additionalKeys.length == 0) {
			return base;
		}

		String[] both = Arrays.copyOf(additionalKeys, additionalKeys.length + base.length);
		System.arraycopy(base, 0, both, additionalKeys.length, base.length);
		return both;
	}

	public static String getDescriptionForCommonS3Key(String key) {
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
		} else if(key.equals(NO_DEFAULT_CREDS)) {
			return "Flag to disallow usage of the global default AWS credentials. Defaults to false";
		}
		return null;
	}
}
