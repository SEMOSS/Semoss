package prerna.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

import prerna.aws.s3.S3Utils;
import prerna.util.DIHelper;


public class S3Utils {
	
	public static final String ENDPOINT = "endpoint";
	public static final String REGION = "region";
	public static final String CONFIG_PATH = "configPath";
	public static final String PROFILE = "profile";
	public static final String ACCESS_KEY = "accessKey";
	public static final String SECRET = "secret";
	
	private static final String AWS_ACCOUUNT = "aws_account";
	private static final String AWS_KEY = "aws_key";
	private static final String AWS_ENDPOINT = "aws_endpoint";
	
	private static Properties socialData = null;
	private static S3Utils instance;
	private static AWSCredentialsProviderChain awsCredsChain = null;
	private static String awsEndpoint = null;
	
	private S3Utils() {
		loadProps();
	}
	
	public static S3Utils getInstance() throws IllegalArgumentException {
		if(instance == null) {
				instance = new S3Utils();
			}
		
		return instance;
	}
	
	private void loadProps() {
		File f = new File(DIHelper.getInstance().getProperty("SOCIAL"));
		FileInputStream fis = null;

		try {
			if(f.exists()) {
				socialData = new Properties();
				fis = new FileInputStream(f);
				socialData.load(fis);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		// creds
		List<AWSCredentialsProvider> credProviders = new ArrayList<>();
		String account = socialData.getProperty(AWS_ACCOUUNT);
		String key = socialData.getProperty(AWS_KEY);
		if (account != null && account.length() > 0 && key != null && key.length() > 0) {
			credProviders.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(account, key)));
		}
		credProviders.add(new EnvironmentVariableCredentialsProvider());
		awsCredsChain = new AWSCredentialsProviderChain(credProviders);
		
		// endpoint
		String socialEndpoint = socialData.getProperty(AWS_ENDPOINT);
		if (socialEndpoint != null && socialEndpoint.length() > 0) {
			awsEndpoint = socialEndpoint;
		} else if (System.getenv().containsKey(AWS_ENDPOINT)) {
			awsEndpoint = System.getenv().get(AWS_ENDPOINT);
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
			endpoint = awsEndpoint;
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
		} else {
			credProviders.add(new ProfileCredentialsProvider(profileName));
		}
		
		// fall back on social.properties and/or env vars
		AWSCredentialsProviderChain socialCredsChain = awsCredsChain;
		if(socialCredsChain != null) {
			credProviders.add(socialCredsChain);
		} else {
			credProviders.add(new EnvironmentVariableCredentialsProvider());
		}
		
		clientBuilder.setCredentials(new AWSCredentialsProviderChain(credProviders));
		
		return clientBuilder.build();
	}
	
	public AWSCredentialsProviderChain getAwsCredsChain() {
		return awsCredsChain;
	}
	
	public String getAwsEndpoint() {
		return awsEndpoint;
	}
	
	public static String[] addCommonS3Keys(String[] additionalKeys) {
		String[] base = new String[] { ENDPOINT, REGION, CONFIG_PATH, PROFILE, ACCESS_KEY, SECRET };
		
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
		}
		return null;
	}
}
