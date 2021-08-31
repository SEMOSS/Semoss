package prerna.sablecc2.reactor.qs.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;

import prerna.util.DIHelper;


public class S3Utils {
	
	private static final String AWS_ACCOUUNT = "aws_account";
	private static final String AWS_KEY = "aws_key";

	private static Properties socialData = null;
	private static S3Utils instance;
	private static AWSCredentialsProviderChain awsCredsChain = null;
	
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
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
		
		List<AWSCredentialsProvider> credProviders = new ArrayList<>();
		String account = socialData.getProperty(AWS_ACCOUUNT);
		String key = socialData.getProperty(AWS_KEY);
		if (account != null && account.length() > 0 && key != null && key.length() > 0) {
			credProviders.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(account, key)));
		}
		credProviders.add(new EnvironmentVariableCredentialsProvider());
		awsCredsChain = new AWSCredentialsProviderChain(credProviders);
	}
	
	public AWSCredentialsProviderChain getAwsCredsChain() {
		return awsCredsChain;
	}
	
}
