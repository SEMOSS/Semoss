package prerna.sablecc2.reactor.qs.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.model.S3Object;

import prerna.util.DIHelper;


public class S3Utils {
	
	private static final Logger LOGGER = LogManager.getLogger(S3Utils.class.getName());
	
	private static Properties socialData = null;
	private static S3Utils instance;
	private static final String AWS_ACCOUUNT = "aws_account";
	private static final String AWS_KEY = "aws_key";
	
	private static AWSCredentials awsCreds = null;
	
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
		
		S3Object fullObject = null;

		String account = socialData.getProperty(AWS_ACCOUUNT);
		String key = socialData.getProperty(AWS_KEY);
		if (account != null && account.length() > 0 && key != null && key.length() > 0) {
		 awsCreds = new BasicAWSCredentials(account, key);
		} else{ 
			 awsCreds = new EnvironmentVariableCredentialsProvider().getCredentials();
		}
	}

	public AWSCredentials getS3Creds(){
		return awsCreds;
	}

}
