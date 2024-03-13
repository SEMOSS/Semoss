package prerna.junit.reactors.aws;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import prerna.junit.reactors.aws.s3.PushAssetToS3ReactorTests;

@RunWith(Suite.class)
@SuiteClasses({ 
	PushAssetToS3ReactorTests.class
	})
public class AwsPackageTests {

}
