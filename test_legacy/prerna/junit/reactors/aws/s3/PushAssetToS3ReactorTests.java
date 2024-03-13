package prerna.junit.reactors.aws.s3;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.aws.s3.PushAssetToS3Reactor;

public class PushAssetToS3ReactorTests {
	
	@Test
	public void testGetDescriptionForKey() {
		PushAssetToS3Reactor r = new PushAssetToS3Reactor();
		String val = r.getDescriptionForKey("bucket");
		assertEquals("S3 bucket name", val);
	}

}
