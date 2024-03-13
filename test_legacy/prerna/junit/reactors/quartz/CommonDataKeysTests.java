package prerna.junit.reactors.quartz;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.quartz.CommonDataKeys;

public class CommonDataKeysTests {

	@Test
	public void testConstants() {
		assertEquals("insightID", CommonDataKeys.INSIGHT_ID);
		assertEquals("dataFrame", CommonDataKeys.DATA_FRAME);
		assertEquals("ifTrueJob", CommonDataKeys.IF_TRUE_JOB);
		assertEquals("engineName", CommonDataKeys.ENGINE_NAME);
		assertEquals("engine", CommonDataKeys.ENGINE);
	}
}
