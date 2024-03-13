package prerna.junit.reactors.rpa;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.rpa.RPAUtil;

public class RPAUtilTests {

	@Test
	public void testMinutesSinceStartTime() {
		long twoMinuteAgo = System.currentTimeMillis() - 120000;
		long res = RPAUtil.minutesSinceStartTime(twoMinuteAgo);
		assertEquals(2, res);
	}
	
}
