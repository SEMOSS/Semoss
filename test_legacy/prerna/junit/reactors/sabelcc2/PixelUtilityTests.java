package prerna.junit.reactors.sabelcc2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.sablecc2.PixelUtility;

public class PixelUtilityTests {
	
	@Test
	public void testRemoveSurroundingQuotes() {
		String literal = "\"test\"";
		String res = PixelUtility.removeSurroundingQuotes(literal);
		assertEquals("test", res);
	}

}
