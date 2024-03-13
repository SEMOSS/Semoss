package prerna.junit.reactors.algorithm.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import prerna.algorithm.api.SemossDataType;

public class SemossDataTypeTest {

	@Test
	public void testIsNotString() {
		SemossDataType test = SemossDataType.BOOLEAN;
		assertTrue(SemossDataType.isNotString(test));
	}
	
	@Test
	public void testIsNotStringString() {
		SemossDataType test = SemossDataType.STRING;
		assertFalse(SemossDataType.isNotString(test));
	}
	
	@Test
	public void testIsNotStringFactor() {
		SemossDataType test = SemossDataType.FACTOR;
		assertFalse(SemossDataType.isNotString(test));
	}
	
}
