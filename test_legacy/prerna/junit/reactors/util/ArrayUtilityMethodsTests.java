package prerna.junit.reactors.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.util.ArrayUtilityMethods;

public class ArrayUtilityMethodsTests {
	
	@Test
	public void testFilterArray() {
		String[] arr = {"one", "two", "three"};
		Boolean[] include = {true, false, true};
		String[] res = ArrayUtilityMethods.filterArray(arr, include);
		assertEquals("one", res[0]);
		assertEquals("three", res[1]);
	}

}
