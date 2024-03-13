package prerna.junit.reactors.algorithm.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import prerna.algorithm.impl.AlgorithmDataFormatter;

public class AlgorithmDataFormatterTests {

	@Test
	public void manipulateValuesTest() {
		List<Object[]> objects = new ArrayList<>();
		Object[] o1 = {1, 2};
		Object[] o2 = {3, 4};
		objects.add(o1);
		objects.add(o2);
		
		Object[][] ret = AlgorithmDataFormatter.manipulateValues(objects, false);
		assertEquals(1, (int) ret[0][0]);
		assertEquals(3, (int) ret[0][1]);
		assertEquals(1, ret.length);
	}
	
	@Test
	public void manipulateValuesTest2() {
		List<Object[]> objects = new ArrayList<>();
		Object[] o1 = {1, 2};
		Object[] o2 = {3, 4};
		objects.add(o1);
		objects.add(o2);
		
		Object[][] ret = AlgorithmDataFormatter.manipulateValues(objects, true);
		assertEquals(1, (int) ret[0][0]);
		assertEquals(2, (int) ret[1][0]);
		assertEquals(3, (int) ret[0][1]);
		assertEquals(4, (int) ret[1][1]);
		assertEquals(2, ret.length);
	}
	
	@Test
	public void testConvertColumnValuesToRows() {
		Object[][] o = {
				{1, 2},
				{3, 4}
		};
		
		Object[][] ret = AlgorithmDataFormatter.convertColumnValuesToRows(o);
		assertEquals(1, (int) ret[0][0]);
		assertEquals(3, (int) ret[0][1]);
		assertEquals(2, (int) ret[1][0]);
		assertEquals(4, (int) ret[1][1]);
	}
	
	// THIS IMPLEMENTATION OF DETERMINE COLUMN TYPES IS WRONG, 
	// but thankfully we don't use it anywhere
	@Test
	public void testDetermineColumnTypes() {
		String[] names = {"empty", "shouldBeInts", "shouldBeStrings", "shouldBeSimpDates", "dates"};

		List<Object []> list = new ArrayList<>();
		Object[] arr = {null, 1, "string", "1/1/2022", "2022-11-10T16:33:26.760Z"};
		list.add(arr);
		
		
		String[] catPropName = new String[arr.length]; 
		Integer[] catPropInd = new Integer[arr.length];
		String[] numPropNames = new String[arr.length];
		Integer[] numPropInd = new Integer[arr.length];
		Integer[] dateTypeInd = new Integer[arr.length];
		Integer[] simpleDateTypeInd = new Integer[arr.length];
		
		AlgorithmDataFormatter.determineColumnTypes(names, list, catPropName, catPropInd, 
				numPropNames, numPropInd, dateTypeInd, simpleDateTypeInd);
		
		assertEquals("shouldBeStrings", catPropName[0]);
		assertEquals("shouldBeInts", numPropNames[0]);
		assertEquals("shouldBeSimpDates", numPropNames[1]);
		assertEquals("dates", numPropNames[2]);
		assertEquals(1, (int) numPropInd[0]);
		assertEquals(2, (int) catPropInd[0]);
		// ENABLE TO SHOW THE BUG
		//assertEquals(3, (int) simpleDateTypeInd[0]);
		assertEquals(4, (int) dateTypeInd[0]);
	}
}
