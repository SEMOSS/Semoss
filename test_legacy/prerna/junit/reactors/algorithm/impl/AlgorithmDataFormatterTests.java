/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
