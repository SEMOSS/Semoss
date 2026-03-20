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
package prerna.poi.main.helper.excel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ExcelRangeUnitTests {

	@Test
	void test_getRangeSyntax() {
		ExcelRange r = new ExcelRange(1, 5, 1, 10);
		assertEquals("A1:E10", r.getRangeSyntax());

		r = new ExcelRange(10, 27, 10, 27);
		assertEquals("J10:AA27", r.getRangeSyntax());

		r = new ExcelRange(7, 703, 8, 100);
		assertEquals("G8:AAA100", r.getRangeSyntax());

		r = new ExcelRange("A1:AA9");
		assertEquals("A1:AA9", r.getRangeSyntax());
	}

	@Test
	void test_getIndices() {
		ExcelRange r = new ExcelRange(1, 5, 1, 10);
		assertArrayEquals(new int[] { 1, 1, 5, 10 }, r.getIndices());

		r = new ExcelRange(10, 27, 10, 27);
		assertArrayEquals(new int[] { 10, 10, 27, 27 }, r.getIndices());

		r = new ExcelRange(7, 703, 8, 100);
		assertArrayEquals(new int[] { 7, 8, 703, 100 }, r.getIndices());

		r = new ExcelRange("A1:AA9");
		assertArrayEquals(new int[] { 1, 1, 27, 9 }, r.getIndices());
	}

	@ParameterizedTest
	@CsvSource({ "1,A", "26,Z", "27,AA", "52,AZ", "53,BA", "702,ZZ", "703,AAA" })
	void test_getCol(int colNum, String expected) {
		assertEquals(expected, ExcelRange.getCol(colNum));
	}

	/**
	 * Note: This test describes the typical/expected Excel behavior (A=1, Z=26,
	 * AA=27). As written, ExcelRange.getExcelColumnNumber currently returns 0-based
	 * values and is incorrect for multi-letter columns (e.g., "AA" returns 0).
	 */
	//@Disabled("Enable after fixing getExcelColumnNumber implementation")
	@ParameterizedTest
	@CsvSource({ "A,1", "B,2", "Z,26", "AA,27", "AZ,52", "BA,53", "ZZ,702", "AAA,703", "DHAVA,1969761" })
	void test_getExcelColumnNumber_expectedExcelSemantics(String col, int expected) {
		assertEquals(expected, ExcelRange.getExcelColumnNumber(col));
	}

	@Test
	void test_getSheetRangeIndex() {
		assertArrayEquals(new int[] { 1, 1, 140, 1459 }, ExcelRange.getSheetRangeIndex("A1:EJ1459"));

		assertArrayEquals(new int[] { 1, 1, 27, 9 }, ExcelRange.getSheetRangeIndex("A1:AA9"));

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> ExcelRange.getSheetRangeIndex("A1"));
		assertTrue(ex.getMessage().contains("Invalid range syntax"));
	}

	@Test
	void test_getStartRow() {
		ExcelRange r = new ExcelRange(1, 5, 1, 10);
		assertEquals(1, r.getStartRow());
	}
}
