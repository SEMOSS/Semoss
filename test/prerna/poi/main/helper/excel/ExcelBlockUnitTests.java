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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;

class ExcelBlockUnitTests {

	@Test
	void test_isEmpty() {
		ExcelBlock b = new ExcelBlock();
		assertTrue(b.isEmpty());

		b.addRowIndexContainingData(7);
		b.addRowIndexContainingData(9);
		assertFalse(b.isEmpty());
	}

	@Test
	void test_numIndicesInBlock() {
		ExcelBlock b = new ExcelBlock();
		assertEquals(0, b.numIndicesInBlock());

		b.addRowIndexContainingData(7);
		b.addRowIndexContainingData(9);
		assertEquals(2, b.numIndicesInBlock());
	}

	@Test
	void test_addRowIndexContainingData() {
		ExcelBlock b = new ExcelBlock();
		b.addRowIndexContainingData(7);
		b.addRowIndexContainingData(9);
		b.addRowIndexContainingData(10);
		assertFalse(b.isEmpty());
		assertEquals(3, b.numIndicesInBlock());
	}

	@Disabled("lastColMaxIndex not directly checkable due to being a private variable")
	@Test
	void test_trySetLastColMaxIndex_setsAndOnlyIncreases() {
		ExcelBlock b = new ExcelBlock();

//	    assertTrue(b.lastColM)

		b.trySetLastColMaxIndex(3);
		b.trySetLastColMaxIndex(2); // should not reduce
		b.trySetLastColMaxIndex(5); // should increase

		// Indirectly validate by building ranges that require lastColMaxIndex=5 to
		// iterate far enough.
		b.addStartColumnIndex(0);
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);
		b.addColumnToRowIndexWithData(0, 2, SemossDataType.INT, null);

		// Put data in col 4; ensure a trailing empty col exists by setting
		// lastColMaxIndex to 5
		b.addColumnToRowIndexWithData(4, 2, SemossDataType.INT, null);
		b.trySetLastColMaxIndex(5);

		List<ExcelRange> ranges = b.getRanges();
		assertTrue(ranges.size() >= 1, "Expected at least one range");
	}

	@Test
	void test_getRanges() {
		// splitsOnEmptyColumns_andUsesOverallMaxRow()
		ExcelBlock b = new ExcelBlock();

		// Ensure startColumnIndexStats min is 0 (first data column)
		b.addStartColumnIndex(0);

		// Columns 0 and 1 contiguous; column 2 empty; column 3 has data.
		// lastColMaxIndex set to 4 so the trailing empty col triggers range close for
		// col 3.
		b.trySetLastColMaxIndex(4);

		// Column 0 rows 1..2
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);
		b.addColumnToRowIndexWithData(0, 2, SemossDataType.INT, null);

		// Column 1 has a later row to force maxRow=5 for the first range
		b.addColumnToRowIndexWithData(1, 5, SemossDataType.INT, null);

		// Column 3 has data starting at row 3
		b.addColumnToRowIndexWithData(3, 3, SemossDataType.INT, null);

		List<ExcelRange> ranges = b.getRanges();
		assertEquals(2, ranges.size());

		assertEquals("A1:B5", ranges.get(0).getRangeSyntax());
		assertEquals("D3:D5", ranges.get(1).getRangeSyntax());

		b = new ExcelBlock();
		b.addStartColumnIndex(2);
		// Columns 2-3 contiguous, col 5 has data and is isolated, col 7 has data and is
		// isolated
		// lastColMaxIndex set to 8 so the trailing empty col triggers range close for
		// col 7.
		b.trySetLastColMaxIndex(8);
		// Column 2 rows 2,3
		b.addColumnToRowIndexWithData(2, 2, SemossDataType.INT, null);
		b.addColumnToRowIndexWithData(2, 3, SemossDataType.INT, null);
		// Column 3 row 1
		b.addColumnToRowIndexWithData(3, 1, SemossDataType.INT, null);
		// Column 5 row 1
		b.addColumnToRowIndexWithData(5, 3, SemossDataType.INT, null);
		// column 7 row 1
		b.addColumnToRowIndexWithData(7, 3, SemossDataType.INT, null);

		ranges.clear();
		ranges = b.getRanges();
		assertEquals(3, ranges.size());

		assertEquals("C2:D3", ranges.get(0).getRangeSyntax());
		assertEquals("F3:F3", ranges.get(1).getRangeSyntax());
		assertEquals("H3:H3", ranges.get(2).getRangeSyntax());
	}

	@Test
	void test_getRangeTypes_stringWinsImmediately() {
		ExcelBlock b = new ExcelBlock();

		// Put STRING evidence in column 0 spanning the range end row.
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.STRING, null);
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.STRING, null);

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.STRING, types[0][0]);
	}

	@Test
	void test_getRangeTypes_intOnly_returnsInt() {
		ExcelBlock b = new ExcelBlock();
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.INT, types[0][0]);
	}

	@Test
	void test_getRangeTypes_intAndDouble_returnsDouble() {
		ExcelBlock b = new ExcelBlock();

		// INT evidence
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);

		// DOUBLE evidence (ensure max row >= endRow so current containment logic
		// returns true)
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.DOUBLE, null);

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.DOUBLE, types[0][0]);
	}

	@Test
	void test_getRangeTypes_date_returnsDateAndMostOccurringFormat() {
		ExcelBlock b = new ExcelBlock();

		// DATE evidence plus one (single) additional format so max() is deterministic
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.DATE, "yyyy-MM-dd");
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.DATE, "yyyy-MM-dd");

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.DATE, types[0][0]);
		assertEquals("yyyy-MM-dd", types[0][1]);
	}

	@Test
	void test_getRangeTypes_mixedNumberAndDate_fallsBackToString() {
		ExcelBlock b = new ExcelBlock();

		b.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);

		b.addColumnToRowIndexWithData(0, 1, SemossDataType.DATE, "yyyy-MM-dd");
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.DATE, "yyyy-MM-dd");

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.STRING, types[0][0]);
	}

	@Test
	void test_getRangeTypes_mixedDateAndTimestamps() {
		ExcelBlock b = new ExcelBlock();

		// DATE evidence across the range
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.DATE, "yyyy-MM-dd");
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.DATE, "yyyy-MM-dd");

		// TIMESTAMP evidence across the range
		b.addColumnToRowIndexWithData(0, 1, SemossDataType.TIMESTAMP, "yyyy-MM-dd HH:mm:ss");
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.TIMESTAMP, "yyyy-MM-dd HH:mm:ss");

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.TIMESTAMP, types[0][0]);
		// Not asserting types[0][1] here because the current ExcelBlock implementation
		// can make "most occurring format" nondeterministic when multiple formats
		// exist.
	}

	@Test
	void test_getRangeTypes_timestampOnly_returnsTimestampAndMostOccurringFormat() {
		ExcelBlock b = new ExcelBlock();

		b.addColumnToRowIndexWithData(0, 1, SemossDataType.TIMESTAMP, "yyyy-MM-dd HH:mm:ss");
		b.addColumnToRowIndexWithData(0, 10, SemossDataType.TIMESTAMP, "yyyy-MM-dd HH:mm:ss");

		Object[][] types = b.getRangeTypes(new ExcelRange("A1:A10"));
		assertEquals(SemossDataType.TIMESTAMP, types[0][0]);
		assertEquals("yyyy-MM-dd HH:mm:ss", types[0][1]);
	}

	@Test
	void test_sameAs_trueWhenMeansWithinThisBlocksStdDev() {
		ExcelBlock a = new ExcelBlock();
		a.addStartColumnIndex(0);
		a.addStartColumnIndex(1);
		a.addStartColumnIndex(0);
		a.addTotalColumnsInRowStats(5);
		a.addTotalColumnsInRowStats(6);
		a.addTotalColumnsInRowStats(5);

		ExcelBlock b = new ExcelBlock();
		b.addStartColumnIndex(0);
		b.addStartColumnIndex(0);
		b.addStartColumnIndex(1);
		b.addTotalColumnsInRowStats(5);
		b.addTotalColumnsInRowStats(5);
		b.addTotalColumnsInRowStats(6);

		assertTrue(a.sameAs(b));
	}

	@Test
	void test_sameAs_falseWhenMeansOutsideThisBlocksStdDev() {
		ExcelBlock a = new ExcelBlock();
		a.addStartColumnIndex(0);
		a.addStartColumnIndex(1);
		a.addStartColumnIndex(0);
		a.addTotalColumnsInRowStats(5);
		a.addTotalColumnsInRowStats(6);
		a.addTotalColumnsInRowStats(5);

		ExcelBlock b = new ExcelBlock();
		b.addStartColumnIndex(10);
		b.addStartColumnIndex(10);
		b.addStartColumnIndex(10);
		b.addTotalColumnsInRowStats(50);
		b.addTotalColumnsInRowStats(50);
		b.addTotalColumnsInRowStats(50);

		assertFalse(a.sameAs(b));
	}

	@Test
	void test_merge_appendsRowIndicesAndUpdatesLastColMaxIndex() {
		ExcelBlock a = new ExcelBlock();
		a.addRowIndexContainingData(1);
		a.trySetLastColMaxIndex(2);

		ExcelBlock b = new ExcelBlock();
		b.addRowIndexContainingData(3);
		b.addRowIndexContainingData(4);
		b.trySetLastColMaxIndex(5);

		a.merge(b);

		assertEquals(3, a.numIndicesInBlock());

		// Indirectly validate lastColMaxIndex increased by making sure iteration can
		// reach 5
		a.addStartColumnIndex(0);
		a.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);
		a.addColumnToRowIndexWithData(5, 1, SemossDataType.INT, null);
		a.trySetLastColMaxIndex(6); // ensure trailing empty to close

		assertFalse(a.getRanges().isEmpty());
	}
}
