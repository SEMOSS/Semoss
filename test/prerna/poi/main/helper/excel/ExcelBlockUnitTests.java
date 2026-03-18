package prerna.poi.main.helper.excel;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;

class ExcelBlockUnitTests {

    @Test
    void test_isEmptyAndNumIndices_workAsExpected() {
        ExcelBlock b = new ExcelBlock();
        assertTrue(b.isEmpty());
        assertEquals(0, b.numIndicesInBlock());

        b.addRowIndexContainingData(3);
        b.addRowIndexContainingData(7);

        assertFalse(b.isEmpty());
        assertEquals(2, b.numIndicesInBlock());
    }

    @Test
    void test_trySetLastColMaxIndex_tracksMaximum_indirectlyViaGetRanges() {
        ExcelBlock b = new ExcelBlock();

        b.addStartColumnIndex(0);
        b.addColumnToRowIndexWithData(0, 5, SemossDataType.INT, null);

        b.trySetLastColMaxIndex(0);
        b.trySetLastColMaxIndex(-5);
        b.trySetLastColMaxIndex(3);

        // With lastColMaxIndex=3 and only col 0 present, the range should close at col 1 (first missing col).
        List<ExcelRange> ranges = b.getRanges();
        assertEquals(1, ranges.size());
        assertArrayEquals(new int[] { 1, 5, 1, 5 }, ranges.get(0).getIndices());
    }

    @Test
    void test_merge_appendsRowIndices_andUpdatesLastColMaxIndex_indirectly() {
    	ExcelBlock a = new ExcelBlock();
        a.addRowIndexContainingData(1);
        a.addStartColumnIndex(0);
        a.addColumnToRowIndexWithData(0, 5, SemossDataType.INT, null);
        a.addColumnToRowIndexWithData(1, 5, SemossDataType.INT, null);
        a.trySetLastColMaxIndex(1);

        ExcelBlock b = new ExcelBlock();
        b.addRowIndexContainingData(10);
        b.addRowIndexContainingData(11);
        b.trySetLastColMaxIndex(5);

        // Before merge, lastColMaxIndex=1 means the loop never hits a missing column after col 1,
        // so the open segment won't be added.
        assertTrue(a.getRanges().isEmpty());

        a.merge(b);

        // Row indices appended
        assertEquals(3, a.numIndicesInBlock());

        // After merge, lastColMaxIndex should be 5, which allows closure at col 2 (missing) and adds the range.
        List<ExcelRange> rangesAfterMerge = a.getRanges();
        assertEquals(1, rangesAfterMerge.size());
        assertArrayEquals(new int[] { 1, 5, 2, 5 }, rangesAfterMerge.get(0).getIndices());
    }

    @Test
    void test_sameAs_trueWhenMeansWithinStdDev() {
        ExcelBlock a = new ExcelBlock();
        a.addStartColumnIndex(0);
        a.addStartColumnIndex(2);
        a.addTotalColumnsInRowStats(5);
        a.addTotalColumnsInRowStats(9);

        ExcelBlock b = new ExcelBlock();
        b.addStartColumnIndex(1);
        b.addStartColumnIndex(1);
        b.addTotalColumnsInRowStats(6);
        b.addTotalColumnsInRowStats(8);

        assertTrue(a.sameAs(b));
    }

    @Test
    void test_sameAs_falseWhenStdDevIsZeroAndDifferent() {
        ExcelBlock a = new ExcelBlock();
        a.addStartColumnIndex(0);
        a.addStartColumnIndex(0);
        a.addTotalColumnsInRowStats(5);
        a.addTotalColumnsInRowStats(5);

        ExcelBlock b = new ExcelBlock();
        b.addStartColumnIndex(1);
        b.addStartColumnIndex(1);
        b.addTotalColumnsInRowStats(6);
        b.addTotalColumnsInRowStats(6);

        assertFalse(a.sameAs(b));
    }

    @Test
    void test_getRanges_throwsWhenCalledWithoutRequiredStats() {
        ExcelBlock b = new ExcelBlock();
        assertThrows(NullPointerException.class, b::getRanges);
    }

    @Test
    void test_getRanges_splitsOnEmptyColumns_andIncludesTrailingRangeWhenLastColIsPastData() {
        ExcelBlock b = new ExcelBlock();

        // start at column 0 (0-based in the block; ExcelRange appears 1-based for columns)
        b.addStartColumnIndex(0);

        // Segment 1: columns 0-1, row 5
        b.addColumnToRowIndexWithData(0, 5, SemossDataType.INT, null);
        b.addColumnToRowIndexWithData(1, 5, SemossDataType.INT, null);

        // Segment 2: columns 3-4, row 7 (gap at col 2)
        b.addColumnToRowIndexWithData(3, 7, SemossDataType.INT, null);
        b.addColumnToRowIndexWithData(4, 7, SemossDataType.INT, null);

        // Set lastColMaxIndex past the last populated column so the final segment closes
        b.trySetLastColMaxIndex(5);

        List<ExcelRange> ranges = b.getRanges();
        assertEquals(2, ranges.size());

        assertArrayEquals(new int[] { 1, 5, 2, 5 }, ranges.get(0).getIndices());
        assertArrayEquals(new int[] { 4, 7, 5, 7 }, ranges.get(1).getIndices());
    }

    @Test
    void test_getRangeTypes_throwsWhenTypeStatsMissingForAColumn() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10); // single column

        // No addColumnToRowIndexWithData => columnToTypeStats.get(0) returns null => NPE
        assertThrows(NullPointerException.class, () -> b.getRangeTypes(range));
    }

    @Test
    void test_getRangeTypes_stringWinsImmediatelyWhenPresent() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10);

        b.addColumnToRowIndexWithData(0, 10, SemossDataType.STRING, null);
        b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);
        b.addColumnToRowIndexWithData(0, 10, SemossDataType.DOUBLE, null);

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.STRING, predicted[0][0]);
    }

    @Test
    void test_getRangeTypes_numericPrefersIntWhenOnlyIntPresent() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10);

        b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.INT, predicted[0][0]);
    }

    @Test
    void test_getRangeTypes_numericPrefersDoubleWhenIntAndDoublePresent() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10);

        b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);
        b.addColumnToRowIndexWithData(0, 10, SemossDataType.DOUBLE, null);

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.DOUBLE, predicted[0][0]);
    }

    @Test
    void test_getRangeTypes_dateReturnsFormatWhenOnlyDatePresent() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10);

        b.addColumnToRowIndexWithData(0, 10, SemossDataType.DATE, "yyyy-MM-dd");

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.DATE, predicted[0][0]);
        assertEquals("yyyy-MM-dd", predicted[0][1]);
    }

    @Test
    void test_getRangeTypes_timestampWinsWhenDateAndTimestampPresent() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10);

        b.addColumnToRowIndexWithData(0, 10, SemossDataType.DATE, "yyyy-MM-dd");
        b.addColumnToRowIndexWithData(0, 10, SemossDataType.TIMESTAMP, "yyyy-MM-dd HH:mm:ss");

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.TIMESTAMP, predicted[0][0]);
        assertNotNull(predicted[0][1]); // some format string chosen
    }

    @Test
    void test_getRangeTypes_mixedNumericAndDateFallsBackToString() {
        ExcelBlock b = new ExcelBlock();
        ExcelRange range = new ExcelRange(1, 1, 1, 10);

        b.addColumnToRowIndexWithData(0, 10, SemossDataType.INT, null);
        b.addColumnToRowIndexWithData(0, 10, SemossDataType.DATE, "yyyy-MM-dd");

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.STRING, predicted[0][0]);
    }

    @Test
    void test_getRangeTypes_typeContained_logicStartRowGreaterThanMinMarksContained() {
        ExcelBlock b = new ExcelBlock();

        // Range rows 5..6
        ExcelRange range = new ExcelRange(1, 5, 1, 6);

        // Only observed INT at row 1; due to testTypeContainedWtihinRange logic,
        // startRow(5) > minRow(1) => treated as "contained".
        b.addColumnToRowIndexWithData(0, 1, SemossDataType.INT, null);

        Object[][] predicted = b.getRangeTypes(range);
        assertEquals(SemossDataType.INT, predicted[0][0]);
    }
}
