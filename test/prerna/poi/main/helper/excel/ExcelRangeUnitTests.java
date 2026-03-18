package prerna.poi.main.helper.excel;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ExcelRangeUnitTests {

    @Test
    void test_constructorWithInts_setsIndicesAndSyntax() {
        ExcelRange r = new ExcelRange(1, 5, 1, 10); // A..E, 1..10
        assertArrayEquals(new int[] { 1, 1, 5, 10 }, r.getIndices());
        assertEquals("A1:E10", r.getRangeSyntax());
        assertEquals(1, r.getStartRow());
    }

    @Test
    void test_constructorWithRangeSyntax_parsesIndicesAndSyntaxRoundTrips() {
        ExcelRange r = new ExcelRange("A1:EJ1459");
        assertArrayEquals(new int[] { 1, 1, 140, 1459 }, r.getIndices()); // EJ = 140
        assertEquals("A1:EJ1459", r.getRangeSyntax());
        assertEquals(1, r.getStartRow());
    }

    @Test
    void test_getSheetRangeIndex_parsesSingleAndMultiLetterColumns() {
        assertArrayEquals(new int[] { 1, 1, 27, 9 }, ExcelRange.getSheetRangeIndex("A1:AA9"));
        assertArrayEquals(new int[] { 26, 2, 28, 100 }, ExcelRange.getSheetRangeIndex("Z2:AB100"));
    }

    @Test
    void test_getSheetRangeIndex_rejectsInvalidSyntax() {
        assertThrows(IllegalArgumentException.class, () -> ExcelRange.getSheetRangeIndex("A1"));
        assertThrows(IllegalArgumentException.class, () -> ExcelRange.getSheetRangeIndex("A1:B2:C3"));
        assertThrows(IllegalArgumentException.class, () -> ExcelRange.getSheetRangeIndex(""));
    }

    @Test
    void test_getCol_boundariesAndLargeColumns() {
        assertEquals("A", ExcelRange.getCol(1));
        assertEquals("Z", ExcelRange.getCol(26));
        assertEquals("AA", ExcelRange.getCol(27));
        assertEquals("AZ", ExcelRange.getCol(52));
        assertEquals("BA", ExcelRange.getCol(53));
        assertEquals("ZZ", ExcelRange.getCol(702));
        assertEquals("AAA", ExcelRange.getCol(703));
        assertEquals("EJ", ExcelRange.getCol(140));
    }

    @Test
    void test_getExcelColumnNumber_matchesCurrentImplementation() {
        // Note: This method is not the inverse of getCol() as written.
        assertEquals(0, ExcelRange.getExcelColumnNumber("A"));
        assertEquals(1, ExcelRange.getExcelColumnNumber("B"));
        assertEquals(25, ExcelRange.getExcelColumnNumber("Z"));
        assertEquals(0, ExcelRange.getExcelColumnNumber("AA"));
        assertEquals(1, ExcelRange.getExcelColumnNumber("AB"));
        assertEquals(9, ExcelRange.getExcelColumnNumber("AJ"));
    }

    @Test
    void test_getIndices_returnsCopyNotBackedByState() {
        ExcelRange r = new ExcelRange(1, 2, 3, 4);
        int[] idx1 = r.getIndices();
        idx1[0] = 999;

        int[] idx2 = r.getIndices();
        assertArrayEquals(new int[] { 1, 3, 2, 4 }, idx2);
    }
}
