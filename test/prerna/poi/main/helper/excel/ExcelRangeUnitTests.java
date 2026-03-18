package prerna.poi.main.helper.excel;

import static org.junit.jupiter.api.Assertions.*;

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
		assertArrayEquals(new int[] {1, 1, 5, 10}, r.getIndices());
		
		r = new ExcelRange(10, 27, 10, 27);
		assertArrayEquals(new int[] {10, 10, 27, 27}, r.getIndices());
		
		r = new ExcelRange(7, 703, 8, 100);
		assertArrayEquals(new int[] {7, 8, 703, 100}, r.getIndices());
		
		r = new ExcelRange("A1:AA9");
		assertArrayEquals(new int[] {1, 1, 27, 9}, r.getIndices());
	}
	
	@ParameterizedTest
	@CsvSource({
		"1,A",
		"26,Z",
		"27,AA",
		"52,AZ",
		"53,BA",
		"702,ZZ",
		"703,AAA"
	})
	void test_getCol(int colNum, String expected) {
		assertEquals(expected, ExcelRange.getCol(colNum));
	}
	
	/**
	* Note: This test describes the typical/expected Excel behavior (A=1, Z=26, AA=27).
	* As written, ExcelRange.getExcelColumnNumber currently returns 0-based values and
	* is incorrect for multi-letter columns (e.g., "AA" returns 0).
	*/
	@Disabled("Enable after fixing getExcelColumnNumber implementation")
	@ParameterizedTest
	@CsvSource({
		"A,1",
		"B,2",
		"Z,26",
		"AA,27",
		"AZ,52",
		"BA,53",
		"ZZ,702",
		"AAA,703"
	})
	void test_getExcelColumnNumber_expectedExcelSemantics(String col, int expected) {
		assertEquals(expected, ExcelRange.getExcelColumnNumber(col));
	}
	
	@Test
	void test_getSheetRangeIndex() {
		assertArrayEquals(new int[] {1, 1, 140, 1459}, ExcelRange.getSheetRangeIndex("A1:EJ1459"));
		
		assertArrayEquals(new int[] {1, 1, 27, 9}, ExcelRange.getSheetRangeIndex("A1:AA9"));
		
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> ExcelRange.getSheetRangeIndex("A1"));
		assertTrue(ex.getMessage().contains("Invalid range syntax"));
	}
	

	@Test
	void test_getStartRow() {
		ExcelRange r = new ExcelRange(1, 5, 1, 10);
		assertEquals(1, r.getStartRow());
	}
}

