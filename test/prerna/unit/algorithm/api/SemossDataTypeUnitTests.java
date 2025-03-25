package prerna.unit.algorithm.api;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.om.PixelDataType;

public class SemossDataTypeUnitTests {
	
	@Test
	void testIsNotStringType() {
		assertTrue(SemossDataType.isNotString(SemossDataType.BOOLEAN));
		assertFalse(SemossDataType.isNotString(SemossDataType.STRING));
		assertFalse(SemossDataType.isNotString(SemossDataType.FACTOR));
	}
	
	@Test
	void testIsNotStringStr() {
		assertTrue(SemossDataType.isNotString("BOOLEAN"));
		assertFalse(SemossDataType.isNotString("STRING"));
		assertFalse(SemossDataType.isNotString("FACTOR"));
	}
	
	@Test
	void testConvertStringToDataType() {
		assertNull(SemossDataType.convertStringToDataType(null));
		assertEquals(SemossDataType.BOOLEAN, SemossDataType.convertStringToDataType("BOOLEAN"));
		assertEquals(SemossDataType.BOOLEAN, SemossDataType.convertStringToDataType("TYPE:BOOLEAN"));
		assertEquals(SemossDataType.INT, SemossDataType.convertStringToDataType("INT"));
		assertEquals(SemossDataType.DOUBLE, SemossDataType.convertStringToDataType("DOUBLE"));
		assertEquals(SemossDataType.DATE, SemossDataType.convertStringToDataType("DATE"));
		assertEquals(SemossDataType.TIMESTAMP, SemossDataType.convertStringToDataType("TIMESTAMP"));
		assertEquals(SemossDataType.FACTOR, SemossDataType.convertStringToDataType("FACTOR"));
		assertEquals(SemossDataType.STRING, SemossDataType.convertStringToDataType("STRING"));
		assertEquals(SemossDataType.STRING, SemossDataType.convertStringToDataType("ELSE"));
	}
	
	@Test
	void testConvertFromSemossDataType() {
		assertEquals(SemossDataType.BOOLEAN, SemossDataType.convertFromSemossDataType(PixelDataType.BOOLEAN));
		assertEquals(SemossDataType.INT, SemossDataType.convertFromSemossDataType(PixelDataType.CONST_INT));
		assertEquals(SemossDataType.DOUBLE, SemossDataType.convertFromSemossDataType(PixelDataType.CONST_DECIMAL));
		assertEquals(SemossDataType.STRING, SemossDataType.convertFromSemossDataType(PixelDataType.CONST_STRING));
		assertEquals(SemossDataType.DATE, SemossDataType.convertFromSemossDataType(PixelDataType.CONST_DATE));
		assertEquals(SemossDataType.TIMESTAMP, SemossDataType.convertFromSemossDataType(PixelDataType.CONST_TIMESTAMP));
		assertNull(SemossDataType.convertFromSemossDataType(PixelDataType.ALIAS));
	}
	
	@Test
	void testConvertToPixelDataType() {
		assertEquals(PixelDataType.BOOLEAN, SemossDataType.convertToPixelDataType(SemossDataType.BOOLEAN));
		assertEquals(PixelDataType.CONST_INT, SemossDataType.convertToPixelDataType(SemossDataType.INT));
		assertEquals(PixelDataType.CONST_DECIMAL, SemossDataType.convertToPixelDataType(SemossDataType.DOUBLE));
		assertEquals(PixelDataType.CONST_STRING, SemossDataType.convertToPixelDataType(SemossDataType.STRING));
		assertEquals(PixelDataType.CONST_DATE, SemossDataType.convertToPixelDataType(SemossDataType.DATE));
		assertEquals(PixelDataType.CONST_TIMESTAMP, SemossDataType.convertToPixelDataType(SemossDataType.TIMESTAMP));
		assertNull(SemossDataType.convertToPixelDataType(SemossDataType.FACTOR));
	}
	
	@Test
	void testConvertDataTypeToString() {
		assertEquals("BOOLEAN", SemossDataType.convertDataTypeToString(SemossDataType.BOOLEAN));
		assertEquals("INT", SemossDataType.convertDataTypeToString(SemossDataType.INT));
		assertEquals("DOUBLE", SemossDataType.convertDataTypeToString(SemossDataType.DOUBLE));
		assertEquals("STRING", SemossDataType.convertDataTypeToString(SemossDataType.STRING));
		assertEquals("STRING", SemossDataType.convertDataTypeToString(SemossDataType.FACTOR));
		assertEquals("DATE", SemossDataType.convertDataTypeToString(SemossDataType.DATE));
		assertEquals("TIMESTAMP", SemossDataType.convertDataTypeToString(SemossDataType.TIMESTAMP));
		assertNull(SemossDataType.convertDataTypeToString(null));
	}
	
	@Test
	void testConvertSemossDataTypeArrToStringArr() {
		assertNull(SemossDataType.convertSemossDataTypeArrToStringArr(null));
		String[] stringArr = {"BOOLEAN", "STRING"};
		SemossDataType[] typeArr = {SemossDataType.BOOLEAN, SemossDataType.STRING};
		assertArrayEquals(stringArr, SemossDataType.convertSemossDataTypeArrToStringArr(typeArr));
	}
}
