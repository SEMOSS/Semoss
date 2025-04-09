package prerna.unit.algorithm.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;

public class DataFrameTypeEnumUnitTests {
	
	@Test
	void testGetTypeAsString() {
		assertEquals("R", DataFrameTypeEnum.R.getTypeAsString());
	}
	
	@Test
	void testToString() {
		assertEquals("R", DataFrameTypeEnum.R.toString());
	}
}
