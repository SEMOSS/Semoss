package prerna.junit.reactors.algorithm.api;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.algorithm.api.DataFrameTypeEnum;

public class DataFrameTypeEnumTest {
	
	@Test
	public void testFromString() {
		DataFrameTypeEnum test = DataFrameTypeEnum.PYTHON;
		assertEquals("PY", test.getTypeAsString());
	}

}
