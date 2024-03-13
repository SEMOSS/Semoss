package prerna.junit.reactors.algorithm.api;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import prerna.algorithm.api.AdditionalDataType;

public class AdditionalDataTypesTest {
	
	@Test
	public void testHelpSize() {
		Map<AdditionalDataType, String> map = AdditionalDataType.getHelp();
		List<AdditionalDataType> l = map.keySet().stream().distinct().collect(Collectors.toList());
		int expected = 14;
		assertEquals(expected, l.size());
	}
	
	@Test
	public void testConvertStringToAdtlDataType() {
		AdditionalDataType adt = AdditionalDataType.convertStringToAdtlDataType("STATE");
		assertEquals(AdditionalDataType.STATE, adt);
	}

}
