package prerna.unit.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.date.SemossYear;
import prerna.date.reactor.YearReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class YearReactorUnitTests {
	
	private YearReactor reactor;
	private Map<String, String> keyValues;
	
	
	@BeforeEach
	void setup() {
		reactor = new YearReactor();
		keyValues = reactor.keyValue;
	}

	@Test
	void testYear() {
		keyValues.put("years", "2020");
		NounMetadata nm = reactor.execute();
		assertEquals(PixelDataType.CONST_YEAR, nm.getNounType());
		SemossYear sy = (SemossYear) nm.getValue();
		assertEquals(2020, sy.getNumYears());
	}
}
