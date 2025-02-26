package prerna.unit.query.querystruct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.query.querystruct.SetParamsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetParamsReactorUnitTests {
	private SetParamsReactor reactor;
	private Map<String, String> keyValues;

    @BeforeEach
	void setup() {
		reactor = new SetParamsReactor();
		keyValues = reactor.keyValue;
	}
	
	@Test
	void SetParamsReactorTest() {
		keyValues.put("PIXEL_ID", "testId");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
	}
}
