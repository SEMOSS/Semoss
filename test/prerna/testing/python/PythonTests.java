package prerna.testing.python;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class PythonTests extends AbstractBaseSemossApiTests {

	@Test
	public void basicPython() {
		String pixel = "Py(\"<encode>2+2</encode>\");";

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm);
		assertEquals(PixelDataType.CODE, nm.getNounType());
		List<NounMetadata> values = (List<NounMetadata>) nm.getValue();
		NounMetadata response = values.get(0);
		assertEquals("4", response.getValue().toString());
	}
	
	@Test
	public void basicPython2() {
		String pixel = "Py(\"<encode>2+2</encode>\");";

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm);
		assertEquals(PixelDataType.CODE, nm.getNounType());
		List<NounMetadata> values = (List<NounMetadata>) nm.getValue();
		NounMetadata response = values.get(0);
		assertEquals("4", response.getValue().toString());
	}
}
