package prerna.testing.python;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class PythonTests extends AbstractBaseSemossApiTests {

	@Test
	public void basicPython() {
		String pixel = "Py(\"<encode>return 2+2</encode>\");";
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm);
	}
}
