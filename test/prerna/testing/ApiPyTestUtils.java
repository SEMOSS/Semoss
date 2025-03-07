package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiPyTestUtils {

	public static void check() {
		String pixel = "Py(\"<encode>1+1</encode>\");";
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(2, Integer.valueOf(nm.getValue().toString()).intValue());
	}

}
