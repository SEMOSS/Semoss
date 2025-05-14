package prerna.engine.api;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import prerna.engine.api.VenvTypeEnum;
import prerna.engine.impl.venv.PythonVenvEngine;

public class VenvTypeEnumUnitTests {

	@Test
	void testPython() {
		VenvTypeEnum testEnum = VenvTypeEnum.PYTHON;
		assertEquals("PYTHON", testEnum.getVenvName());
		assertEquals(PythonVenvEngine.class.getName(), testEnum.getVenvClass());
	}
	
	@Test
	void testBadVectorDatabaseName() {
		String badName = "NOT_A_REAL_VENV_TYPE";
		Exception thrown = assertThrows(
				IllegalArgumentException.class,
				() -> VenvTypeEnum.getEnumFromName(badName)
				);
		assertEquals("Invalid input for name " + badName, thrown.getMessage());
	}
}
