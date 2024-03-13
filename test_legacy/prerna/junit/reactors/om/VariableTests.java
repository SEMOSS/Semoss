package prerna.junit.reactors.om;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.om.Variable;
import prerna.om.Variable.LANGUAGE;

public class VariableTests {
	
	@Test
	public void testGetExtension() {
		assertEquals("java", Variable.getExtension(LANGUAGE.JAVA));
	}

}
