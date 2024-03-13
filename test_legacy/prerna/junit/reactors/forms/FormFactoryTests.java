package prerna.junit.reactors.forms;

import static org.junit.Assert.assertNull;

import org.junit.Test;

import prerna.engine.impl.r.RNativeEngine;
import prerna.forms.FormFactory;

public class FormFactoryTests {

	@Test
	public void getFormBuilderNullTest() {
		RNativeEngine r = new RNativeEngine();
		assertNull(FormFactory.getFormBuilder(r));
	}
}
