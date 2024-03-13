package prerna.junit.reactors.rdf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import prerna.rdf.engine.wrappers.WrapperManager;

public class WrapperManagerTests {
	
	
	@Test
	public void testGetWrapperManager() {
		WrapperManager wm = WrapperManager.getInstance();
		assertNotNull(wm);
		assertEquals(wm, WrapperManager.getInstance());
	}

}
