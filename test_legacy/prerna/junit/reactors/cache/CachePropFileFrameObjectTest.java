package prerna.junit.reactors.cache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.cache.CachePropFileFrameObject;

public class CachePropFileFrameObjectTest {
	
	@Test
	public void setAndGetTests() {
		CachePropFileFrameObject o = new CachePropFileFrameObject();
		o.setFrameType("test");
		assertEquals("test", o.getFrameType());
	}

}
