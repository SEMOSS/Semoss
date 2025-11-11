package prerna.unit.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.cache.CachePropFileFrameObject;

public class CachePropFileFrameObjectUnitTests {
	
    @Test
   	public void testSetFrameCacheLocation() {
    	String frameLocation = "location";
    	CachePropFileFrameObject cache = new CachePropFileFrameObject();
    	cache.setFrameCacheLocation(frameLocation);
    	assertEquals(frameLocation, cache.getFrameCacheLocation());
    }
    
    @Test
   	public void testSetFrameMetaCacheLocation() {
    	String frameLocation = "location";
    	CachePropFileFrameObject cache = new CachePropFileFrameObject();
    	cache.setFrameMetaCacheLocation(frameLocation);
    	assertEquals(frameLocation, cache.getFrameMetaCacheLocation());
    }
    
    @Test
   	public void testSetFrameStateCacheLocation() {
    	String frameLocation = "location";
    	CachePropFileFrameObject cache = new CachePropFileFrameObject();
    	cache.setFrameStateCacheLocation(frameLocation);
    	assertEquals(frameLocation, cache.getFrameStateCacheLocation());
    }
    
    @Test
   	public void testSetFrameName() {
    	String frameName = "location";
    	CachePropFileFrameObject cache = new CachePropFileFrameObject();
    	cache.setFrameName(frameName);
    	assertEquals(frameName, cache.getFrameName());
    }

    
    @Test
   	public void testSetFrameType() {
    	String frameType = "grid";
    	CachePropFileFrameObject cache = new CachePropFileFrameObject();
    	cache.setFrameType(frameType);
    	assertEquals(frameType, cache.getFrameType());
    }

}
