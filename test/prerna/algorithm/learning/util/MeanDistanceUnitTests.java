package prerna.algorithm.learning.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MeanDistanceUnitTests {
	 private MeanDistance meanDistance;

	    @BeforeEach
	    public void setUp() {
	        meanDistance = new MeanDistance();
	    }

	    @Test
	    public void testGetCentroidValue() {
	        assertEquals(0.0, meanDistance.getCentroidValue());
	    }

	    @Test
	    public void testAddPartialToCentroidValue() {
	        meanDistance.addPartialToCentroidValue(10.0, 0.5);
	        assertEquals(5.0, meanDistance.getCentroidValue());
	        meanDistance.addPartialToCentroidValue(null, 0.5);
	        assertTrue(meanDistance.isPreviousNull());
	    }

	    @Test
	    public void testAddToCentroidValue() {
	        meanDistance.addToCentroidValue(10.0);
	        assertEquals(10.0, meanDistance.getCentroidValue());
	        meanDistance.addToCentroidValue(null);
	        assertTrue(meanDistance.isPreviousNull());
	    }

	    @Test
	    public void testRemovePartialFromCentroidValue() {
	        meanDistance.addPartialToCentroidValue(10.0, 0.5);
	        meanDistance.removePartialFromCentroidValue(10.0, 0.5);
	        assertEquals(5.0, meanDistance.getCentroidValue());
	        meanDistance.removePartialFromCentroidValue(null, 0.5);
	        assertTrue(meanDistance.isPreviousNull());
	    }

	    @Test
	    public void testRemoveFromCentroidValue() {
	        meanDistance.addToCentroidValue(10.0);
	        meanDistance.removeFromCentroidValue(10.0);
	        assertEquals(0.0, meanDistance.getCentroidValue());
	        meanDistance.removeFromCentroidValue(null);
	        assertTrue(meanDistance.isPreviousNull());
	    }

	    @Test
	    public void testGetNullRatio() {
	        meanDistance.addToCentroidValue(10.0);
	        meanDistance.addToCentroidValue(null);
	        assertEquals(0.5, meanDistance.getNullRatio());
	    }

	    @Test
	    public void testReset() {
	        meanDistance.addToCentroidValue(10.0);
	        meanDistance.reset();
	        assertEquals(0.0, meanDistance.getCentroidValue());
	        assertEquals(0.0, meanDistance.getNumInstances());
	        assertEquals(0.0, meanDistance.getNumNull());
	    }

	    @Test
	    public void testGetPreviousCentroidValue() {
	        meanDistance.addToCentroidValue(10.0);
	        assertEquals(0.0, meanDistance.getPreviousCentroidValue());
	    }

	    @Test
	    public void testGetChangeToCentroidValue() {
	        meanDistance.addToCentroidValue(10.0);
	        assertEquals(10.0, meanDistance.getChangeToCentroidValue());
	    }

	    @Test
	    public void testGetNumNull() {
	        meanDistance.addToCentroidValue(null);
	        assertEquals(1.0, meanDistance.getNumNull());
	    }

	    @Test
	    public void testGetNumInstances() {
	        meanDistance.addToCentroidValue(10.0);
	        assertEquals(1.0, meanDistance.getNumInstances());
	    }

	    @Test
	    public void testIsPreviousNull() {
	        meanDistance.addToCentroidValue(null);
	        assertTrue(meanDistance.isPreviousNull());
	    }
}
