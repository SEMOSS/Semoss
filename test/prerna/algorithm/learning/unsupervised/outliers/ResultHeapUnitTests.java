package prerna.algorithm.learning.unsupervised.outliers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ResultHeapUnitTests {
	 private ResultHeap<String> resultHeap;
	    private final int capacity = 5;

	    @BeforeEach
	    public void setUp() {
	        resultHeap = new ResultHeap<>(capacity);
	    }

	    @Test
	    public void testOffer() {
	        resultHeap.offer(1.0, "A");
	        resultHeap.offer(2.0, "B");
	        resultHeap.offer(0.5, "C");
	        assertEquals(3, resultHeap.size());
	        assertEquals("C", resultHeap.returnData()[0]);
	        assertEquals("A", resultHeap.returnData()[1]);
	        assertEquals("B", resultHeap.returnData()[2]);
	    }

	    @Test
	    public void testGetMaxKey() {
	        resultHeap.offer(1.0, "A");
	        resultHeap.offer(2.0, "B");
	        assertEquals(2.0, resultHeap.getMaxKey());
	    }

	    @Test
	    public void testReturnData() {
	        resultHeap.offer(1.0, "A");
	        resultHeap.offer(2.0, "B");
	        Object[] data = resultHeap.returnData();
	        assertEquals("A", data[0]);
	        assertEquals("B", data[1]);
	    }

	    @Test
	    public void testRemoveMax() {
	        resultHeap.offer(1.0, "A");
	        resultHeap.offer(2.0, "B");
	        assertEquals("B", resultHeap.removeMax());
	        assertEquals(1, resultHeap.size());
	        assertEquals("A", resultHeap.removeMax());
	        assertTrue(resultHeap.isEmpty());
	    }

	    @Test
	    public void testIsEmpty() {
	        assertTrue(resultHeap.isEmpty());
	        resultHeap.offer(1.0, "A");
	        assertFalse(resultHeap.isEmpty());
	    }

	    @Test
	    public void testIsFull() {
	        for (int i = 0; i < capacity; i++) {
	            resultHeap.offer(i, "Value" + i);
	        }
	        assertTrue(resultHeap.isFull());
	    }

	    @Test
	    public void testSize() {
	        assertEquals(0, resultHeap.size());
	        resultHeap.offer(1.0, "A");
	        assertEquals(1, resultHeap.size());
	    }

	    @Test
	    public void testCapacity() {
	        assertEquals(capacity, resultHeap.capacity());
	    }
}
