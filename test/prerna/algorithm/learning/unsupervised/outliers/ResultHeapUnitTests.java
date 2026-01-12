/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
