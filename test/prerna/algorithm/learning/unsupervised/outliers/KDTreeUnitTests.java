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

import java.util.List;

public class KDTreeUnitTests {

	 private KDTree<String> kdTree;
	    private final int dimensions = 3;
	    private final int bucketSize = 10;

	    @BeforeEach
	    public void setUp() {
	        kdTree = new KDTree<>(dimensions, bucketSize);
	    }

	    @Test
	    public void testAdd() {
	        double[] key = {1.0, 2.0, 3.0};
	        kdTree.add(key, "A");
	        List<String> result = kdTree.getRange(new double[]{0.0, 0.0, 0.0}, new double[]{2.0, 2.0, 2.0});
	        assertFalse(result.contains("A"));
	    }

	    @Test
	    public void testGetRange() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        double[] key2 = {4.0, 5.0, 6.0};
	        kdTree.add(key1, "A");
	        kdTree.add(key2, "B");
	        List<String> result = kdTree.getRange(new double[]{0.0, 0.0, 0.0}, new double[]{5.0, 5.0, 5.0});
	        assertTrue(result.contains("A"));
	        assertFalse(result.contains("B"));
	    }

	    @Test
	    public void testGetNearestNeighbors() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        double[] key2 = {4.0, 5.0, 6.0};
	        kdTree.add(key1, "A");
	        kdTree.add(key2, "B");
	        ResultHeap<String> result = kdTree.getNearestNeighbors(new double[]{1.0, 2.0, 3.0}, 1);
	        assertEquals("A", result.removeMax());
	    }

	    @Test
	    public void testGetNearestNeighborsWithMoreResults() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        double[] key2 = {4.0, 5.0, 6.0};
	        kdTree.add(key1, "A");
	        kdTree.add(key2, "B");
	        ResultHeap<String> result = kdTree.getNearestNeighbors(new double[]{1.0, 2.0, 3.0}, 2);
	        assertEquals("B", result.removeMax());
	    }

	    @Test
	    public void testGetRangeWithNoResults() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        kdTree.add(key1, "A");
	        List<String> result = kdTree.getRange(new double[]{4.0, 4.0, 4.0}, new double[]{5.0, 5.0, 5.0});
	        assertTrue(result.isEmpty());
	    }
}
