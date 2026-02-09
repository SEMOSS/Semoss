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
package prerna.algorithm.learning.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CategoricalClusterUnitTests {

	 private CategoricalCluster cluster;

	    @Mock
	    private Map<String, Double> mockWeights;

	    @BeforeEach
	    public void setUp() {
	        MockitoAnnotations.openMocks(this);
	        mockWeights = new HashMap<>();
	        mockWeights.put("attribute1", 1.0);
	        mockWeights.put("attribute2", 2.0);
	        cluster = new CategoricalCluster(mockWeights);
	    }

	    @Test
	    public void testAddToClusterSingle() {
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        assertEquals(1.0, cluster.get("attribute1").get("instance1"), 0.01);
	    }

	    @Test
	    public void testAddToClusterMultiple() {
	        List<String> attributeNames = Arrays.asList("attribute1", "attribute2");
	        List<String> attributeInstances = Arrays.asList("instance1", "instance2");
	        List<Double> values = Arrays.asList(1.0, 2.0);

	        cluster.addToCluster(attributeNames, attributeInstances, values);

	        assertEquals(1.0, cluster.get("attribute1").get("instance1"), 0.01);
	        assertEquals(2.0, cluster.get("attribute2").get("instance2"), 0.01);
	    }

	    @Test
	    public void testRemoveFromClusterSingle() {
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        cluster.removeFromCluster("attribute1", "instance1", 1.0);
	        assertFalse(cluster.get("attribute1").containsKey("instance1"));
	    }

	    @Test
	    public void testRemoveFromClusterNonExistent() {
	        assertThrows(NullPointerException.class, () ->
	            cluster.removeFromCluster("attribute1", "instance1", 1.0));
	    }

	    @Test
	    public void testGetSimilarity() {
	        // Implement the logic for getSimilarity method
	        // This is just a placeholder test
	        assertEquals(0.0, cluster.getSimilarity("attribute1", "instance1"), 0.01);
	    }

	    @Test
	    public void testReset() {
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        cluster.reset();
	        assertEquals(0.0, cluster.get("attribute1").get("instance1"), 0.01);
	    }

	    @Test
	    public void testGetClusterSimilarity() {
	        CategoricalCluster cluster2 = new CategoricalCluster(mockWeights);
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        cluster2.addToCluster("attribute1", "instance1", 1.0);

	        double similarity = cluster.getClusterSimilarity(cluster2, "instanceType");
	        assertEquals(1.0, similarity, 0.01);
	    }

	    @Test
	    public void testGetWeights() {
	        assertEquals(mockWeights, cluster.getWeights());
	    }
}
