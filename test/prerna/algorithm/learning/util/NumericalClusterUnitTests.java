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
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import prerna.algorithm.learning.util.CategoricalCluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.NumericalCluster;

public class NumericalClusterUnitTests {

    private NumericalCluster numericalCluster;

    @Mock
    private IClusterDistanceMode mockDistanceMode;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Map<String, Double> weights = new HashMap<>();
        weights.put("attribute1", 1.0);
        weights.put("attribute2", 2.0);

        numericalCluster = new NumericalCluster(weights);
        numericalCluster.setDistanceMode("attribute1", mockDistanceMode);
        numericalCluster.setDistanceMode("attribute2", mockDistanceMode);
    }

    @Test
    public void testGetSimilarityWithNullValue() {
        List<String> attributes = Arrays.asList("attribute1", "attribute2");
        List<Double> values = Arrays.asList(null, 2.0);

        when(mockDistanceMode.getNullRatio()).thenReturn(0.5);
        when(mockDistanceMode.getCentroidValue()).thenReturn(1.0);

        Double similarity = numericalCluster.getSimilarity(attributes, values, -1);

        assertNotNull(similarity);
        assertEquals(-0.5811388300841898, similarity, 0.01);
    }

    @Test
    public void testGetSimilarityWithNonNullValue() {
        List<String> attributes = Arrays.asList("attribute1", "attribute2");
        List<Double> values = Arrays.asList(1.0, 2.0);

        when(mockDistanceMode.getCentroidValue()).thenReturn(1.0);

        Double similarity = numericalCluster.getSimilarity(attributes, values, -1);

        assertNotNull(similarity);
        assertEquals(-0.41421356237309515, similarity, 0.01);
    }

    @Test
    public void testAddToCluster() {
        numericalCluster.addToCluster("attribute1", 2.0, 0.5);
        verify(mockDistanceMode, times(1)).addPartialToCentroidValue(2.0, 0.5);
    }

    @Test
    public void testRemoveFromCluster() {
        numericalCluster.removeFromCluster("attribute1", 2.0, 0.5);
        verify(mockDistanceMode, times(1)).removePartialFromCentroidValue(2.0, 0.5);
    }

    @Test
    public void testGetClusterSimilarity() {
        NumericalCluster otherCluster = mock(NumericalCluster.class);
        when(otherCluster.isEmpty()).thenReturn(false);
        when(otherCluster.getCenterValueForAttribute("attribute1")).thenReturn(1.0);
        when(otherCluster.getCenterValueForAttribute("attribute2")).thenReturn(2.0);

        when(mockDistanceMode.getCentroidValue()).thenReturn(1.0);

        double similarity = numericalCluster.getClusterSimilarity(otherCluster, "instanceType");

        assertNotNull(similarity);
        assertEquals(-0.41421356237309515, similarity, 0.01);
    }
	
}
